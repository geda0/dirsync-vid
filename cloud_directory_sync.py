"""
Consolidated Cloud Directory — Synchronization Starter Module
==============================================================

OVERVIEW
--------
This module implements the synchronization engine between external Cloud Identity
Providers (IdPs) and our local Consolidated Cloud Directory, along with a simulated
provider, incident logs, and a baseline test suite.

================================================================================
SECTION 1: CLOUD PROVIDER API DOCUMENTATION (v2.4)
================================================================================

Endpoint: `provider.list_users(...)`
------------------------------------
Returns user records from the cloud provider ordered by `(updated_at ASC, user_id ASC)`.

Parameters:
  - `updated_since` (float | None):
        Filters records by `updated_at > updated_since` when `inclusive=False` (default),
        or `updated_at >= updated_since` when `inclusive=True`.
        NOTE: Provider timestamps have 1-second resolution under standard operation
        (e.g., `1700000001.0`), so multiple updates within the same second share an
        identical `updated_at` value.
  - `inclusive` (bool, default=False):
        Controls whether `updated_since` is inclusive (`>=`) or strict (`>`).
  - `offset` (int, default=0):
        Zero-based row offset into the filtered result set.
        NOTE: `offset` pagination is evaluated against live index order on each request
        (not snapshot-isolated). If a previously fetched record is updated mid-traversal,
        its `updated_at` moves to the tail of the index and shifts subsequent rows.
  - `cursor` (tuple[float, str] | None, default=None):
        Keyset pagination cursor `(last_updated_at, last_user_id)`. When provided,
        `offset` is ignored and the provider returns records strictly after
        `(last_updated_at, last_user_id)` in `(updated_at ASC, user_id ASC)` order.
  - `limit` (int, default=50):
        Maximum number of records returned per page.
  - `include_deleted` (bool, default=True):
        Whether soft-deleted (`deleted=True`) user tombstones are returned.

Endpoint: `provider.get_delta_feed(delta_token=0, limit=50)`
------------------------------------------------------------
Monotonic change-log endpoint:
  - Every create, update, or soft-delete at the provider is assigned a strictly
    increasing integer `change_seq` (`1, 2, 3, ...`).
  - Passing `delta_token` returns up to `limit` events with `change_seq > delta_token`
    ordered by `change_seq ASC`, along with `next_delta_token` and `has_more`.

Endpoint: `provider.push_user_update(...)`
------------------------------------------
Updates a user record at the provider:
  - `expected_version` (int | None): Optional optimistic concurrency check.
    When provided, raises `VersionConflictError` if the provider's current
    `user.version != expected_version`. When `None`, overwrites unconditionally.

================================================================================
SECTION 2: LOST UPDATE INCIDENT REPORTS
================================================================================

Incident #4012 — Updates during multi-page sync are permanently missed
  - Reported: During a multi-page sync (250+ users, `page_size=25`), an admin
    updated `usr_014` (`role="admin"` -> `"viewer"`) while later pages were still
    being fetched. The sync reported success, and subsequent sync runs never
    picked up `usr_014`'s change.

Incident #4019 — Unmodified user skipped when an earlier user is edited mid-sync
  - Reported: While paginating through updates (`limit=5`), `usr_001` (already
    fetched on page 1) was edited at the provider before page 2 was requested.
    `usr_005` was never synced during that run or future runs.

Incident #4027 — Burst updates within the same second drop trailing records
  - Reported: Automated provisioning updated several users within the same
    1-second timestamp tick (`t = 1700000100.0`). A sync executed mid-burst,
    synced the first batch, and never picked up the remaining users updated at
    `t = 1700000100.0` on subsequent runs.

Incident #4033 — Outbound local edit clobbers concurrent provider update
  - Reported: A local `display_name` change was queued for `usr_005` at `version=2`.
    Before the next sync ran, `usr_005`'s `role` was changed to `"admin"` at the
    provider (`version=3`). When sync executed, `usr_005`'s `role` reverted to
    `"member"` at both the provider and local store.

Incident #4041 — Deactivated/deleted users remain active locally
  - Reported: Users soft-deleted at the provider after initial onboarding remain
    active in the consolidated directory indefinitely.
"""

from __future__ import annotations

import copy
import threading
import unittest
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple


# ==============================================================================
# Exceptions & Data Models
# ==============================================================================

class VersionConflictError(Exception):
    """Raised when an optimistic concurrency check (`expected_version`) fails."""

    def __init__(self, user_id: str, expected_version: int, actual_version: int):
        super().__init__(
            f"Version conflict for {user_id}: expected v{expected_version}, "
            f"provider has v{actual_version}"
        )
        self.user_id = user_id
        self.expected_version = expected_version
        self.actual_version = actual_version


@dataclass
class ProviderUser:
    user_id: str
    email: str
    display_name: str
    role: str = "member"
    active: bool = True
    deleted: bool = False
    version: int = 1
    updated_at: float = 0.0

    def clone(self) -> "ProviderUser":
        return copy.deepcopy(self)


@dataclass
class ProviderPage:
    users: List[ProviderUser]
    has_more: bool
    next_offset: Optional[int] = None
    next_cursor: Optional[Tuple[float, str]] = None


@dataclass
class DeltaEvent:
    change_seq: int
    user: ProviderUser


@dataclass
class DeltaPage:
    events: List[DeltaEvent]
    next_delta_token: int
    has_more: bool


@dataclass
class LocalUser:
    user_id: str
    provider_id: str
    email: str
    display_name: str
    role: str = "member"
    active: bool = True
    deleted: bool = False
    version: int = 1
    provider_updated_at: float = 0.0
    base_version: Optional[int] = None
    pending_local_fields: Dict[str, object] = field(default_factory=dict)

    def clone(self) -> "LocalUser":
        return copy.deepcopy(self)


@dataclass
class SyncStats:
    provider_id: str
    pulled_creations: int = 0
    pulled_updates: int = 0
    pulled_deletions: int = 0
    pushed_updates: int = 0
    conflicts_resolved: int = 0


# ==============================================================================
# Simulated Cloud Identity Provider
# ==============================================================================

class SimulatedClock:
    """Deterministic clock with controllable resolution (default 1.0s ticks)."""

    def __init__(self, start_time: float = 1_700_000_000.0) -> None:
        self._now = float(start_time)
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float = 1.0) -> float:
        with self._lock:
            self._now += seconds
            return self._now


class MockCloudProvider:
    """
    Thread-safe in-memory simulation of an external Cloud Identity Provider.
    Supports deterministic hooks (`on_page_fetched`) to simulate concurrent
    provider-side mutations mid-sync without flaky sleeps.
    """

    def __init__(self, provider_id: str, clock: Optional[SimulatedClock] = None) -> None:
        self.provider_id = provider_id
        self.clock = clock or SimulatedClock()
        self._lock = threading.RLock()
        self._users: Dict[str, ProviderUser] = {}
        self._change_log: List[DeltaEvent] = []
        self._change_seq: int = 0
        self.on_page_fetched: Optional[Callable[[int], None]] = None
        self._page_fetch_count: int = 0

    def current_time(self) -> float:
        return self.clock.now()

    # --- External / Admin Provider Mutations ---

    def create_user(
        self,
        user_id: str,
        email: str,
        display_name: str,
        role: str = "member",
        active: bool = True,
    ) -> ProviderUser:
        with self._lock:
            if user_id in self._users:
                raise ValueError(f"User {user_id} already exists at provider {self.provider_id}")
            now = self.clock.now()
            user = ProviderUser(
                user_id=user_id,
                email=email,
                display_name=display_name,
                role=role,
                active=active,
                deleted=False,
                version=1,
                updated_at=now,
            )
            self._users[user_id] = user
            self._record_delta(user)
            return user.clone()

    def update_user_at_provider(
        self,
        user_id: str,
        *,
        email: Optional[str] = None,
        display_name: Optional[str] = None,
        role: Optional[str] = None,
        active: Optional[bool] = None,
    ) -> ProviderUser:
        """Simulates an external edit directly on the cloud provider."""
        with self._lock:
            if user_id not in self._users:
                raise KeyError(f"Unknown user {user_id}")
            user = self._users[user_id]
            if email is not None:
                user.email = email
            if display_name is not None:
                user.display_name = display_name
            if role is not None:
                user.role = role
            if active is not None:
                user.active = active
            user.version += 1
            user.updated_at = self.clock.now()
            self._record_delta(user)
            return user.clone()

    def delete_user_at_provider(self, user_id: str) -> ProviderUser:
        """Simulates a soft-delete (tombstone) on the cloud provider."""
        with self._lock:
            if user_id not in self._users:
                raise KeyError(f"Unknown user {user_id}")
            user = self._users[user_id]
            user.deleted = True
            user.active = False
            user.version += 1
            user.updated_at = self.clock.now()
            self._record_delta(user)
            return user.clone()

    def get_user(self, user_id: str) -> Optional[ProviderUser]:
        with self._lock:
            user = self._users.get(user_id)
            return user.clone() if user else None

    # --- Sync API Endpoints ---

    def list_users(
        self,
        *,
        updated_since: Optional[float] = None,
        inclusive: bool = False,
        offset: int = 0,
        cursor: Optional[Tuple[float, str]] = None,
        limit: int = 50,
        include_deleted: bool = True,
    ) -> ProviderPage:
        with self._lock:
            records = list(self._users.values())
            if not include_deleted:
                records = [u for u in records if not u.deleted]

            if updated_since is not None:
                if inclusive:
                    records = [u for u in records if u.updated_at >= updated_since]
                else:
                    records = [u for u in records if u.updated_at > updated_since]

            records.sort(key=lambda u: (u.updated_at, u.user_id))

            if cursor is not None:
                records = [u for u in records if (u.updated_at, u.user_id) > cursor]
                slice_records = records[:limit]
                has_more = len(records) > limit
                next_cursor = (
                    (slice_records[-1].updated_at, slice_records[-1].user_id)
                    if slice_records
                    else cursor
                )
                page = ProviderPage(
                    users=[u.clone() for u in slice_records],
                    has_more=has_more,
                    next_offset=None,
                    next_cursor=next_cursor,
                )
            else:
                slice_records = records[offset : offset + limit]
                has_more = (offset + limit) < len(records)
                page = ProviderPage(
                    users=[u.clone() for u in slice_records],
                    has_more=has_more,
                    next_offset=(offset + limit) if has_more else None,
                    next_cursor=None,
                )

            self._page_fetch_count += 1
            fetch_index = self._page_fetch_count

        if self.on_page_fetched is not None:
            self.on_page_fetched(fetch_index)

        return page

    def get_delta_feed(self, delta_token: int = 0, limit: int = 50) -> DeltaPage:
        with self._lock:
            matching = [ev for ev in self._change_log if ev.change_seq > delta_token]
            batch = matching[:limit]
            has_more = len(matching) > limit
            next_token = batch[-1].change_seq if batch else delta_token
            page = DeltaPage(
                events=[DeltaEvent(ev.change_seq, ev.user.clone()) for ev in batch],
                next_delta_token=next_token,
                has_more=has_more,
            )
            self._page_fetch_count += 1
            fetch_index = self._page_fetch_count

        if self.on_page_fetched is not None:
            self.on_page_fetched(fetch_index)

        return page

    def push_user_update(
        self,
        user_id: str,
        *,
        email: str,
        display_name: str,
        role: str,
        active: bool,
        expected_version: Optional[int] = None,
    ) -> ProviderUser:
        with self._lock:
            if user_id not in self._users:
                raise KeyError(f"Unknown user {user_id}")
            user = self._users[user_id]
            if expected_version is not None and user.version != expected_version:
                raise VersionConflictError(
                    user_id=user_id,
                    expected_version=expected_version,
                    actual_version=user.version,
                )
            user.email = email
            user.display_name = display_name
            user.role = role
            user.active = active
            user.version += 1
            user.updated_at = self.clock.now()
            self._record_delta(user)
            return user.clone()

    def _record_delta(self, user: ProviderUser) -> None:
        self._change_seq += 1
        self._change_log.append(DeltaEvent(change_seq=self._change_seq, user=user.clone()))


# ==============================================================================
# Local Consolidated Cloud Directory Store
# ==============================================================================

class ConsolidatedDirectoryStore:
    """In-memory store representing our local Consolidated Cloud Directory."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._users: Dict[Tuple[str, str], LocalUser] = {}
        self._sync_state: Dict[str, Dict[str, object]] = {}

    def get_user(self, provider_id: str, user_id: str) -> Optional[LocalUser]:
        with self._lock:
            u = self._users.get((provider_id, user_id))
            return u.clone() if u else None

    def list_users(self, provider_id: Optional[str] = None, include_deleted: bool = False) -> List[LocalUser]:
        with self._lock:
            result = []
            for (pid, _), u in self._users.items():
                if provider_id is not None and pid != provider_id:
                    continue
                if not include_deleted and u.deleted:
                    continue
                result.append(u.clone())
            return sorted(result, key=lambda x: (x.provider_id, x.user_id))

    def upsert_from_provider(self, provider_id: str, p_user: ProviderUser) -> str:
        """
        Applies an inbound provider record to the local store.
        Returns one of: `"created"`, `"updated"`, `"deleted"`, `"noop"`.
        """
        with self._lock:
            key = (provider_id, p_user.user_id)
            existing = self._users.get(key)
            if existing is None:
                self._users[key] = LocalUser(
                    user_id=p_user.user_id,
                    provider_id=provider_id,
                    email=p_user.email,
                    display_name=p_user.display_name,
                    role=p_user.role,
                    active=p_user.active,
                    deleted=p_user.deleted,
                    version=p_user.version,
                    provider_updated_at=p_user.updated_at,
                )
                return "deleted" if p_user.deleted else "created"

            if existing.version == p_user.version and existing.deleted == p_user.deleted:
                return "noop"

            existing.email = p_user.email
            existing.display_name = p_user.display_name
            existing.role = p_user.role
            existing.active = p_user.active
            existing.deleted = p_user.deleted
            existing.version = p_user.version
            existing.provider_updated_at = p_user.updated_at

            # Re-overlay any unpushed local field edits on top of refreshed state
            for field_name, val in existing.pending_local_fields.items():
                setattr(existing, field_name, val)

            return "deleted" if p_user.deleted else "updated"

    def queue_local_edit(self, provider_id: str, user_id: str, **fields: object) -> LocalUser:
        """Queues field-level local edits to be pushed to the provider on next sync."""
        allowed = {"email", "display_name", "role", "active"}
        invalid = set(fields.keys()) - allowed
        if invalid:
            raise ValueError(f"Unsupported editable fields: {invalid}")

        with self._lock:
            key = (provider_id, user_id)
            if key not in self._users:
                raise KeyError(f"User {user_id} for provider {provider_id} not found locally")
            user = self._users[key]
            if user.base_version is None:
                user.base_version = user.version
            for k, v in fields.items():
                setattr(user, k, v)
                user.pending_local_fields[k] = v
            return user.clone()

    def get_users_with_pending_edits(self, provider_id: str) -> List[LocalUser]:
        with self._lock:
            return [
                u.clone()
                for (pid, _), u in self._users.items()
                if pid == provider_id and u.pending_local_fields
            ]

    def mark_local_edits_synced(self, provider_id: str, p_user: ProviderUser) -> None:
        with self._lock:
            key = (provider_id, p_user.user_id)
            if key in self._users:
                local = self._users[key]
                local.email = p_user.email
                local.display_name = p_user.display_name
                local.role = p_user.role
                local.active = p_user.active
                local.deleted = p_user.deleted
                local.version = p_user.version
                local.provider_updated_at = p_user.updated_at
                local.base_version = None
                local.pending_local_fields.clear()

    def get_sync_state(self, provider_id: str) -> Dict[str, object]:
        with self._lock:
            return copy.deepcopy(self._sync_state.get(provider_id, {}))

    def set_sync_state(self, provider_id: str, state: Dict[str, object]) -> None:
        with self._lock:
            self._sync_state[provider_id] = copy.deepcopy(state)


# ==============================================================================
# Synchronization Engine
# ==============================================================================

class DirectorySyncEngine:
    """
    Synchronizes user directories between registered `MockCloudProvider`
    instances and the `ConsolidatedDirectoryStore`.
    """

    def __init__(self, store: ConsolidatedDirectoryStore, page_size: int = 25) -> None:
        self.store = store
        self.page_size = page_size
        self._providers: Dict[str, MockCloudProvider] = {}

    def register_provider(self, provider: MockCloudProvider) -> None:
        self._providers[provider.provider_id] = provider

    def sync_all(self) -> Dict[str, SyncStats]:
        results: Dict[str, SyncStats] = {}
        for provider_id in list(self._providers.keys()):
            results[provider_id] = self.sync_provider(provider_id)
        return results

    def sync_provider(self, provider_id: str) -> SyncStats:
        if provider_id not in self._providers:
            raise KeyError(f"Provider {provider_id} is not registered")

        provider = self._providers[provider_id]
        stats = SyncStats(provider_id=provider_id)
        state = self.store.get_sync_state(provider_id)
        last_sync_ts: Optional[float] = state.get("last_sync_ts")  # type: ignore[assignment]

        # Step 1: Push local pending edits upstream
        for local_user in self.store.get_users_with_pending_edits(provider_id):
            updated_remote = provider.push_user_update(
                local_user.user_id,
                email=local_user.email,
                display_name=local_user.display_name,
                role=local_user.role,
                active=local_user.active,
                expected_version=None,
            )
            self.store.mark_local_edits_synced(provider_id, updated_remote)
            stats.pushed_updates += 1

        # Step 2: Pull updated users from provider
        offset = 0
        while True:
            page = provider.list_users(
                updated_since=last_sync_ts,
                inclusive=False,
                offset=offset,
                limit=self.page_size,
                include_deleted=False,
            )

            for p_user in page.users:
                outcome = self.store.upsert_from_provider(provider_id, p_user)
                if outcome == "created":
                    stats.pulled_creations += 1
                elif outcome == "updated":
                    stats.pulled_updates += 1
                elif outcome == "deleted":
                    stats.pulled_deletions += 1

            if not page.has_more or page.next_offset is None:
                break
            offset = page.next_offset

        # Record completion timestamp for next incremental sync
        state["last_sync_ts"] = provider.current_time()
        self.store.set_sync_state(provider_id, state)

        return stats


# ==============================================================================
# Starter Test Suite
# ==============================================================================

class TestDirectorySyncBaseline(unittest.TestCase):
    """
    Baseline happy-path tests. Extend this test suite to reproduce the
    incident reports and verify concurrent synchronization guarantees.
    """

    def setUp(self) -> None:
        self.clock = SimulatedClock(start_time=1_700_000_000.0)
        self.provider = MockCloudProvider("okta_prod", clock=self.clock)
        self.store = ConsolidatedDirectoryStore()
        self.engine = DirectorySyncEngine(self.store, page_size=5)
        self.engine.register_provider(self.provider)

    def test_initial_full_sync(self) -> None:
        for i in range(12):
            self.provider.create_user(
                user_id=f"usr_{i:03d}",
                email=f"user{i}@example.com",
                display_name=f"User {i}",
            )
            self.clock.advance(1.0)

        stats = self.engine.sync_provider("okta_prod")
        self.assertEqual(stats.pulled_creations, 12)
        self.assertEqual(len(self.store.list_users("okta_prod")), 12)

    def test_incremental_sync_non_concurrent(self) -> None:
        self.provider.create_user("usr_001", "u1@example.com", "User One")
        self.clock.advance(1.0)
        self.engine.sync_provider("okta_prod")

        self.clock.advance(5.0)
        self.provider.update_user_at_provider("usr_001", role="admin")
        self.clock.advance(1.0)

        stats = self.engine.sync_provider("okta_prod")
        self.assertEqual(stats.pulled_updates, 1)
        synced = self.store.get_user("okta_prod", "usr_001")
        self.assertIsNotNone(synced)
        self.assertEqual(synced.role, "admin")  # type: ignore[union-attr]

    # TODO: Add concurrent mid-sync mutation & edge-case tests here.


if __name__ == "__main__":
    unittest.main(verbosity=2)
