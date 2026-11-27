from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.clock import FakeClock
from app.sync import DirectorySync
from app.storage import Storage
from provider.server import create_provider_app
from provider.store import Store, make_user, uid

PAGE_1_USER = uid(1)
PAGE_2_USER = uid(4)
PAGE_3_USER = uid(7)

SEED = [
    ("Aaron", "Abbott", "Finance", "Senior Analyst"),
    ("Abigail", "Adams", "Engineering", "Staff Engineer"),
    ("Bruno", "Costa", "Sales", "Account Executive"),
    ("Chen", "Wei", "Engineering", "Engineer"),
    ("Dana", "Ellis", "Support", "Support Lead"),
    ("Farah", "Haddad", "Legal", "Counsel"),
    ("Gabe", "Ito", "Finance", "Controller"),
    ("Hana", "Kim", "HR", "HR Partner"),
    ("Ivan", "Novak", "Engineering", "Senior Engineer"),
]


class Provider:
    def __init__(self, app, clock):
        self.app = app
        self.store = app.state.store
        self.clock = clock

    def update(self, user_id, advance=1, **profile):
        self.clock.advance(advance)
        return self.store.update(user_id, **profile)

    def deactivate(self, user_id, advance=1):
        self.clock.advance(advance)
        return self.store.set_status(user_id, "DEACTIVATED")

    def activate(self, user_id, advance=1):
        self.clock.advance(advance)
        return self.store.set_status(user_id, "ACTIVE")

    def create(self, advance=1, **profile):
        self.clock.advance(advance)
        return self.store.create(**profile)

    def bulk_import(self, advance=1):
        self.clock.advance(advance)
        return self.store.touch_all()

    def after_page(self, n, fn):
        self.app.state.hooks.after_page(n, fn)

    def set_host_clock_ahead(self, seconds):
        self.store.clock_skew_s = seconds


@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc))


@pytest.fixture
def provider(clock):
    store = Store(clock)
    store.load([make_user(n, *row) for n, row in enumerate(SEED, start=1)])
    return Provider(create_provider_app(store, page_size_max=3), clock)


@pytest.fixture
def directory(provider, clock):
    return DirectorySync(TestClient(provider.app), Storage(), clock, page_size=3)


@pytest.fixture
def initial_sync(directory, clock):
    result = directory.full_reconcile()
    clock.advance(60)
    return result
