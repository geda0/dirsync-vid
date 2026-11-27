import bisect
import copy
import random
import threading
from datetime import timedelta

FIRST = ["James", "Maria", "Wei", "Fatima", "Lucas", "Priya", "Noah", "Elena", "Omar", "Sofia"]
LAST = ["Smith", "Garcia", "Chen", "Khan", "Silva", "Patel", "Brown", "Rossi", "Haddad", "Novak"]
DEPARTMENTS = ["Finance", "Engineering", "Sales", "Support", "HR", "Legal"]
TITLES = ["Analyst", "Engineer", "Manager", "Director", "Lead", "Specialist"]
SEED_TIME = "2026-01-01T09:00:00Z"


def uid(n):
    return f"u{n:06d}"


def fmt(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_user(n, first, last, department, title):
    return {
        "id": uid(n),
        "status": "ACTIVE",
        "lastUpdated": SEED_TIME,
        "profile": {
            "firstName": first,
            "lastName": last,
            "email": f"{first.lower()}.{last.lower()}{n}@example.com",
            "department": department,
            "title": title,
        },
    }


def seed_users(count, seed=42):
    rng = random.Random(seed)
    users = [make_user(1, "Aaron", "Abbott", "Finance", "Senior Analyst"),
             make_user(2, "Abigail", "Adams", "Engineering", "Staff Engineer")]
    for n in range(3, count + 1):
        users.append(make_user(n, rng.choice(FIRST), rng.choice(LAST), rng.choice(DEPARTMENTS), rng.choice(TITLES)))
    return users


class Store:
    def __init__(self, clock, clock_skew_s=0.0, search_lag_s=0.0, seed=0):
        self.clock = clock
        self.clock_skew_s = clock_skew_s
        self.search_lag_s = search_lag_s
        self.rng = random.Random(seed)
        self.lock = threading.RLock()
        self.ids = []
        self.versions = {}

    def provider_now(self):
        return (self.clock.now() - timedelta(seconds=self.clock_skew_s)).replace(microsecond=0)

    def _write(self, user, index_delay=None, stamp=True):
        with self.lock:
            user = copy.deepcopy(user)
            now = self.provider_now()
            if stamp:
                user["lastUpdated"] = fmt(now)
            if index_delay is None:
                index_delay = self.rng.uniform(0, self.search_lag_s) if self.search_lag_s else 0
            visible_at = fmt(now + timedelta(seconds=index_delay)) if stamp else user["lastUpdated"]
            if user["id"] not in self.versions:
                bisect.insort(self.ids, user["id"])
                self.versions[user["id"]] = []
            self.versions[user["id"]].append((visible_at, user))
            return copy.deepcopy(user)

    def load(self, users):
        for user in users:
            self._write(user, stamp=False)

    def current(self, user_id):
        with self.lock:
            return copy.deepcopy(self.versions[user_id][-1][1])

    def update(self, user_id, index_delay=None, **profile):
        user = self.current(user_id)
        user["profile"].update(profile)
        return self._write(user, index_delay)

    def set_status(self, user_id, status, index_delay=None):
        user = self.current(user_id)
        user["status"] = status
        return self._write(user, index_delay)

    def create(self, index_delay=None, **profile):
        user = make_user(len(self.ids) + 1, profile.pop("firstName"), profile.pop("lastName"), "", "")
        user["profile"].update(profile)
        return self._write(user, index_delay)

    def touch_all(self):
        for user_id in list(self.ids):
            self._write(self.current(user_id))
        return len(self.ids)

    def list_page(self, after, limit, op=None, bound=None):
        with self.lock:
            start = bisect.bisect_right(self.ids, after) if after else 0
            if op is None:
                ids = self.ids[start:start + limit]
                return ([copy.deepcopy(self.versions[i][-1][1]) for i in ids],
                        start + limit < len(self.ids), len(self.ids))

            now = fmt(self.provider_now())
            test = {"gt": bound.__lt__, "ge": bound.__le__, "lt": bound.__gt__, "eq": bound.__eq__}[op]
            page, more, total = [], False, 0
            for user_id in self.ids[start:]:
                visible = next((u for at, u in reversed(self.versions[user_id]) if at <= now), None)
                if visible is None or not test(visible["lastUpdated"]):
                    continue
                total += 1
                if len(page) < limit:
                    page.append(copy.deepcopy(visible))
                else:
                    more = True
                    if after:
                        break
            return page, more, (total if after is None else -1)
