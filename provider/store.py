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
    def __init__(self, clock, clock_skew_s=0.0):
        self.clock = clock
        self.clock_skew_s = clock_skew_s
        self.lock = threading.RLock()
        self.ids = []
        self.users = {}

    def provider_now(self):
        return (self.clock.now() - timedelta(seconds=self.clock_skew_s)).replace(microsecond=0)

    def _write(self, user, stamp=True):
        with self.lock:
            user = copy.deepcopy(user)
            if stamp:
                user["lastUpdated"] = fmt(self.provider_now())
            if user["id"] not in self.users:
                bisect.insort(self.ids, user["id"])
            self.users[user["id"]] = user
            return copy.deepcopy(user)

    def load(self, users):
        for user in users:
            self._write(user, stamp=False)

    def current(self, user_id):
        with self.lock:
            return copy.deepcopy(self.users[user_id])

    def update(self, user_id, **profile):
        user = self.current(user_id)
        user["profile"].update(profile)
        return self._write(user)

    def set_status(self, user_id, status):
        user = self.current(user_id)
        user["status"] = status
        return self._write(user)

    def create(self, **profile):
        user = make_user(len(self.ids) + 1, profile.pop("firstName"), profile.pop("lastName"), "", "")
        user["profile"].update(profile)
        return self._write(user)

    def touch_all(self):
        for user_id in list(self.ids):
            self._write(self.current(user_id))
        return len(self.ids)

    def list_page(self, after, limit, since=None):
        with self.lock:
            matching = [i for i in self.ids if since is None or self.users[i]["lastUpdated"] > since]
            rest = [i for i in matching if i > after] if after else matching
            page = [copy.deepcopy(self.users[i]) for i in rest[:limit]]
            return page, len(rest) > limit, len(matching)
