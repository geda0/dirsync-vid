import time
from datetime import datetime, timedelta, timezone


class SystemClock:
    def now(self):
        return datetime.now(timezone.utc)

    def sleep(self, seconds):
        time.sleep(seconds)


class FakeClock:
    def __init__(self, start):
        self._now = start

    def now(self):
        return self._now

    def advance(self, seconds):
        self._now += timedelta(seconds=seconds)
        return self._now

    def sleep(self, seconds):
        self.advance(seconds)
