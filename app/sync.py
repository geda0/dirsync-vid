import math

from app.storage import User


def format_ts(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_user(raw):
    p = raw["profile"]
    return User(
        external_id=raw["id"],
        email=p["email"],
        first_name=p["firstName"],
        last_name=p["lastName"],
        department=p["department"],
        title=p["title"],
        status=raw["status"],
        provider_last_updated=raw["lastUpdated"],
    )


class DirectorySync:
    def __init__(self, http, storage, clock, page_size=200):
        self.http = http
        self.storage = storage
        self.clock = clock
        self.page_size = page_size
        self.requests = 0

    def delta_sync(self, on_progress=None):
        cursor = self.storage.cursor()
        if cursor is None:
            return self.full_reconcile(on_progress)
        params = {"limit": self.page_size, "filter": f'lastUpdated gt "{cursor}"'}
        return self._run("delta", params, on_progress)

    def full_reconcile(self, on_progress=None):
        return self._run("full", {"limit": self.page_size}, on_progress)

    def _pages(self, params):
        response = self.http.get("/users", params=params)
        while True:
            self.requests += 1
            response.raise_for_status()
            yield response
            next_url = response.links.get("next", {}).get("url")
            if not next_url:
                return
            response = self.http.get(next_url)

    def _run(self, kind, params, on_progress):
        started_at = format_ts(self.clock.now())
        pages = seen = total_pages = 0

        for response in self._pages(params):
            pages += 1
            for raw in response.json():
                seen += 1
                self.storage.upsert(to_user(raw), format_ts(self.clock.now()))
            if "X-Total-Count" in response.headers:
                total_pages = math.ceil(int(response.headers["X-Total-Count"]) / self.page_size)
            if on_progress:
                on_progress(pages, max(pages, total_pages), seen)

        cursor_saved = format_ts(self.clock.now())
        self.storage.record_run(kind, started_at, format_ts(self.clock.now()), cursor_saved, pages, seen)
        return {"kind": kind, "pages": pages, "users_seen": seen, "cursor_saved": cursor_saved}
