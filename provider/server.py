import argparse
import time
from datetime import datetime, timezone
from email.utils import format_datetime
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from provider.store import Store, fmt, seed_users

ADMIN = """<!doctype html>
<html><head><meta charset="utf-8"><title>Identity Provider</title>
<style>
body { margin:0; font:14px sans-serif; background:#f4f5fb; color:#1d2140; }
header { background:#3949ab; color:#fff; padding:14px 24px; display:flex; justify-content:space-between; }
main { padding:20px 24px; }
.row { display:flex; gap:10px; margin-bottom:12px; }
button { padding:6px 10px; border:1px solid #3949ab; border-radius:6px; background:#fff; color:#3949ab; cursor:pointer; }
button.primary { background:#3949ab; color:#fff; } button.danger { border-color:#c62828; color:#c62828; }
table { width:100%; border-collapse:collapse; background:#fff; }
th, td { padding:7px 10px; border-bottom:1px solid #e1e3ef; text-align:left; }
.mono { font-family:monospace; font-size:12px; }
.ACTIVE, .DEACTIVATED { padding:2px 8px; border-radius:10px; color:#fff; font-size:12px; }
.ACTIVE { background:#2e7d32; } .DEACTIVATED { background:#9e9e9e; }
</style></head>
<body>
<header><b>Identity Provider &middot; Users</b><span>Provider time: <span id="now" class="mono"></span></span></header>
<main>
  <div class="row"><input id="q" placeholder="Search"><button id="prev">Prev</button><button id="next">Next</button><span id="count"></span></div>
  <table><thead><tr><th>ID</th><th>Name</th><th>Department</th><th>Title</th><th>Status</th><th>lastUpdated</th><th></th></tr></thead>
  <tbody id="rows"></tbody></table>
</main>
<script>
const $ = id => document.getElementById(id);
let offset = 0, total = 0;

async function load() {
  const d = await (await fetch(`/admin/users?q=${encodeURIComponent($("q").value)}&offset=${offset}`)).json();
  total = d.total;
  $("now").textContent = d.now;
  $("count").textContent = `${d.total} users`;
  $("rows").innerHTML = d.users.map(u => `<tr><td class="mono">${u.id}</td>
    <td>${u.profile.firstName} ${u.profile.lastName}</td><td>${u.profile.department}</td><td>${u.profile.title}</td>
    <td><span class="${u.status}">${u.status}</span></td><td class="mono">${u.lastUpdated}</td>
    <td><button onclick="edit('${u.id}', '${u.profile.title}')">Edit title</button>
    ${u.status === "ACTIVE"
      ? `<button class="danger" onclick="act('${u.id}', 'deactivate')">Deactivate</button>`
      : `<button onclick="act('${u.id}', 'activate')">Activate</button>`}</td></tr>`).join("");
}

async function act(id, action) {
  await fetch(`/users/${id}/status/${action}`, { method: "POST" });
  load();
}

async function edit(id, title) {
  const value = prompt("Title", title);
  if (value === null) return;
  await fetch(`/users/${id}`, { method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ profile: { title: value } }) });
  load();
}

$("q").oninput = () => { offset = 0; load(); };
$("prev").onclick = () => { offset = Math.max(0, offset - 50); load(); };
$("next").onclick = () => { if (offset + 50 < total) { offset += 50; load(); } };
setInterval(() => fetch("/admin/users?limit=0").then(r => r.json()).then(d => $("now").textContent = d.now), 1000);
load();
</script>
</body></html>
"""


class Hooks:
    def __init__(self):
        self.by_page = {}
        self.page = 0

    def after_page(self, n, fn):
        self.by_page.setdefault(n, []).append(fn)

    def clear(self):
        self.by_page.clear()


def create_provider_app(store, latency_ms=0, sim_latency_s=0.0, page_size_max=200):
    app = FastAPI()
    app.state.store = store
    app.state.hooks = Hooks()
    app.state.request_times = []

    def on_request():
        if sim_latency_s:
            store.clock.advance(sim_latency_s)
        if latency_ms:
            time.sleep(latency_ms / 1000)
        app.state.request_times.append(store.provider_now())
        return {"Date": format_datetime(store.provider_now(), usegmt=True)}

    @app.get("/users")
    def list_users(request: Request, limit: int = 200, after: str | None = None, filter: str | None = None):
        headers = on_request()
        since = None
        if filter:
            field, op, value = filter.split(" ", 2)
            if field != "lastUpdated" or op != "gt":
                raise HTTPException(400, "Invalid search criteria")
            since = value.strip('"')
        limit = min(limit, page_size_max)
        users, more, total = store.list_page(after, limit, since)
        headers["X-Total-Count"] = str(total)
        if more:
            params = urlencode({"limit": limit, "after": users[-1]["id"], **({"filter": filter} if filter else {})})
            headers["Link"] = f'<{str(request.base_url).rstrip("/")}/users?{params}>; rel="next"'
        response = JSONResponse(users, headers=headers)
        hooks = app.state.hooks
        hooks.page = 1 if after is None else hooks.page + 1
        for fn in hooks.by_page.pop(hooks.page, []):
            fn(store)
        return response

    @app.post("/users/{user_id}")
    async def update_user(user_id: str, request: Request):
        return store.update(user_id, **(await request.json())["profile"])

    @app.post("/users/{user_id}/status/{action}")
    def set_status(user_id: str, action: str):
        return store.set_status(user_id, "DEACTIVATED" if action == "deactivate" else "ACTIVE")

    @app.get("/admin", response_class=HTMLResponse)
    def admin():
        return ADMIN

    @app.get("/admin/users")
    def admin_users(q: str = "", offset: int = 0, limit: int = 50):
        q = q.lower()
        matches = []
        for user_id in list(store.ids):
            user = store.current(user_id)
            if q in " ".join(user["profile"].values()).lower():
                matches.append(user)
        return {"users": matches[offset:offset + limit], "total": len(matches), "now": fmt(store.provider_now())}

    return app


class RealClock:
    def now(self):
        return datetime.now(timezone.utc)


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=6000)
    parser.add_argument("--latency-ms", type=int, default=1000)
    parser.add_argument("--skew", type=float, default=0.0)
    args = parser.parse_args()
    store = Store(RealClock(), args.skew)
    store.load(seed_users(args.users))
    uvicorn.run(create_provider_app(store, latency_ms=args.latency_ms), port=9000, log_level="warning", date_header=False)
