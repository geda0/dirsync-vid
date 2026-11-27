import os
import threading

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from app.clock import SystemClock
from app.sync import DirectorySync
from app.storage import Storage

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Cloud Directory</title>
<style>
body { margin:0; font:14px sans-serif; background:#f6f7f9; color:#18202b; }
header { background:#0f766e; color:#fff; padding:14px 24px; font-size:18px; font-weight:600; }
main { padding:20px 24px; display:grid; gap:16px; }
.panel { background:#fff; border:1px solid #e4e7ec; border-radius:8px; padding:14px 16px; }
.row { display:flex; gap:10px; align-items:center; }
button { padding:8px 14px; border:1px solid #0f766e; border-radius:6px; background:#fff; color:#0f766e; font-weight:600; cursor:pointer; }
button.primary { background:#0f766e; color:#fff; }
button:disabled { opacity:.5; }
.bar { flex:1; height:10px; background:#eef0f3; border-radius:5px; overflow:hidden; }
.bar div { height:100%; width:0; background:#0f766e; }
.mono { font-family:monospace; font-size:12px; }
table { width:100%; border-collapse:collapse; }
th, td { padding:7px 10px; border-bottom:1px solid #e4e7ec; text-align:left; }
.ACTIVE, .DEACTIVATED { padding:2px 8px; border-radius:10px; color:#fff; font-size:12px; }
.ACTIVE { background:#16a34a; } .DEACTIVATED { background:#98a2b3; }
</style></head>
<body>
<header>Cloud Directory</header>
<main>
  <div class="panel row">
    <button id="delta" class="primary">Sync now</button>
    <div class="bar"><div id="fill"></div></div>
    <span id="label">Idle</span>
  </div>
  <div class="panel mono" id="last">No sync yet</div>
  <div class="panel">
    <div class="row"><input id="q" placeholder="Search users"><span id="count"></span>
      <button id="prev">Prev</button><button id="next">Next</button></div>
    <table><thead><tr><th>ID</th><th>Name</th><th>Email</th><th>Department</th><th>Title</th>
      <th>Status</th><th>Provider lastUpdated</th><th>Synced at</th></tr></thead><tbody id="rows"></tbody></table>
  </div>
</main>
<script>
const $ = id => document.getElementById(id);
let page = 0, total = 0, running = false;

async function users() {
  const d = await (await fetch(`/api/users?q=${encodeURIComponent($("q").value)}&page=${page}`)).json();
  total = d.total;
  $("count").textContent = `${d.total} users, page ${page + 1}`;
  $("rows").innerHTML = d.users.map(u => `<tr><td class="mono">${u.external_id}</td>
    <td>${u.first_name} ${u.last_name}</td><td>${u.email}</td><td>${u.department}</td><td>${u.title}</td>
    <td><span class="${u.status}">${u.status}</span></td><td class="mono">${u.provider_last_updated}</td>
    <td class="mono">${u.synced_at}</td></tr>`).join("");
}

async function poll() {
  const s = await (await fetch("/api/sync/status")).json();
  $("delta").disabled = s.running;
  $("fill").style.width = s.total_pages ? `${100 * s.page / s.total_pages}%` : "0";
  $("label").textContent = s.running ? `${s.kind} sync · Page ${s.page} / ${s.total_pages} · ${s.users_seen} users` : "Idle";
  const r = s.last_run;
  if (r) $("last").textContent = `Last sync: ${r.kind} · started ${r.started_at} · finished ${r.finished_at} · cursor saved ${r.cursor_saved} · users seen ${r.users_seen}`;
  if (running && !s.running) users();
  running = s.running;
}

async function start(kind) {
  await fetch("/api/sync", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ kind }) });
  poll();
}

$("delta").onclick = () => start("delta");
$("q").oninput = () => { page = 0; users(); };
$("prev").onclick = () => { page = Math.max(0, page - 1); users(); };
$("next").onclick = () => { if ((page + 1) * 50 < total) { page++; users(); } };
setInterval(poll, 500);
poll();
users();
</script>
</body></html>
"""


def create_app(sync):
    app = FastAPI()
    lock = threading.Lock()
    status = {"running": False, "kind": None, "page": 0, "total_pages": 0, "users_seen": 0}

    def progress(page, total_pages, users_seen):
        status.update(page=page, total_pages=total_pages, users_seen=users_seen)

    def run(kind):
        try:
            (sync.full_reconcile if kind == "full" else sync.delta_sync)(progress)
        finally:
            status["running"] = False
            lock.release()

    @app.get("/", response_class=HTMLResponse)
    def index():
        return PAGE

    @app.post("/api/sync")
    def start(body: dict):
        if not lock.acquire(blocking=False):
            raise HTTPException(409, "a sync is already running")
        status.update(running=True, kind=body.get("kind", "delta"), page=0, total_pages=0, users_seen=0)
        threading.Thread(target=run, args=(status["kind"],), daemon=True).start()
        return {"started": status["kind"]}

    @app.get("/api/sync/status")
    def sync_status():
        runs = sync.storage.runs()
        return {**status, "last_run": runs[-1] if runs else None}

    @app.get("/api/users")
    def list_users(q: str = "", page: int = 0):
        rows, total = sync.storage.page(q, 50, page * 50)
        return {"users": rows, "total": total}

    @app.get("/api/users/{external_id}")
    def get_user(external_id: str):
        user = sync.storage.get(external_id)
        if user is None:
            raise HTTPException(404)
        return user

    return app


def app_factory():
    http = httpx.Client(base_url=os.environ.get("PROVIDER_URL", "http://localhost:9000"), timeout=30)
    storage = Storage(os.environ.get("DB_PATH", ":memory:"))
    return create_app(DirectorySync(http, storage, SystemClock()))
