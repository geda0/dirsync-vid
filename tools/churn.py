import argparse
import random
import warnings
from datetime import datetime, timedelta, timezone

warnings.filterwarnings("ignore")

from fastapi.testclient import TestClient

from app.clock import FakeClock
from app.sync import DirectorySync
from app.storage import FIELDS, Storage
from provider.server import create_provider_app
from provider.store import DEPARTMENTS, TITLES, Store, seed_users

START = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
CYCLE_S = 300


def provider_view(raw):
    p = raw["profile"]
    return (p["email"], p["firstName"], p["lastName"], p["department"], p["title"], raw["status"])


def directory_view(user):
    return tuple(getattr(user, f) for f in FIELDS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=30000)
    parser.add_argument("--cycles", type=int, default=12)
    parser.add_argument("--changes-per-cycle", type=int, default=60)
    parser.add_argument("--bulk-cycles", default="3,8")
    parser.add_argument("--skew", type=float, default=0.0)
    parser.add_argument("--lag", type=float, default=0.0)
    parser.add_argument("--mode", choices=["delta", "full"], default="delta")
    parser.add_argument("--budget", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    clock = FakeClock(START)
    store = Store(clock, args.skew, args.lag, args.seed)
    store.load(seed_users(args.users, args.seed))
    app = create_provider_app(store, sim_latency_s=0.1)
    storage = Storage()
    sync = DirectorySync(TestClient(app), storage, clock)
    run = sync.full_reconcile if args.mode == "full" else sync.delta_sync
    bulk = {int(c) for c in args.bulk_cycles.split(",") if c}
    ids = list(store.ids)
    pending = {}
    staleness = []

    def mutate(cycle):
        clock.advance(rng.uniform(1, 3))
        user_id = rng.choice(ids)
        if rng.random() < 0.2:
            active = store.current(user_id)["status"] == "ACTIVE"
            store.set_status(user_id, "DEACTIVATED" if active else "ACTIVE")
        else:
            store.update(user_id, department=rng.choice(DEPARTMENTS), title=rng.choice(TITLES))
        pending.setdefault(user_id, cycle)

    def resolve(cycle):
        for user_id, since in list(pending.items()):
            local = storage.get(user_id)
            if local and directory_view(local) == provider_view(store.current(user_id)):
                staleness.append(cycle - since)
                del pending[user_id]

    sync.full_reconcile()
    for cycle in range(1, args.cycles + 1):
        clock.advance(max(0, (START + timedelta(seconds=CYCLE_S * cycle) - clock.now()).total_seconds()))
        if cycle in bulk:
            store.touch_all()
        for _ in range(args.changes_per_cycle // 2):
            mutate(cycle)
        pages = len(ids) // 200 if args.mode == "full" or cycle in bulk else 2
        for _ in range(args.changes_per_cycle - args.changes_per_cycle // 2):
            app.state.hooks.after_page(rng.randint(1, max(2, pages)), lambda s, c=cycle: mutate(c))
        run()
        app.state.hooks.clear()
        resolve(cycle)
    clock.advance(CYCLE_S)
    run()
    resolve(args.cycles + 1)

    local = storage.all()
    lost = [i for i in ids if i not in local or directory_view(local[i]) != provider_view(store.current(i))]
    lost_deactivations = [i for i in lost if store.current(i)["status"] == "DEACTIVATED"]
    per_hour = round(len(app.state.request_times) * 3600 / ((args.cycles + 1) * CYCLE_S))
    lines = [
        f"mode={args.mode} users={args.users} cycles={args.cycles} skew={args.skew:g}s lag={args.lag:g}s seed={args.seed}",
        f"lost_changes:            {len(lost)}",
        f"lost_deactivations:      {len(lost_deactivations)}",
        f"worst_staleness_cycles:  {'never synced' if pending else max(staleness, default=0)}",
        f"requests_per_hour:       {per_hour} (budget {args.budget}: {'OVER' if per_hour > args.budget else 'ok'})",
        f"audit_rows_written:      {storage.audit_count()}",
    ]
    text = "\n".join(lines)
    print(text)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
