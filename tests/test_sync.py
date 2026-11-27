from conftest import PAGE_1_USER, PAGE_3_USER, SEED


def test_initial_full_sync_imports_all_users(directory, provider, initial_sync):
    assert initial_sync["users_seen"] == len(SEED)
    assert initial_sync["pages"] == 3
    assert all(u.status == "ACTIVE" for u in directory.storage.all().values())
    assert directory.storage.cursor() == "2026-01-01T10:00:00Z"


def test_profile_change_between_syncs_is_synced(directory, provider, initial_sync, clock):
    provider.update(PAGE_1_USER, department="Legal", title="Counsel")
    clock.advance(300)
    directory.delta_sync()
    u = directory.storage.get(PAGE_1_USER)
    assert (u.department, u.title) == ("Legal", "Counsel")


def test_deactivation_between_syncs_is_synced(directory, provider, initial_sync, clock):
    provider.deactivate(PAGE_1_USER)
    clock.advance(300)
    directory.delta_sync()
    assert directory.storage.get(PAGE_1_USER).status == "DEACTIVATED"


def test_reactivation_between_syncs_is_synced(directory, provider, initial_sync, clock):
    provider.deactivate(PAGE_3_USER)
    clock.advance(300)
    directory.delta_sync()
    provider.activate(PAGE_3_USER)
    clock.advance(300)
    directory.delta_sync()
    assert directory.storage.get(PAGE_3_USER).status == "ACTIVE"


def test_new_user_between_syncs_is_imported(directory, provider, initial_sync, clock):
    created = provider.create(firstName="Jade", lastName="Lopez", department="Sales", title="SDR")
    clock.advance(300)
    directory.delta_sync()
    assert directory.storage.get(created["id"]).department == "Sales"


def test_change_on_later_page_during_sync_is_synced(directory, provider, initial_sync, clock):
    provider.bulk_import()
    provider.after_page(1, lambda s: provider.update(PAGE_3_USER, title="Director"))
    directory.delta_sync()
    clock.advance(300)
    directory.delta_sync()
    assert directory.storage.get(PAGE_3_USER).title == "Director"


def test_full_reconcile_repairs_drift(directory, provider, initial_sync, clock):
    directory.storage.execute("UPDATE users SET title = 'Wrong' WHERE external_id = ?", (PAGE_3_USER,))
    directory.full_reconcile()
    assert directory.storage.get(PAGE_3_USER).title == "Controller"


def test_sync_run_is_recorded(directory, provider, initial_sync, clock):
    provider.update(PAGE_1_USER, title="Lead Analyst")
    clock.advance(300)
    result = directory.delta_sync()
    runs = directory.storage.runs()
    assert [r["kind"] for r in runs] == ["full", "delta"]
    assert runs[-1]["cursor_saved"] == result["cursor_saved"]
    assert directory.storage.audit(PAGE_1_USER) == ["user.created", "user.updated"]
