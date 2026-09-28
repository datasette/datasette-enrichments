import asyncio
import html
import inspect
import random
import re
import sqlite3
import time

import pytest
from datasette import version
from datasette.utils import tilde_encode
from packaging.version import parse

from datasette_enrichments.utils import wait_for_job


@pytest.mark.asyncio
@pytest.mark.parametrize("is_root", [True, False])
@pytest.mark.parametrize("table", ("t", "rowid_table", "foo/bar"))
async def test_uppercase_plugin(datasette, is_root, table):
    encoded_table = tilde_encode(table)
    if not is_root:
        response1 = await datasette.client.get(f"/-/enrich/data/{encoded_table}")
        assert response1.status_code == 403
        return

    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get(
        f"/-/enrich/data/{encoded_table}", cookies=cookies
    )
    assert response1.status_code == 200
    assert (
        f'<a href="/-/enrich/data/{encoded_table}/uppercasedemo">Convert to uppercase</a>'
        in response1.text
    )

    response2 = await datasette.client.get(
        f"/-/enrich/data/{encoded_table}/uppercasedemo", cookies=cookies
    )
    assert "<h2>Convert to uppercase</h2>" in response2.text

    # Now try and run it
    assert not hasattr(datasette, "_initialize_called_with")

    response3 = await datasette.client.post(
        f"/-/enrich/data/{encoded_table}/uppercasedemo",
        cookies=cookies,
        data={"columns": "s"},
    )
    assert response3.status_code == 302
    assert response3.headers["location"].startswith(
        f"/data/{encoded_table}?_enrichment_job="
    )
    # It should be queued up
    job_id = response3.headers["location"].split("=")[-1]
    status, enrichment, database_name, table_name, config = datasette._test_db.execute(
        """
        select status, enrichment, database_name, table_name, config
        from _enrichment_jobs where id = ?
    """,
        (job_id,),
    ).fetchone()
    assert status == "pending"
    assert enrichment == "uppercasedemo"
    assert database_name == "data"
    assert table_name == table
    assert config == '{"columns": "s"}'
    # Wait a moment and it should start running
    await wait_until(
        lambda: get_status(datasette, job_id) == "running",
        "enrichment to start running",
    )
    assert hasattr(datasette, "_initialize_called_with")
    assert not hasattr(datasette, "_finalize_called_with")

    await wait_for_job(datasette, job_id, database_name, timeout=5)
    assert hasattr(datasette, "_finalize_called_with"), "Enrichment did not complete"


@pytest.mark.asyncio
async def test_error_log(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    datasette._trigger_enrich_batch_error = True
    response = await datasette.client.post(
        "/-/enrich/data/t/uppercasedemo",
        cookies=cookies,
        data={"columns": "s"},
    )
    assert response.status_code == 302
    job_id = response.headers["location"].split("=")[-1]
    # Wait for it to finish, should populate error table
    await wait_for_job(datasette, job_id, "data", timeout=5)
    errors = datasette._test_db.execute(
        "select job_id, row_pks, error from _enrichment_errors"
    ).fetchall()
    assert errors == [(int(job_id), "[1, 2]", "Error in enrich_batch()")]
    # Should have recorded errors on the job itself
    job_details = datasette._test_db.execute(
        "select error_count, done_count from _enrichment_jobs where id = ?", (job_id,)
    ).fetchone()
    assert job_details == (2, 2)


@pytest.mark.asyncio
@pytest.mark.skipif(
    parse(version.__version__) < parse("1.0a13"),
    reason="uses row_actions() plugin hook",
)
@pytest.mark.parametrize(
    "path,expected_path",
    (
        ("/data/t/1", "t?id=1"),
        ("/data/rowid_table/1", "rowid_table?rowid=1"),
        ("/data/compound_pk_table/dog,a", "compound_pk_table?category=dog&amp;name=a"),
        ("/data/foo~2Fbar/1", "foo~2Fbar?_id__exact=1"),
    ),
)
async def test_row_actions(datasette, path, expected_path):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response = await datasette.client.get(path, cookies=cookies)
    assert response.status_code == 200
    m = re.search(
        r'<a href="(/-/enrich/data/[^"]+)"[^>]*>Enrich this row', response.text
    )
    assert m, response.text
    assert m.group(1) == f"/-/enrich/data/{expected_path}"
    # And check that page offers to enrich just one row
    enrich_path = html.unescape(m.group(1))
    enrich_page_response = await datasette.client.get(enrich_path, cookies=cookies)
    assert enrich_page_response.status_code == 200
    assert "1 row selected" in enrich_page_response.text


@pytest.mark.asyncio
async def test_job_listings(datasette):
    "Test /-/enrich/data/-/jobs and /-/enrich/data/-/jobs/18 and database action button"
    # /-/enrich/data/-/jobs requires auth
    response = await datasette.client.get("/-/enrich/data/-/jobs")
    assert response.status_code == 403
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response2 = await datasette.client.get("/-/enrich/data/-/jobs", cookies=cookies)
    # The table doesn't exist yet, but this should still return 200
    assert response2.status_code == 200
    assert (
        "No enrichment jobs have been run against this database yet" in response2.text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ("env", "user-input"))
async def test_enrichment_using_secret(datasette, scenario, monkeypatch):
    if scenario == "env":
        monkeypatch.setenv("DATASETTE_SECRETS_STRING_SECRET", "env-secret")

    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get("/-/enrich/data/t", cookies=cookies)
    assert response1.status_code == 200
    assert (
        '<a href="/-/enrich/data/t/secretreplace">Replace string with a secret</a>'
        in response1.text
    )
    response2 = await datasette.client.get(
        "/-/enrich/data/t/secretreplace", cookies=cookies
    )
    assert "<h2>Replace string with a secret</h2>" in response2.text

    # name="enrichment_secret" should be present only if not set in env
    if scenario == "env":
        assert ' name="enrichment_secret"' not in response2.text
    else:
        assert ' name="enrichment_secret"' in response2.text

    # Now try and run it
    form_data = {"column": "s", "string": "hello"}
    if scenario == "user-input":
        form_data["enrichment_secret"] = "user-secret"

    response3 = await datasette.client.post(
        "/-/enrich/data/t/secretreplace",
        cookies=cookies,
        data=form_data,
    )
    assert response3.status_code == 302
    job_id = response3.headers["location"].split("=")[-1]

    # Wait for it to finish and check it worked
    await wait_for_job(datasette, job_id, "data", timeout=5)
    # Check for errors
    job_details = datasette._test_db.execute(
        "select error_count, done_count from _enrichment_jobs where id = ?", (job_id,)
    ).fetchone()
    assert job_details == (0, 2)
    # Check rows show enrichment ran correctly
    rows = datasette._test_db.execute("select s from t order by id").fetchall()
    if scenario == "env":
        assert rows == [("env-secret",), ("goodbye",)]
    else:
        assert rows == [("user-secret",), ("goodbye",)]


@pytest.mark.asyncio
async def test_enrichment_with_no_config_form(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get("/-/enrich/data/t", cookies=cookies)
    assert response1.status_code == 200
    assert (
        '<a href="/-/enrich/data/t/hashrows">Calculate a hash for each row</a>'
        in response1.text
    )
    response2 = await datasette.client.get("/-/enrich/data/t/hashrows", cookies=cookies)
    assert "<h2>Calculate a hash for each row</h2>" in response2.text

    # Now try and run it
    form_data = {}

    response3 = await datasette.client.post(
        "/-/enrich/data/t/hashrows",
        cookies=cookies,
        data=form_data,
    )
    assert response3.status_code == 302
    job_id = response3.headers["location"].split("=")[-1]

    # Wait for it to finish and check it worked
    await wait_for_job(datasette, job_id, "data", timeout=5)

    job_details = datasette._test_db.execute(
        "select error_count, done_count from _enrichment_jobs where id = ?", (job_id,)
    ).fetchone()
    assert job_details == (0, 2)
    # Check rows show enrichment ran correctly
    rows = datasette._test_db.execute("select sha_256 from t order by id").fetchall()
    # Should be two 64 length strings
    assert len(rows[0][0]) == 64
    assert len(rows[1][0]) == 64


@pytest.mark.asyncio
async def test_enrichment_with_errors(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get(
        "/-/enrich/data/has_50_rows/haserrors", cookies=cookies
    )
    assert "<h2>8 success then 2 errors, repeated</h2>" in response1.text

    form_data = {}

    response2 = await datasette.client.post(
        "/-/enrich/data/has_50_rows/haserrors",
        cookies=cookies,
        data=form_data,
    )
    assert response2.status_code == 302
    job_id = response2.headers["location"].split("=")[-1]

    # Wait for it to finish and check it worked
    await wait_for_job(datasette, job_id, "data", timeout=5)

    # Check for errors
    errors = datasette._test_db.execute(
        "select job_id, row_pks, error from _enrichment_errors"
    ).fetchall()
    assert errors == [
        (1, "[9, 10]", "Error"),
        (1, "[19, 20]", "Error"),
        (1, "[29, 30]", "Error"),
        (1, "[39, 40]", "Error"),
        (1, "[49, 50]", "Error"),
    ]
    # Check _enrichment_progress has the right sequence of events
    progress = datasette._test_db.execute(
        "select success_count, error_count from _enrichment_progress where job_id = ? order by id",
        (job_id,),
    ).fetchall()
    assert progress == [
        (0, 2),
        (8, 0),
        (0, 2),
        (8, 0),
        (0, 2),
        (8, 0),
        (0, 2),
        (8, 0),
        (0, 2),
        (8, 0),
    ]
    # Should add up to 50
    assert sum([x[0] + x[1] for x in progress]) == 50

    # Check that the job status API works
    response3 = await datasette.client.get(
        f"/-/enrichment-jobs/data/{job_id}", cookies=cookies
    )
    assert response3.status_code == 200
    data = response3.json()
    assert data == {
        "total": 50,
        "title": f"Job {job_id}: 8 success then 2 errors, repeated",
        "url": f"/-/enrich/data/-/jobs/{job_id}",
        "is_complete": True,
        "sections": [
            {"type": "error", "count": 2},
            {"type": "success", "count": 8},
            {"type": "error", "count": 2},
            {"type": "success", "count": 8},
            {"type": "error", "count": 2},
            {"type": "success", "count": 8},
            {"type": "error", "count": 2},
            {"type": "success", "count": 8},
            {"type": "error", "count": 2},
            {"type": "success", "count": 8},
        ],
    }


@pytest.mark.asyncio
async def test_enrichments_start_on_startup(datasette):
    # Add a partially complete enrichment to the table
    db = datasette.get_database("data")
    from datasette_enrichments import ensure_tables

    job_id = random.randint(1000, 100000)

    await ensure_tables(db)
    await db.execute_write(
        """
        insert into _enrichment_jobs (
            id, status, enrichment, database_name, table_name, filter_querystring, config,
            next_cursor, done_count, error_count
        ) values (
            ?, 'running', 'haserrors', 'data', 'has_50_rows', '', '{}', '40', 40, 0
        )
    """,
        (job_id,),
    )
    # No datasette.client request has been made yet, so background tasks have
    # not launched. Launching them runs the restart task, which resumes the job
    await datasette.start_background_tasks()
    await wait_for_job(datasette, job_id, "data", timeout=5)
    # Check that the enrichment is now complete
    row = dict(
        (
            await db.execute("select * from _enrichment_jobs where id = ?", (job_id,))
        ).first()
    )
    assert row["status"] == "finished"
    assert row["done_count"] == 50
    # The restart ran as a supervised one-shot background task
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    tasks = (await datasette.client.get("/-/tasks.json", cookies=cookies)).json()
    restart = [
        t for t in tasks["tasks"] if t["name"] == "datasette-enrichments-restart"
    ]
    assert len(restart) == 1
    assert restart[0]["state"] == "completed"


def get_status(datasette, job_id):
    return datasette._test_db.execute(
        "select status from _enrichment_jobs where id = ?", (job_id,)
    ).fetchone()[0]


def get_progress_rows(datasette, job_id):
    cursor = datasette._test_db.cursor()
    cursor.row_factory = sqlite3.Row
    return [
        dict(row)
        for row in cursor.execute(
            """
            select job_id, success_count, error_count, message
            from _enrichment_progress where job_id = ? order by id
            """,
            (job_id,),
        )
    ]


async def wait_until(condition, description, timeout=5):
    "Poll condition (sync or async callable) until it returns truthy"
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() > deadline:
            raise AssertionError(
                f"Timed out after {timeout}s waiting for {description}"
            )
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_enrichments_pause_resume_cancel_buttons(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get(
        "/-/enrich/data/has_50_rows/queue", cookies=cookies
    )
    assert "<h2>Queue controlled enrichment</h2>" in response1.text

    form_data = {}

    response2 = await datasette.client.post(
        "/-/enrich/data/has_50_rows/queue",
        cookies=cookies,
        data=form_data,
    )
    job_id = int(response2.headers["location"].split("=")[-1])

    # Now feed it some results
    queue = datasette.enrichment_queue
    for i in range(3):
        await queue.put(str(i))

    async def three_rows_done():
        response = await datasette.client.get(
            f"/-/enrichment-jobs/data/{job_id}", cookies=cookies
        )
        return response.json()["sections"] == [{"type": "success", "count": 3}]

    await wait_until(three_rows_done, "3 rows to be processed")

    # Call the API and check that 3 are done
    response3 = await datasette.client.get(
        f"/-/enrichment-jobs/data/{job_id}", cookies=cookies
    )
    assert response3.status_code == 200
    data = response3.json()
    assert data == {
        "total": 50,
        "title": "Job 1: Queue controlled enrichment",
        "url": f"/-/enrich/data/-/jobs/{job_id}",
        "is_complete": False,
        "sections": [{"type": "success", "count": 3}],
    }

    # Now pause it
    response4 = await datasette.client.post(
        f"/-/enrich/data/-/jobs/{job_id}/pause", cookies=cookies, data=form_data
    )
    assert response4.status_code == 302
    assert get_status(datasette, job_id) == "paused"

    # And resume it
    response5 = await datasette.client.post(
        f"/-/enrich/data/-/jobs/{job_id}/resume",
        cookies=cookies,
        data=form_data,
    )
    assert response5.status_code == 302
    assert get_status(datasette, job_id) == "running"

    # And cancel it
    response6 = await datasette.client.post(
        f"/-/enrich/data/-/jobs/{job_id}/cancel",
        cookies=cookies,
        data=form_data,
    )
    assert response6.status_code == 302
    assert get_status(datasette, job_id) == "cancelled"

    # Check the messages were correctly logged
    rows = get_progress_rows(datasette, job_id)
    assert rows == [
        {
            "job_id": job_id,
            "success_count": 1,
            "error_count": 0,
            "message": None,
        },
        {
            "job_id": job_id,
            "success_count": 1,
            "error_count": 0,
            "message": None,
        },
        {
            "job_id": job_id,
            "success_count": 1,
            "error_count": 0,
            "message": None,
        },
        {
            "job_id": job_id,
            "success_count": 0,
            "error_count": 0,
            "message": "paused: by root",
        },
        {
            "job_id": job_id,
            "success_count": 0,
            "error_count": 0,
            "message": "running: by root",
        },
        {
            "job_id": job_id,
            "success_count": 0,
            "error_count": 0,
            "message": "cancelled: by root",
        },
    ]


@pytest.mark.asyncio
async def test_enrichments_pause_cancel_exceptions(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    form_data = {}
    response2 = await datasette.client.post(
        "/-/enrich/data/has_50_rows/queue",
        cookies=cookies,
        data=form_data,
    )
    job_id = int(response2.headers["location"].split("=")[-1])

    assert get_status(datasette, job_id) == "pending"
    await wait_until(
        lambda: get_status(datasette, job_id) != "pending", "job to leave pending"
    )
    assert get_status(datasette, job_id) == "running"

    # Now feed it three results and then pause then cancel
    queue = datasette.enrichment_queue
    for i in range(3):
        await queue.put(str(i))
    await queue.put("pause")
    # The status is updated before the progress row is written, so wait for
    # both: resuming too early would record "running" before "paused"
    await wait_until(
        lambda: len(get_progress_rows(datasette, job_id)) == 4,
        "3 rows and the pause to be recorded",
    )
    assert get_status(datasette, job_id) == "paused"

    # Resume it again
    await datasette.client.post(
        f"/-/enrich/data/-/jobs/{job_id}/resume",
        cookies=cookies,
        data=form_data,
    )

    # Now cancel it
    await queue.put("cancel")
    await wait_until(
        lambda: len(get_progress_rows(datasette, job_id)) == 6,
        "the cancellation to be recorded",
    )
    assert get_status(datasette, job_id) == "cancelled"

    rows = get_progress_rows(datasette, job_id)
    assert rows == [
        {"job_id": job_id, "success_count": 1, "error_count": 0, "message": None},
        {"job_id": job_id, "success_count": 1, "error_count": 0, "message": None},
        {"job_id": job_id, "success_count": 1, "error_count": 0, "message": None},
        {
            "job_id": job_id,
            "success_count": 0,
            "error_count": 0,
            "message": "paused: pause message",
        },
        {
            "job_id": job_id,
            "success_count": 0,
            "error_count": 0,
            "message": "running: by root",
        },
        {
            "job_id": job_id,
            "success_count": 0,
            "error_count": 0,
            "message": "cancelled: cancel message",
        },
    ]


@pytest.mark.asyncio
@pytest.mark.skipif(
    parse(version.__version__) < parse("1.0a13"),
    reason="uses datasette.Permission",
)
async def test_action_registered(datasette):
    action = datasette.actions.get("enrichments")
    assert action.name == "enrichments"
    assert action.resource_class.__name__ == "DatabaseResource"


def _live_job_tasks(datasette):
    registry = getattr(datasette, "_enrichment_job_tasks", {})
    return [t for t in registry.values() if not t.done()]


async def _insert_job(datasette, status, enrichment="countbatches"):
    # Insert a job row directly, without a datasette.client request (which
    # would launch the restart background task)
    from datasette_enrichments import ensure_tables

    db = datasette.get_database("data")
    await ensure_tables(db)
    return (
        await db.execute_write(
            """
            insert into _enrichment_jobs (
                status, enrichment, database_name, table_name, filter_querystring,
                config, done_count, error_count
            ) values (?, ?, 'data', 'has_50_rows', '', '{}', 0, 0)
            """,
            (status, enrichment),
        )
    ).lastrowid


async def _start_job(datasette, job_id, enrichment="countbatches"):
    from datasette_enrichments import get_enrichments

    enrichments = await get_enrichments(datasette)
    await enrichments[enrichment].start_enrichment_in_process(
        datasette, datasette.get_database("data"), job_id
    )


def _first_batch_in_flight(datasette, job_id):
    return (
        get_status(datasette, job_id) == "running"
        and getattr(datasette, "_enrich_in_flight", 0) == 1
    )


@pytest.mark.asyncio
async def test_pause_resume_mid_batch_does_not_run_two_loops(datasette):
    # Pausing and resuming while enrich_batch() is still running used to start a
    # second loop for the same job: every remaining row got enriched twice
    datasette._enrich_gate = asyncio.Event()
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response = await datasette.client.post(
        "/-/enrich/data/has_50_rows/countbatches", cookies=cookies, data={}
    )
    assert response.status_code == 302
    job_id = int(response.headers["location"].split("=")[-1])

    # Sample the registry throughout the run
    max_live = 0

    async def sample():
        nonlocal max_live
        while True:
            max_live = max(max_live, len(_live_job_tasks(datasette)))
            await asyncio.sleep(0.01)

    sampler = asyncio.create_task(sample())
    try:
        # The first batch is blocked on the gate, so pause + resume both land
        # while it is in flight
        await wait_until(
            lambda: _first_batch_in_flight(datasette, job_id), "first batch to start"
        )
        pause = await datasette.client.post(
            f"/-/enrich/data/-/jobs/{job_id}/pause", cookies=cookies, data={}
        )
        assert pause.status_code == 302
        resume = await datasette.client.post(
            f"/-/enrich/data/-/jobs/{job_id}/resume", cookies=cookies, data={}
        )
        assert resume.status_code == 302
        assert datasette._enrich_counts.get(11) is None, "first batch finished early"
        datasette._enrich_gate.set()
        await wait_for_job(datasette, job_id, "data", timeout=5)
    finally:
        sampler.cancel()
        await asyncio.gather(sampler, return_exceptions=True)

    assert get_status(datasette, job_id) == "finished"
    # Every row enriched exactly once, by one loop at a time
    assert datasette._enrich_counts == {i: 1 for i in range(1, 51)}
    assert datasette._enrich_max_in_flight == 1
    assert max_live <= 1


@pytest.mark.asyncio
async def test_stopping_loop_restarts_itself_if_resumed(datasette, monkeypatch):
    # The window the loop's re-read closes: the loop has read 'paused' and is
    # exiting, but is still registered, so a Resume landing now does not start
    # a new loop. The exiting loop must notice and restart the job itself.
    import datasette_enrichments

    real_forget = datasette_enrichments._forget_job_task
    resumed = []

    def forget_after_resume(registry, key, task):
        if not resumed:
            # First call is the stopping loop deregistering itself
            assert registry.get(key) is task and not task.done()
            # A Resume here would see this live task and do nothing more
            # than write the status
            with datasette._test_db:
                datasette._test_db.execute(
                    "update _enrichment_jobs set status = 'running'"
                )
            resumed.append(task)
        return real_forget(registry, key, task)

    monkeypatch.setattr(datasette_enrichments, "_forget_job_task", forget_after_resume)
    datasette._enrich_gate = asyncio.Event()
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response = await datasette.client.post(
        "/-/enrich/data/has_50_rows/countbatches", cookies=cookies, data={}
    )
    job_id = int(response.headers["location"].split("=")[-1])
    await wait_until(
        lambda: _first_batch_in_flight(datasette, job_id), "first batch to start"
    )
    pause = await datasette.client.post(
        f"/-/enrich/data/-/jobs/{job_id}/pause", cookies=cookies, data={}
    )
    assert pause.status_code == 302
    # Let the batch finish: the loop then sees 'paused' and exits
    datasette._enrich_gate.set()
    await wait_for_job(datasette, job_id, "data", timeout=5)

    assert resumed, "loop never deregistered itself"
    assert get_status(datasette, job_id) == "finished"
    assert datasette._enrich_counts == {i: 1 for i in range(1, 51)}
    assert datasette._enrich_max_in_flight == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("finished", "cancelled", "paused", "error"))
async def test_start_ignores_jobs_that_are_not_pending_or_running(datasette, status):
    # e.g. the restart task acting on a stale snapshot of 'running' jobs
    job_id = await _insert_job(datasette, status)
    await _start_job(datasette, job_id)
    assert _live_job_tasks(datasette) == []
    assert get_status(datasette, job_id) == status
    assert not hasattr(datasette, "_enrich_counts")


@pytest.mark.asyncio
async def test_cancel_before_loop_starts_wins(datasette):
    job_id = await _insert_job(datasette, "pending")
    await _start_job(datasette, job_id)
    tasks = _live_job_tasks(datasette)
    assert len(tasks) == 1
    # The task is created but has not run yet: cancel in the gap between the
    # status check and the loop's first write
    with datasette._test_db:
        datasette._test_db.execute(
            "update _enrichment_jobs set status = 'cancelled' where id = ?", (job_id,)
        )
    await asyncio.wait(tasks, timeout=5)
    assert get_status(datasette, job_id) == "cancelled"
    assert not hasattr(datasette, "_enrich_counts")
    # Cancelled is terminal, so waiters are released
    await wait_for_job(datasette, job_id, "data", timeout=5)


@pytest.mark.asyncio
async def test_finalize_exception_is_logged_and_does_not_hang(datasette, caplog):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response = await datasette.client.post(
        "/-/enrich/data/t/finalizeraises", cookies=cookies, data={}
    )
    assert response.status_code == 302
    job_id = int(response.headers["location"].split("=")[-1])
    # finalize() raising used to leave wait_for_job() waiting forever
    await wait_for_job(datasette, job_id, "data", timeout=5)
    assert get_status(datasette, job_id) == "error"
    records = [
        r
        for r in caplog.records
        if r.name == "datasette_enrichments" and r.levelname == "ERROR"
    ]
    assert len(records) == 1
    assert f"Enrichment job {job_id}" in records[0].getMessage()
    assert isinstance(records[0].exc_info[1], RuntimeError)
    # A fresh wait (no in-process event) also treats 'error' as terminal
    datasette._enrichment_completed_jobs.clear()
    await wait_for_job(datasette, job_id, "data", timeout=5)


@pytest.mark.asyncio
async def test_job_task_registry_drains(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response = await datasette.client.post(
        "/-/enrich/data/t/hashrows", cookies=cookies, data={}
    )
    job_id = int(response.headers["location"].split("=")[-1])
    registry = datasette._enrichment_job_tasks
    tasks = list(registry.values())
    assert len(tasks) == 1
    await wait_for_job(datasette, job_id, "data", timeout=5)
    # wait_for_job() returns just before the task itself finishes
    await asyncio.wait(tasks, timeout=5)
    assert registry == {}


@pytest.mark.asyncio
async def test_shutdown_cancels_job_tasks(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response = await datasette.client.post(
        "/-/enrich/data/has_50_rows/queue", cookies=cookies, data={}
    )
    job_id = int(response.headers["location"].split("=")[-1])
    # The queue enrichment blocks in enrich_batch() until fed
    await wait_until(
        lambda: get_status(datasette, job_id) == "running", "job to start running"
    )
    tasks = _live_job_tasks(datasette)
    assert len(tasks) == 1
    await datasette.invoke_shutdown()
    assert tasks[0].cancelled()
    assert datasette._enrichment_job_tasks == {}
    # Left as 'running' so the restart task resumes it on the next start
    assert get_status(datasette, job_id) == "running"


@pytest.mark.asyncio
async def test_no_new_job_tasks_once_shutdown_starts(datasette):
    # A stopping loop's self-restart, or the restart task, must not start a job
    # after the shutdown hook has taken its snapshot of tasks to cancel
    job_id = await _insert_job(datasette, "pending")
    datasette._enrichment_shutting_down = True
    await _start_job(datasette, job_id)
    assert _live_job_tasks(datasette) == []
    assert get_status(datasette, job_id) == "pending"
