import asyncio
from datasette_enrichments.utils import wait_for_job
from datasette.app import Datasette
from datasette.utils import tilde_encode
from datasette import version
from packaging.version import parse
import pytest
import pytest_asyncio
import random
import sqlite3


@pytest_asyncio.fixture
async def datasette(tmpdir):
    data = str(tmpdir / "data.db")
    db = sqlite3.connect(data)
    with db:
        db.execute("create table t (id integer primary key, s text)")
        db.execute("insert into t (s) values ('hello')")
        db.execute("insert into t (s) values ('goodbye')")
        db.execute("create table rowid_table (s text)")
        db.execute("insert into rowid_table (s) values ('one')")
        db.execute("insert into rowid_table (s) values ('two')")
        db.execute("create table [foo/bar] (_id integer primary key, s text)")
        db.execute("insert into [foo/bar] (_id, s) values (1, 'one')")
        db.execute("insert into [foo/bar] (_id, s) values (2, 'two')")
        db.execute(
            "create table compound_pk_table (category text, name text, value integer, primary key (category, name))"
        )
        db.execute(
            "insert into compound_pk_table (category, name, value) values ('dog', 'a', 34)"
        )
        db.execute("create table has_50_rows (id integer primary key, name text)")
        for i in range(50):
            db.execute("insert into has_50_rows (name) values (?)", (str(i),))

    datasette = Datasette(
        [data],
        metadata={
            "databases": {
                # Lock down permissions to test
                # https://github.com/datasette/datasette-enrichments/issues/13
                "data": {"allow": {"id": "root"}}
            }
        },
    )
    datasette._test_db = db
    await datasette.invoke_startup()
    return datasette


@pytest.mark.asyncio
@pytest.mark.parametrize("is_root", [True, False])
@pytest.mark.parametrize("table", ("t", "rowid_table", "foo/bar"))
async def test_uppercase_plugin(datasette, is_root, table):
    encoded_table = tilde_encode(table)
    if not is_root:
        response1 = await datasette.client.get(
            "/-/enrich/data/{}".format(encoded_table)
        )
        assert response1.status_code == 403
        return

    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get(
        "/-/enrich/data/{}".format(encoded_table), cookies=cookies
    )
    assert response1.status_code == 200
    assert (
        '<a href="/-/enrich/data/{}/uppercasedemo">Convert to uppercase</a>'.format(
            encoded_table
        )
        in response1.text
    )

    response2 = await datasette.client.get(
        "/-/enrich/data/{}/uppercasedemo".format(encoded_table), cookies=cookies
    )
    assert "<h2>Convert to uppercase</h2>" in response2.text

    # Now try and run it
    csrftoken = response2.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken

    assert not hasattr(datasette, "_initialize_called_with")

    response3 = await datasette.client.post(
        "/-/enrich/data/{}/uppercasedemo".format(encoded_table),
        cookies=cookies,
        data={"columns": "s", "csrftoken": csrftoken},
    )
    assert response3.status_code == 302
    assert response3.headers["location"].startswith(
        "/data/{}?_enrichment_job=".format(encoded_table)
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
    tries = 0
    ok = False
    while tries < 10:
        await asyncio.sleep(0.1)
        tries += 1
        status = datasette._test_db.execute(
            "select status from _enrichment_jobs"
        ).fetchall()[0][0]
        if status == "running":
            ok = True
            break

    assert ok, "Enrichment did not start running"
    assert hasattr(datasette, "_initialize_called_with")
    assert not hasattr(datasette, "_finalize_called_with")

    await wait_for_job(datasette, job_id, database_name, timeout=1)
    assert hasattr(datasette, "_finalize_called_with"), "Enrichment did not complete"


@pytest.mark.asyncio
async def test_error_log(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    csrftoken = (
        await datasette.client.get("/-/enrich/data/t/uppercasedemo", cookies=cookies)
    ).cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    datasette._trigger_enrich_batch_error = True
    response = await datasette.client.post(
        "/-/enrich/data/t/uppercasedemo",
        cookies=cookies,
        data={"columns": "s", "csrftoken": csrftoken},
    )
    assert response.status_code == 302
    job_id = response.headers["location"].split("=")[-1]
    # Wait for it to finish, should populate error table
    await wait_for_job(datasette, job_id, "data", timeout=1)
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
    assert (
        '<a href="/-/enrich/data/{}">Enrich this row'.format(expected_path)
        in response.text
    )
    # And check that page offers to enrich just one row
    enrich_path = "/-/enrich/data/" + response.text.split('<a href="/-/enrich/data/')[
        1
    ].split('">')[0].replace("&amp;", "&")
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
    csrftoken = response2.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken

    form_data = {"column": "s", "string": "hello", "csrftoken": csrftoken}
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
    await wait_for_job(datasette, job_id, "data", timeout=1)
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
    csrftoken = response2.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken

    form_data = {"csrftoken": csrftoken}

    response3 = await datasette.client.post(
        "/-/enrich/data/t/hashrows",
        cookies=cookies,
        data=form_data,
    )
    assert response3.status_code == 302
    job_id = response3.headers["location"].split("=")[-1]

    # Wait for it to finish and check it worked
    await wait_for_job(datasette, job_id, "data", timeout=1)

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

    csrftoken = response1.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    form_data = {"csrftoken": csrftoken}

    response2 = await datasette.client.post(
        "/-/enrich/data/has_50_rows/haserrors",
        cookies=cookies,
        data=form_data,
    )
    assert response2.status_code == 302
    job_id = response2.headers["location"].split("=")[-1]

    # Wait for it to finish and check it worked
    await wait_for_job(datasette, job_id, "data", timeout=1)

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
        "/-/enrichment-jobs/data/{}".format(job_id), cookies=cookies
    )
    assert response3.status_code == 200
    data = response3.json()
    assert data == {
        "total": 50,
        "title": "Job {}: 8 success then 2 errors, repeated".format(job_id),
        "url": "/-/enrich/data/-/jobs/{}".format(job_id),
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
    # First request to the app should cause that to finish
    await datasette.client.get("/")
    await wait_for_job(datasette, job_id, "data", timeout=1)
    # Check that the enrichment is now complete
    row = dict(
        (
            await db.execute("select * from _enrichment_jobs where id = ?", (job_id,))
        ).first()
    )
    assert row["status"] == "finished"
    assert row["done_count"] == 50


def get_status(datasette, job_id):
    return datasette._test_db.execute(
        "select status from _enrichment_jobs where id = ?", (job_id,)
    ).fetchone()[0]


@pytest.mark.asyncio
async def test_enrichments_pause_resume_cancel_buttons(datasette):
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get(
        "/-/enrich/data/has_50_rows/queue", cookies=cookies
    )
    assert "<h2>Queue controlled enrichment</h2>" in response1.text

    csrftoken = response1.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    form_data = {"csrftoken": csrftoken}

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

    await asyncio.sleep(0.1)

    # Call the API and check that 10 are done
    response3 = await datasette.client.get(
        "/-/enrichment-jobs/data/{}".format(job_id), cookies=cookies
    )
    assert response3.status_code == 200
    data = response3.json()
    assert data == {
        "total": 50,
        "title": "Job 1: Queue controlled enrichment",
        "url": "/-/enrich/data/-/jobs/{}".format(job_id),
        "is_complete": False,
        "sections": [{"type": "success", "count": 3}],
    }

    # Now pause it
    response4 = await datasette.client.post(
        "/-/enrich/data/-/jobs/{}/pause".format(job_id), cookies=cookies, data=form_data
    )
    assert response4.status_code == 302
    assert get_status(datasette, job_id) == "paused"

    # And resume it
    response5 = await datasette.client.post(
        "/-/enrich/data/-/jobs/{}/resume".format(job_id),
        cookies=cookies,
        data=form_data,
    )
    assert response5.status_code == 302
    assert get_status(datasette, job_id) == "running"

    # And cancel it
    response6 = await datasette.client.post(
        "/-/enrich/data/-/jobs/{}/cancel".format(job_id),
        cookies=cookies,
        data=form_data,
    )
    assert response6.status_code == 302
    assert get_status(datasette, job_id) == "cancelled"

    # Check the messages were correctly logged
    cursor = datasette._test_db.cursor()
    cursor.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in cursor.execute(
            """
            select job_id, success_count, error_count, message
            from _enrichment_progress where job_id = ? order by id
            """,
            (job_id,),
        )
    ]
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
    response1 = await datasette.client.get(
        "/-/enrich/data/has_50_rows/queue", cookies=cookies
    )
    csrftoken = response1.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    form_data = {"csrftoken": csrftoken}
    response2 = await datasette.client.post(
        "/-/enrich/data/has_50_rows/queue",
        cookies=cookies,
        data=form_data,
    )
    job_id = int(response2.headers["location"].split("=")[-1])

    assert get_status(datasette, job_id) == "pending"
    await asyncio.sleep(0.1)
    assert get_status(datasette, job_id) == "running"

    # Now feed it three results and then pause then cancel
    queue = datasette.enrichment_queue
    for i in range(3):
        await queue.put(str(i))
    await queue.put("pause")
    await asyncio.sleep(0.1)
    assert get_status(datasette, job_id) == "paused"

    # Resume it again
    await datasette.client.post(
        "/-/enrich/data/-/jobs/{}/resume".format(job_id),
        cookies=cookies,
        data=form_data,
    )

    # Now cancel it
    await queue.put("cancel")
    await asyncio.sleep(0.1)
    assert get_status(datasette, job_id) == "cancelled"

    cursor = datasette._test_db.cursor()
    cursor.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in cursor.execute(
            """
            select job_id, success_count, error_count, message
            from _enrichment_progress where job_id = ? order by id
            """,
            (job_id,),
        )
    ]
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
async def test_permission_registered(datasette):
    permission = datasette.get_permission("enrichments")
    assert permission.name == "enrichments"
    assert permission.takes_database
    assert not permission.takes_resource
