import asyncio
from datasette_enrichments.utils import wait_for_job
from datasette.app import Datasette
from datasette.utils import tilde_encode
import pytest, pytest_asyncio
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
        db.execute("create table [foo/bar] (s text)")
        db.execute("insert into [foo/bar] (s) values ('one')")
        db.execute("insert into [foo/bar] (s) values ('two')")
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
