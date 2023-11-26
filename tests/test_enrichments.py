import asyncio
from datasette.app import Datasette
import pytest
import sqlite3


@pytest.mark.asyncio
@pytest.mark.parametrize("is_root", [True, False])
async def test_uppercase_plugin(tmpdir, is_root):
    data = str(tmpdir / "data.db")
    db = sqlite3.connect(data)
    with db:
        db.execute("create table t (id integer primary key, s text)")
        db.execute("insert into t (s) values ('hello')")
        db.execute("insert into t (s) values ('goodbye')")
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

    if not is_root:
        response1 = await datasette.client.get("/-/enrich/data/t")
        assert response1.status_code == 403
        return

    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    response1 = await datasette.client.get("/-/enrich/data/t", cookies=cookies)
    assert (
        '<a href="/-/enrich/data/t/uppercasedemo?_enrichment=uppercasedemo">Convert to uppercase</a>'
        in response1.text
    )

    response2 = await datasette.client.get(
        "/-/enrich/data/t/uppercasedemo?_enrichment=uppercasedemo", cookies=cookies
    )
    assert "<h2>Convert to uppercase</h2>" in response2.text

    # Now try and run it
    csrftoken = response2.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken

    assert not hasattr(datasette, "_initialize_called_with")

    response3 = await datasette.client.post(
        "/-/enrich/data/t/uppercasedemo?_enrichment=uppercasedemo",
        cookies=cookies,
        data={"columns": "s", "csrftoken": csrftoken},
    )
    assert response3.status_code == 302
    assert response3.headers["location"] == "/data/t"
    # It should be queued up
    assert db.execute(
        "select status, enrichment, database_name, table_name, config from _enrichment_jobs"
    ).fetchall() == [("pending", "uppercasedemo", "data", "t", '{"columns": "s"}')]
    # Wait a moment and it should start running
    tries = 0
    ok = False
    while tries < 10:
        await asyncio.sleep(0.1)
        tries += 1
        if db.execute("select status from _enrichment_jobs").fetchall() == [
            ("running",)
        ]:
            ok = True
            break
    assert ok, "Enrichment did not start running"
    assert hasattr(datasette, "_initialize_called_with")
    assert not hasattr(datasette, "_finalize_called_with")
    # Wait for it to finish
    tries = 0
    ok2 = False
    while tries < 10:
        await asyncio.sleep(0.1)
        if hasattr(datasette, "_finalize_called_with"):
            ok2 = True
            break
    assert ok2, "Enrichment did not complete"
