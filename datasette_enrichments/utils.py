import asyncio
import secrets
from typing import Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from datasette.app import Datasette


async def get_with_auth(datasette, *args, **kwargs):
    if not hasattr(datasette, "_secret_enrichments_token"):
        datasette._secret_enrichments_token = secrets.token_hex(16)
    headers = kwargs.pop("headers", None) or {}
    headers["x-datasette-enrichments"] = datasette._secret_enrichments_token
    kwargs["headers"] = headers
    return await datasette.client.get(*args, **kwargs)


class WaitForJobException(Exception):
    def __init__(self, job_id, msg):
        self.job_id = job_id
        self.msg = msg

    def __repr__(self):
        return "<WaitForJobException {}: {}>".format(self.job_id, self.msg)


async def wait_for_job(
    datasette: "Datasette",
    job_id: Union[int, str],
    database: Optional[str] = None,
    timeout=None,
):
    "Returns when the job has completed, using an asyncio.Event"
    job_id = int(job_id)
    db = datasette.get_database(database) if database else datasette.get_database()
    _ensure_enrichment_properties(datasette)
    completed_jobs = datasette._enrichment_completed_jobs
    if (db.name, job_id) in completed_jobs:
        return
    # Now look it up in the database
    job = (
        await db.execute("select status from _enrichment_jobs where id = ?", (job_id,))
    ).first()
    if job is None:
        raise WaitForJobException(job_id, "Job not found")
    if job["status"] == "finished":
        datasette._enrichment_completed_jobs.add((db.name, job_id))
        return
    # Otherwise wait for it to complete
    event = datasette._enrichment_completed_events.get((db.name, job_id))
    if not event:
        event = asyncio.Event()
        datasette._enrichment_completed_events[(db.name, job_id)] = event
    if timeout is None:
        await event.wait()
    else:
        await asyncio.wait_for(event.wait(), timeout)


async def mark_job_complete(
    datasette: "Datasette", job_id: int, database: Optional[str] = None
):
    _ensure_enrichment_properties(datasette)
    if not database:
        database = datasette.get_database().name
    event = None
    if (database, job_id) in datasette._enrichment_completed_events:
        event = datasette._enrichment_completed_events.get((database, job_id))
    datasette._enrichment_completed_jobs.add((database, job_id))
    if event is not None:
        event.set()


def _ensure_enrichment_properties(datasette: "Datasette"):
    if not hasattr(datasette, "_enrichment_completed_jobs"):
        datasette._enrichment_completed_jobs = set()
    if not hasattr(datasette, "_enrichment_completed_events"):
        datasette._enrichment_completed_events = {}


def pks_for_rows(rows, pks):
    if not pks:
        pks = ["rowid"]
    is_single = len(pks) == 1
    if is_single:
        pk = pks[0]
        return [row[pk] for row in rows]
    else:
        return [tuple(row[pk] for pk in pks) for row in rows]
