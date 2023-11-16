import asyncio
from datasette import hookimpl
from datasette.database import Database
import json
from datasette.plugins import pm
from .views import enrichment_picker, enrichment_view
from . import hookspecs

from datasette.utils import await_me_maybe


pm.add_hookspecs(hookspecs)


async def get_enrichments(datasette):
    enrichments = []
    for result in pm.hook.register_enrichments(datasette=datasette):
        result = await await_me_maybe(result)
        enrichments.extend(result)
    return {enrichment.slug: enrichment for enrichment in enrichments}


CREATE_JOB_TABLE_SQL = """
create table if not exists _enrichment_jobs (
    id integer primary key,
    status text, -- [p]ending, [r]unning, [c]ancelled, [f]inished
    enrichment text, -- slug of enrichment
    database_name text,
    table_name text,
    filter_querystring text, -- querystring used to filter rows
    config text, -- JSON dictionary of config
    started_at text, -- ISO8601 when added
    finished_at text, -- ISO8601 when completed or cancelled
    cancel_reason text, -- null or reason for cancellation
    next_cursor text, -- next cursor to fetch
    row_count integer, -- number of rows to enrich at start
    error_count integer, -- number of rows with errors encountered
    done_count integer, -- number of rows processed
    actor_id text, -- optional ID of actor who created the job
    cost_100ths_cent integer -- cost of job so far in 1/100ths of a cent
)
""".strip()


class Enrichment:
    batch_size = 100
    runs_in_process = False
    # Cancel run after this many errors
    default_max_errors = 5

    def __repr__(self):
        return "<Enrichment: {}>".format(self.slug)

    async def get_config_form(self, db: Database, table: str):
        return None

    async def initialize(self, db, table, config):
        pass

    async def increment_cost(self, db, job_id, total_cost_rounded_up):
        await db.execute_write(
            """
            update _enrichment_jobs
            set cost_100ths_cent = cost_100ths_cent + ?
            where id = ?
            """,
            (total_cost_rounded_up, job_id),
        )

    async def enqueue(
        self, datasette, db, table, filter_querystring, config, actor_id=None
    ):
        # Enqueue a job
        qs = filter_querystring
        if qs:
            qs += "&"
        qs += "_size=0&_extra=count"
        table_path = datasette.urls.table(db.name, table)
        response = await datasette.client.get(table_path + ".json" + "?" + qs)
        row_count = response.json()["count"]
        await db.execute_write(CREATE_JOB_TABLE_SQL)

        def _insert(conn):
            with conn:
                cursor = conn.execute(
                    """
                    insert into _enrichment_jobs (
                        enrichment, status, database_name, table_name, filter_querystring,
                        config, started_at, row_count, error_count, done_count, cost_100ths_cent, actor_id
                    ) values (
                        :enrichment, 'p', :database_name, :table_name, :filter_querystring, :config,
                        datetime('now'), :row_count, 0, 0, 0{}
                    )
                """.format(
                        ", :actor_id" if actor_id else ", null"
                    ),
                    {
                        "enrichment": self.slug,
                        "database_name": db.name,
                        "table_name": table,
                        "filter_querystring": filter_querystring,
                        "config": json.dumps(config or {}),
                        "row_count": row_count,
                        "actor_id": actor_id,
                    },
                )
            return cursor.lastrowid

        job_id = await db.execute_write_fn(_insert)
        if self.runs_in_process:
            await self.start_enrichment_in_process(datasette, db, job_id)

    async def start_enrichment_in_process(self, datasette, db, job_id):
        loop = asyncio.get_event_loop()
        job_row = (
            await db.execute("select * from _enrichment_jobs where id = ?", (job_id,))
        ).first()
        if not job_row:
            return
        job = dict(job_row)

        async def run_enrichment():
            next_cursor = job["next_cursor"]
            while True:
                # Get next batch
                table_path = datasette.urls.table(
                    job["database_name"], job["table_name"], format="json"
                )
                qs = job["filter_querystring"]
                if next_cursor:
                    qs += "&_next={}".format(next_cursor)
                qs += "&_size={}".format(self.batch_size)
                response = await datasette.client.get(table_path + "?" + qs)
                rows = response.json()["rows"]
                if not rows:
                    break
                # Enrich batch
                pks = await db.primary_keys(job["table_name"])
                await self.enrich_batch(
                    db, job["table_name"], rows, pks, json.loads(job["config"]), job_id
                )
                # Update next_cursor
                next_cursor = response.json()["next"]
                if next_cursor:
                    await db.execute_write(
                        """
                        update _enrichment_jobs
                        set
                            next_cursor = ?,
                            done_count = done_count + ?
                        where id = ?
                        """,
                        (next_cursor, len(rows), job["id"]),
                    )
                else:
                    # Mark complete
                    await db.execute_write(
                        """
                        update _enrichment_jobs
                        set
                            finished_at = datetime('now'),
                            status = 'f',
                            done_count = done_count + ?
                        where id = ?
                        """,
                        (len(rows), job["id"]),
                    )
                    break

        loop.create_task(run_enrichment())


@hookimpl
def register_routes():
    return [
        (r"^/-/enrich/(?P<database>[^/]+)/(?P<table>[^/]+)$", enrichment_picker),
        (
            r"^/-/enrich/(?P<database>[^/]+)/(?P<table>[^/]+)/(?P<enrichment>[^/]+)$",
            enrichment_view,
        ),
    ]


@hookimpl
def table_actions(datasette, actor, database, table, request):
    async def inner():
        if await datasette.permission_allowed(
            actor, "enrichments", resource=database, default=False
        ):
            return [
                {
                    "href": datasette.urls.path(
                        "/-/enrich/{}/{}{}".format(
                            database,
                            table,
                            "?{}".format(request.query_string)
                            if request.query_string
                            else "",
                        )
                    ),
                    "label": "Enrich selected data",
                }
            ]

    return inner


@hookimpl
def permission_allowed(actor, action):
    # Root user can always use enrichments
    if action == "enrichments" and actor and actor.get("id") == "root":
        return True
