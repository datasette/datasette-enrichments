from abc import ABC, abstractmethod
import asyncio
from datasette import hookimpl
from datasette.utils import async_call_with_supported_arguments, tilde_encode
import json
import secrets
import traceback
import urllib
from datasette.plugins import pm
from .views import enrichment_picker, enrichment_view
from .utils import get_with_auth, mark_job_complete, pks_for_rows
from . import hookspecs

from datasette.utils import await_me_maybe

from typing import TYPE_CHECKING, Any, Tuple, Union, List

if TYPE_CHECKING:
    from datasette.app import Datasette
    from datasette.database import Database

pm.add_hookspecs(hookspecs)


IdType = Union[int, str, Tuple[Union[int, str], ...]]


async def get_enrichments(datasette):
    enrichments = []
    for result in pm.hook.register_enrichments(datasette=datasette):
        result = await await_me_maybe(result)
        enrichments.extend(result)
    return {enrichment.slug: enrichment for enrichment in enrichments}


CREATE_JOB_TABLE_SQL = """
create table if not exists _enrichment_jobs (
    id integer primary key,
    status text, -- pending, running, cancelled, finished
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

CREATE_ERROR_TABLE_SQL = """
create table if not exists _enrichment_errors (
    id integer primary key,
    job_id integer references _enrichment_jobs(id),
    created_at text,
    row_pks text, -- JSON list of row primary keys
    error text
)
""".strip()


class Enrichment(ABC):
    batch_size: int = 100
    # Cancel run after this many errors
    default_max_errors: int = 5
    log_traceback: bool = False

    @property
    @abstractmethod
    def slug(self):
        # A unique short text identifier for this enrichment
        ...

    @property
    @abstractmethod
    def name(self):
        # The name of this enrichment
        ...

    description = ""  # Short description of this enrichment

    def __repr__(self):
        return "<Enrichment: {}>".format(self.slug)

    async def log_error(
        self, db: "Database", job_id: int, ids: List[IdType], error: str
    ):
        if self.log_traceback:
            error += "\n\n" + traceback.format_exc()
        # Record error and increment error_count
        await db.execute_write(
            """
            insert into _enrichment_errors (job_id, row_pks, error)
            values (?, ?, ?)
        """,
            (job_id, json.dumps(ids), error),
        )
        await db.execute_write(
            """
            update _enrichment_jobs
            set error_count = error_count + ?
            where id = ?
        """,
            (len(ids), job_id),
        )

    async def get_config_form(self, datasette: "Datasette", db: "Database", table: str):
        return None

    async def initialize(
        self, datasette: "Datasette", db: "Database", table: str, config: dict
    ):
        pass

    async def finalize(
        self, datasette: "Datasette", db: "Database", table: str, config: dict
    ):
        pass

    @abstractmethod
    async def enrich_batch(
        self,
        datasette: "Datasette",
        db: "Database",
        table: str,
        rows: list,
        pks: list,
        config: dict,
        job_id: int,
    ):
        raise NotImplementedError

    async def increment_cost(
        self, db: "Database", job_id: int, total_cost_rounded_up: int
    ):
        await db.execute_write(
            """
            update _enrichment_jobs
            set cost_100ths_cent = cost_100ths_cent + ?
            where id = ?
            """,
            (total_cost_rounded_up, job_id),
        )

    async def enqueue(
        self,
        datasette: "Datasette",
        db: "Database",
        table: str,
        filter_querystring: str,
        config: dict,
        actor_id: str = None,
    ) -> int:
        # Enqueue a job
        qs = filter_querystring
        if qs:
            qs += "&"
        qs += "_size=0&_extra=count"
        table_path = datasette.urls.table(db.name, table)

        response = await get_with_auth(datasette, table_path + ".json" + "?" + qs)
        filtered_data = response.json()
        if "count" in filtered_data:
            row_count = filtered_data["count"]
        else:
            row_count = filtered_data["filtered_table_rows_count"]
        await db.execute_write(CREATE_JOB_TABLE_SQL)
        await db.execute_write(CREATE_ERROR_TABLE_SQL)

        def _insert(conn):
            with conn:
                cursor = conn.execute(
                    """
                    insert into _enrichment_jobs (
                        enrichment, status, database_name, table_name, filter_querystring,
                        config, started_at, row_count, error_count, done_count, cost_100ths_cent, actor_id
                    ) values (
                        :enrichment, 'pending', :database_name, :table_name, :filter_querystring, :config,
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
        await self.start_enrichment_in_process(datasette, db, job_id)
        return job_id

    async def start_enrichment_in_process(
        self, datasette: "Datasette", db: "Database", job_id: int
    ):
        loop = asyncio.get_event_loop()
        job_row = (
            await db.execute("select * from _enrichment_jobs where id = ?", (job_id,))
        ).first()
        if not job_row:
            return
        job = dict(job_row)

        async def run_enrichment():
            next_cursor = job["next_cursor"]
            # Set state to running
            await db.execute_write(
                """
                update _enrichment_jobs
                set status = 'running'
                where id = ?
                """,
                (job["id"],),
            )
            while True:
                # Get next batch
                table_path = datasette.urls.table(
                    job["database_name"], job["table_name"], format="json"
                )
                qs = job["filter_querystring"]
                if next_cursor:
                    qs += "&_next={}".format(next_cursor)
                qs += "&_size={}&_shape=objects".format(self.batch_size)
                response = await get_with_auth(datasette, table_path + "?" + qs)
                rows = response.json()["rows"]
                if not rows:
                    break
                # Enrich batch
                pks = await db.primary_keys(job["table_name"])
                try:
                    await async_call_with_supported_arguments(
                        self.enrich_batch,
                        datasette=datasette,
                        db=db,
                        table=job["table_name"],
                        rows=rows,
                        pks=pks or ["rowid"],
                        config=json.loads(job["config"]),
                        job_id=job_id,
                    )
                except Exception as ex:
                    await self.log_error(db, job_id, pks_for_rows(rows, pks), str(ex))
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
                            status = 'finished',
                            done_count = done_count + ?
                        where id = ?
                        """,
                        (len(rows), job["id"]),
                    )
                    await async_call_with_supported_arguments(
                        self.finalize,
                        datasette=datasette,
                        db=db,
                        table=job["table_name"],
                        config=json.loads(job["config"]),
                    )
                    await mark_job_complete(datasette, job["id"], job["database_name"])
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
                            tilde_encode(table),
                            (
                                "?{}".format(request.query_string)
                                if request.query_string
                                else ""
                            ),
                        )
                    ),
                    "label": "Enrich selected data",
                    "description": "Run a data cleaning operation against every selected row",
                }
            ]

    return inner


@hookimpl
def row_actions(datasette, database, table, actor, row):
    async def inner():
        if await datasette.permission_allowed(
            actor, "enrichments", resource=database, default=False
        ):
            # query_string to select row based on its primary keys
            db = datasette.get_database(database)
            pks = await db.primary_keys(table)
            if not pks:
                pks = ["rowid"]
            # Build the querystring to select this row
            bits = []
            for pk in pks:
                if pk.startswith("_"):
                    bits.append((pk + "__exact", row[pk]))
                else:
                    bits.append((pk, row[pk]))
            query_string = urllib.parse.urlencode(bits)
            return [
                {
                    "href": datasette.urls.path(
                        "/-/enrich/{}/{}?{}".format(
                            database,
                            tilde_encode(table),
                            query_string,
                        )
                    ),
                    "label": "Enrich this row",
                }
            ]

    return inner


@hookimpl
def permission_allowed(actor, action):
    # Special actor used for internal datasette.client.get() calls
    if actor == {"_datasette_enrichments": True}:
        return True
    # Root user can always use enrichments
    if action == "enrichments" and actor and actor.get("id") == "root":
        return True


@hookimpl(tryfirst=True)
def actor_from_request(datasette, request):
    secret_token = request.headers.get("x-datasette-enrichments") or ""
    expected_token = getattr(datasette, "_secret_enrichments_token", None)
    if expected_token and secrets.compare_digest(
        secret_token, datasette._secret_enrichments_token
    ):
        return {"_datasette_enrichments": True}


PROGRESS_JS = """
const endpoint = 'ENDPOINT';
const pollInterval = 2000;

let lastPolledTime = Date.now();
let lastDone = 0;
let intervalId;

// Function to create and insert progress bar elements
function setupProgressBar() {
    // Create elements
    const progressBarWrapper = document.createElement('div');
    const progressBar = document.createElement('div');
    const etaText = document.createElement('p');
    const etaSpan = document.createElement('span');

    // Set attributes and styles
    progressBarWrapper.id = 'progressBarWrapper';
    progressBar.id = 'progressBar';
    progressBar.style.width = '0%';
    progressBar.style.height = '20px';
    progressBar.style.backgroundColor = 'green';
    etaSpan.id = 'eta';
    etaText.innerText = 'ETA: ';
    etaText.appendChild(etaSpan);

    // Append elements
    progressBarWrapper.appendChild(progressBar);
    progressBarWrapper.appendChild(etaText);

    // Insert elements into the DOM
    const table = document.querySelector('table.rows-and-columns');
    if (table) {
        table.parentNode.insertBefore(progressBarWrapper, table);
    } else {
        console.error('Table not found.');
    }
}

function updateProgress() {
    fetch(endpoint)
        .then(response => response.json())
        .then(data => {
            const row = data.rows[0]
            const todo = row.row_count;
            const done = row.done_count;
            const total = todo + done;
            const progressPercent = (done / total) * 100;

            // Update progress bar
            document.getElementById('progressBar').style.width = `${progressPercent}%`;

            // Check if there are remaining tasks
            const tasksRemaining = total - done;
            if (tasksRemaining <= 0) {
                // Stop polling when no tasks remain and update ETA to "Completed"
                clearInterval(intervalId);
                document.getElementById('eta').innerText = 'Completed';
                return;
            }

            // Calculate ETA
            const currentTime = Date.now();
            const timeElapsed = currentTime - lastPolledTime;
            const tasksCompleted = done - lastDone;

            if (tasksCompleted > 0) {
                const rate = tasksCompleted / timeElapsed; // tasks per millisecond
                const timeRemaining = tasksRemaining / rate;
                const eta = new Date(currentTime + timeRemaining);

                // Update ETA display
                document.getElementById('eta').innerText = eta.toLocaleTimeString();
            }

            lastPolledTime = currentTime;
            lastDone = done;
        })
        .catch(error => console.error('Error fetching data:', error));
}

// Setup progress bar and initiate polling
setupProgressBar();
updateProgress();
intervalId = setInterval(updateProgress, pollInterval);
""".strip()


@hookimpl
def extra_body_script(request, view_name, table, database, datasette):
    if view_name != "table":
        return
    job_id = request.args.get("_enrichment_job_id")
    if not job_id:
        return

    async def inner():
        # Are there any incomplete jobs for this table?
        db = datasette.get_database(database)
        try:
            jobs = await db.execute(
                """
                select id, status, done_count, row_count
                from _enrichment_jobs
                where table_name = ?
            """,
                (table,),
            )
            row = jobs.first()
            if not row:
                return
        except Exception:
            return
        if row["done_count"] < row["row_count"]:
            return PROGRESS_JS.replace(
                "ENDPOINT",
                datasette.urls.path(
                    datasette.urls.table(database, "_enrichment_jobs")
                    + "?"
                    + urllib.parse.urlencode(
                        {
                            "database_name": database,
                            "table_name": table,
                        }
                    )
                ),
            )

    return inner
