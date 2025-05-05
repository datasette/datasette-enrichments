from abc import ABC, abstractmethod
import asyncio
import datetime
from datasette import hookimpl

try:
    from datasette import Permission
except ImportError:
    Permission = None
from datasette.utils import async_call_with_supported_arguments, tilde_encode, sqlite3
from datasette_secrets import Secret, get_secret
import json
import secrets
import sys
import time
import traceback
import urllib
from datasette.plugins import pm
from markupsafe import Markup, escape
from . import views
from wtforms import PasswordField
from wtforms.validators import DataRequired
from .utils import get_with_auth, mark_job_complete, pks_for_rows
from urllib.parse import quote
from . import hookspecs

from datasette.utils import await_me_maybe

from typing import TYPE_CHECKING, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from datasette.app import Datasette
    from datasette.database import Database

pm.add_hookspecs(hookspecs)


IdType = Union[int, str, Tuple[Union[int, str], ...]]

# Custom epoch to save space in the _enrichment_progress table
JAN_1_2025_EPOCH = int(datetime.datetime(2025, 1, 1).timestamp() * 1000)


def ms_since_2025_to_datetime(ms_since_2025):
    unix_ms = JAN_1_2025_EPOCH + ms_since_2025
    unix_seconds = unix_ms / 1000
    return datetime.datetime.fromtimestamp(unix_seconds, tz=datetime.timezone.utc)


async def get_enrichments(datasette):
    enrichments = []
    for result in pm.hook.register_enrichments(datasette=datasette):
        result = await await_me_maybe(result)
        enrichments.extend(result)
    return {enrichment.slug: enrichment for enrichment in enrichments}


CREATE_JOB_TABLE_SQL = """
create table if not exists _enrichment_jobs (
    id integer primary key,
    status text, -- pending, running, cancelled, finished, paused
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

CREATE_PROGRESS_TABLE_SQL = """
create table if not exists _enrichment_progress (
    id integer primary key,
    job_id integer references _enrichment_jobs(id),
    timestamp_ms_2025 integer, -- milliseconds since 2025-01-01
    success_count integer,
    error_count integer,
    message text
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


async def ensure_tables(db):
    await db.execute_write(CREATE_JOB_TABLE_SQL)
    await db.execute_write(CREATE_PROGRESS_TABLE_SQL)
    await db.execute_write(CREATE_ERROR_TABLE_SQL)


async def set_job_status(
    db: "Database",
    job_id: int,
    status: str,
    allowed_statuses: Optional[Tuple[str]] = None,
    message: Optional[str] = None,
):
    if allowed_statuses:
        # First check the current status
        current_status = (
            await db.execute(
                "select status from _enrichment_jobs where id = ?", (job_id,)
            )
        ).first()[0]
        if current_status not in allowed_statuses:
            raise ValueError(
                f"Job {job_id} is in status {current_status}, not in {allowed_statuses}"
            )
    await db.execute_write(
        """
        update _enrichment_jobs
        set status = :status
        {}
        where id = :job_id
    """.format(
            ", cancel_reason = :cancel_reason"
            if (message and status == "cancelled")
            else ""
        ),
        {"status": status, "job_id": job_id, "cancel_reason": message},
    )
    progress_message = status
    if message:
        progress_message += ": " + message
    await record_progress(db, job_id, 0, 0, progress_message)


async def record_progress(db, job_id, success_count, error_count, message=""):
    await db.execute_write(
        """
        insert into _enrichment_progress (
            job_id, timestamp_ms_2025, success_count, error_count, message
        ) values (
            :job_id, :timestamp_ms_2025, :success_count, :error_count, {}
        )
    """.format(
            ":message" if message else "null"
        ),
        {
            "job_id": job_id,
            "timestamp_ms_2025": int(time.time() * 1000) - JAN_1_2025_EPOCH,
            "success_count": success_count,
            "error_count": error_count,
            "message": message,
        },
    )


@hookimpl
def register_secrets():
    secrets = []
    for subclass in Enrichment._subclasses:
        if subclass.secret:
            secrets.append(subclass.secret)
    return secrets


class SecretError(Exception):
    pass


class Enrichment(ABC):
    _subclasses = []

    batch_size: int = 100
    # Cancel run after this many errors
    default_max_errors: int = 5
    log_traceback: bool = False

    class Cancel(Exception):
        def __init__(self, reason: Optional[str] = None):
            self.reason = reason

        def __str__(self) -> str:
            return self.reason or "Cancelled by enrichment"

    class Pause(Exception):
        def __init__(self, reason: Optional[str] = None):
            self.reason = reason

        def __str__(self) -> str:
            return self.reason or "Paused by enrichment"

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

    description: str = ""  # Short description of this enrichment
    secret: Optional[Secret] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._subclasses.append(cls)

    def __repr__(self):
        return "<Enrichment: {}>".format(self.slug)

    async def get_secret(self, datasette: "Datasette", config: dict):
        if self.secret is None:
            raise SecretError("No secret defined for this enrichment")
        secret = await get_secret(datasette, self.secret.name)
        if secret is not None:
            return secret
        # Try the stashed secrets instead
        if not hasattr(datasette, "_enrichments_stashed_secrets"):
            raise SecretError("No secrets have been stashed")
        stashed_keys = datasette._enrichments_stashed_secrets
        stash_key = config.get("enrichment_secret")
        if stash_key not in stashed_keys:
            raise SecretError(
                "No secret found in stash for {}".format(self.secret.name)
            )
        return stashed_keys[stash_key]

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
        await record_progress(db, job_id, 0, len(ids))

    async def get_config_form(self, datasette: "Datasette", db: "Database", table: str):
        return None

    async def _get_config_form(
        self, datasette: "Datasette", db: "Database", table: str
    ):
        # Helper method that adds a `_secret` form field if the enrichment has a secret
        FormClass = await async_call_with_supported_arguments(
            self.get_config_form, datasette=datasette, db=db, table=table
        )
        if self.secret is None:
            return FormClass
        # If secret is already set, return form unmodified
        if await get_secret(datasette, self.secret.name):
            return FormClass

        # Otherwise, return form with secret field
        def stash_api_key(form, field):
            if not hasattr(datasette, "_enrichments_stashed_secrets"):
                datasette._enrichments_stashed_secrets = {}
            key = secrets.token_urlsafe(16)
            datasette._enrichments_stashed_secrets[key] = field.data
            field.data = key

        formatted_description = self.secret.description
        if self.secret.obtain_url and self.secret.obtain_label:
            html_bits = []
            if self.secret.description:
                html_bits.append(escape(self.secret.description))
                html_bits.append(" - ")
            html_bits.append(
                f'<a href="{self.secret.obtain_url}" target="_blank">{self.secret.obtain_label}</a>'
            )
            formatted_description = Markup("".join(html_bits))

        class FormWithSecret(FormClass):
            enrichment_secret = PasswordField(
                self.secret.name,
                description=formatted_description,
                validators=[
                    DataRequired(message="Secret is required."),
                    stash_api_key,
                ],
                render_kw={"autocomplete": "off"},
            )

        return FormWithSecret

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
    ) -> Optional[int]:
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

        await ensure_tables(db)

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
                # Check something else hasn't set the state to paused or cancelled
                job_row = (
                    await db.execute(
                        "select status from _enrichment_jobs where id = ?", (job_id,)
                    )
                ).first()
                if not job_row or job_row[0] != "running":
                    break
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
                    success_count = await async_call_with_supported_arguments(
                        self.enrich_batch,
                        datasette=datasette,
                        db=db,
                        table=job["table_name"],
                        rows=rows,
                        pks=pks or ["rowid"],
                        config=json.loads(job["config"]),
                        job_id=job_id,
                    )
                    if success_count is None:
                        success_count = len(rows)
                    await record_progress(db, job_id, success_count, 0)
                except self.Cancel as ex:
                    await set_job_status(db, job_id, "cancelled", message=str(ex))
                    return
                except self.Pause as ex:
                    await set_job_status(db, job_id, "paused", message=str(ex))
                    return
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
        # Job management
        (r"^/-/enrich/(?P<database>[^/]+)/-/jobs$", views.list_jobs_view),
        (r"^/-/enrich/(?P<database>[^/]+)/-/jobs/(?P<job_id>[0-9]+)$", views.job_view),
        (
            r"^/-/enrich/(?P<database>[^/]+)/-/jobs/(?P<job_id>[0-9]+)/pause$",
            views.pause_job_view,
        ),
        (
            r"^/-/enrich/(?P<database>[^/]+)/-/jobs/(?P<job_id>[0-9]+)/resume$",
            views.resume_job_view,
        ),
        (
            r"^/-/enrich/(?P<database>[^/]+)/-/jobs/(?P<job_id>[0-9]+)/cancel$",
            views.cancel_job_view,
        ),
        (
            r"^/-/enrichment-jobs/(?P<database>[^/]+)/(?P<job_id>[0-9]+)$",
            views.job_progress_view,
        ),
        # Select and execute enrichments UI
        (r"^/-/enrich/(?P<database>[^/]+)/(?P<table>[^/]+)$", views.enrichment_picker),
        (
            r"^/-/enrich/(?P<database>[^/]+)/(?P<table>[^/]+)/(?P<enrichment>[^/]+)$",
            views.enrichment_view,
        ),
    ]


@hookimpl
def table_actions(datasette, actor, database, table, request):
    async def inner():
        if await datasette.permission_allowed(
            actor, "enrichments", resource=database, default=False
        ):
            items = [
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
            # Are there any runs?
            try:
                job_count = (
                    await datasette.get_database(database).execute(
                        "select count(*) from _enrichment_jobs where database_name = ? and table_name = ?",
                        (database, table),
                    )
                ).single_value()
                if job_count:
                    items.append(
                        {
                            "href": datasette.urls.path(
                                "/-/enrich/{}/-/jobs?table={}".format(
                                    database, quote(table)
                                ),
                            ),
                            "label": "Enrichment jobs",
                            "description": "View and manage {} enrichment job{} for this table".format(
                                job_count, "s" if job_count != 1 else ""
                            ),
                        }
                    )
            except sqlite3.OperationalError:  # No such table
                pass

            return items

    return inner


@hookimpl
def database_actions(datasette, actor, database):
    async def inner():
        if await datasette.permission_allowed(
            actor, "enrichments", resource=database, default=False
        ):
            # Are there any runs?
            try:
                job_count = (
                    await datasette.get_database(database).execute(
                        "select count(*) from _enrichment_jobs where database_name = ?",
                        (database,),
                    )
                ).single_value()
                if not job_count:
                    return
            except sqlite3.OperationalError:  # No such table
                return
            return [
                {
                    "href": datasette.urls.path(
                        "/-/enrich/{}/-/jobs".format(database),
                    ),
                    "label": "Enrichment jobs",
                    "description": "View and manage {} enrichment job{} for this database".format(
                        job_count, "s" if job_count != 1 else ""
                    ),
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
                    "description": "Run a data cleaning operation against this row",
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


async def jobs_for_table(datasette, database_name, table_name):
    jobs = []
    db = datasette.get_database(database_name)
    if await db.table_exists("_enrichment_jobs"):
        sql = "select * from _enrichment_jobs where database_name = ? and table_name = ? and status = 'running' order by id desc"
        jobs = [
            dict(row)
            for row in (await db.execute(sql, (database_name, table_name))).rows
        ]
    return jobs


CUSTOM_ELEMENT_JS = """
class JobProgress extends HTMLElement {
  static observedAttributes = ['api-url', 'poll-interval'];

  constructor() {
    super();
    this.pollInterval = null;
    this.sections = [];
    this.total = 0;
    this.initialized = false;
    this.attachShadow({ mode: 'open' });

    const style = document.createElement('style');
    style.textContent = `
      :host {
        display: none;
        font-family: Helvetica, Arial, sans-serif;
      }

      :host(.initialized) {
        display: block;
      }

      .container {
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 1rem;
      }

      h2 {
        margin: 0 0 1rem 0;
        font-size: 1.25rem;
        font-weight: 500;
      }

      .job-link {
        color: #2196F3;
        text-decoration: none;
      }

      .job-link:hover {
        text-decoration: underline;
      }

      .progress-wrapper {
        width: 100%;
        height: 24px;
        background: #f5f5f5;
        border-radius: 12px;
        overflow: hidden;
        position: relative;
      }

      .progress-bar {
        height: 100%;
        position: absolute;
        transition: all 0.3s ease;
      }

      .progress-success {
        background: #4CAF50;
      }

      .progress-error {
        background: #f44336;
      }

      .progress-stats {
        display: flex;
        justify-content: space-between;
        margin-top: 0.5rem;
        color: #666;
      }

      .stat {
        display: flex;
        align-items: center;
        gap: 0.5rem;
      }

      .stat-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
      }

      .stat-dot.success {
        background: #4CAF50;
      }

      .stat-dot.error {
        background: #f44336;
      }
    `;

    const template = document.createElement('template');
    let h2 = `<h2><a href="#" class="job-link"></a></h2>`;
    if (this.getAttribute('hide-title')) {
       h2 = h2.replace('<h2>', '<h2 style="display: none;">');
    }
    template.innerHTML = `
      <div class="container" role="region" aria-label="Task enrichment progress">
        ${h2}
        <div class="progress-wrapper" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
        </div>
        <div class="progress-stats" aria-live="polite">
          <div class="stat">
            <div class="stat-dot success" aria-hidden="true"></div>
            <span class="completed-count">0</span>
            <span class="sr-only">tasks</span> completed
          </div>
          <div class="stat">
            <div class="stat-dot error" aria-hidden="true"></div>
            <span class="error-count">0</span>
            <span class="sr-only">tasks with</span> errors
          </div>
          <div class="stat">
            Total: <span class="total-count">0</span>
            <span class="sr-only">tasks</span>
          </div>
        </div>
      </div>
    `;

    this.shadowRoot.appendChild(style);
    this.shadowRoot.appendChild(template.content.cloneNode(true));
  }

  connectedCallback() {
    const apiUrl = this.getAttribute('api-url');
    if (apiUrl) {
      this.startPolling();
    }
  }

  disconnectedCallback() {
    this.stopPolling();
  }

  async startPolling() {
    const pollMs = parseInt(this.getAttribute('poll-interval')) || 1000;
    const apiUrl = this.getAttribute('api-url');

    const poll = async () => {
      try {
        const response = await fetch(apiUrl);
        // Stop polling if response is not OK (non-200 status)
        if (!response.ok) {
          console.error(`Failed to fetch progress: ${response.status} ${response.statusText}`);
          this.stopPolling();
          return;
        }
        const data = await response.json();
        // Initialize component with first API response
        if (!this.initialized) {
          this.initialized = true;
          this.classList.add('initialized');
          this.total = data.total;
          this.shadowRoot.querySelector('.total-count').textContent = this.total;
          this.shadowRoot.querySelector('.job-link').textContent = data.title;
          this.shadowRoot.querySelector('.job-link').setAttribute('href', data.url);
        }
        this.updateProgress(data.sections);
        if (data.is_complete) {
          this.stopPolling();
        }
      } catch (error) {
        console.error('Error polling progress:', error);
        this.stopPolling();
      }
    };
    this.pollInterval = setInterval(poll, pollMs);
    poll();
  }

  stopPolling() {
    clearInterval(this.pollInterval);
  }

  updateProgress(sections) {
    const progressWrapper = this.shadowRoot.querySelector('.progress-wrapper');
    // Clear existing bars
    progressWrapper.innerHTML = '';
    // Calculate totals
    let completed = 0;
    let errors = 0;
    let currentPosition = 0;
    // Create and position bars
    sections.forEach(section => {
      const width = (section.count / this.total) * 100;
      const bar = document.createElement('div');
      bar.className = `progress-bar progress-${section.type}`;
      bar.style.width = `${width}%`;
      bar.style.left = `${currentPosition}%`;
      progressWrapper.appendChild(bar);
      currentPosition += width;
      if (section.type === 'success') {
        completed += section.count;
      } else {
        errors += section.count;
      }
    });
    // Update stats
    this.shadowRoot.querySelector('.completed-count').textContent = completed;
    this.shadowRoot.querySelector('.error-count').textContent = errors;
    const totalProgress = ((completed + errors) / this.total) * 100;
    // Update ARIA
    progressWrapper.setAttribute('aria-valuenow', totalProgress);
    progressWrapper.setAttribute('aria-label',
      `Overall progress: ${Math.round(totalProgress)}%. ` +
      `${completed} tasks completed, ${errors} errors, ${this.total} total tasks`
    );
  }
}
customElements.define('job-progress', JobProgress);
"""

POLL_JS = (
    CUSTOM_ELEMENT_JS
    + """
async function initEnrichmentProgress(jobs) {
  try {
    // Validate jobs argument
    if (!Array.isArray(jobs)) {
      throw new Error('jobs argument must be an array');
    }

    // Find the first table element
    const firstTable = document.querySelector('table');
    if (!firstTable) {
      console.warn('No table element found on page');
      return;
    }

    // Create a container for our progress elements
    const container = document.createElement('div');

    // Add each job's progress element
    jobs.forEach(job => {
      if (!job.id) {
        console.warn('Job missing ID, skipping:', job);
        return;
      }
      const progressElement = document.createElement('job-progress');
      progressElement.setAttribute('api-url', `/-/enrichment-jobs/{{ database }}/${job.id}`);
      progressElement.style.marginBottom = '1rem';
      container.appendChild(progressElement);
    });

    // Insert the container before the table
    firstTable.parentNode.insertBefore(container, firstTable);
  } catch (error) {
    console.error('Error initializing enrichment progress:', error);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  initEnrichmentProgress({{ jobs }});
});
"""
)

_restart_running_jobs_lock = asyncio.Lock()


async def _restart_running_jobs_task(datasette):
    # For each database known to Datasette, look for running jobs
    for database_name in datasette.databases:
        db = datasette.get_database(database_name)

        # If the _enrichment_jobs table doesn't exist in this DB, skip
        table_names = await db.table_names()
        if (
            "_enrichment_jobs" not in table_names
            or "_enrichment_progress" not in table_names
        ):
            continue

        # Find jobs marked as 'running'
        running_jobs = (
            await db.execute(
                """
            SELECT * FROM _enrichment_jobs
            WHERE status = 'running'
            """
            )
        ).rows

        # Grab all known enrichments
        all_enrichments = await get_enrichments(datasette)

        # Start each running job again
        for job in running_jobs:
            job_id = job["id"]
            enrichment_slug = job["enrichment"]

            # Look up the enrichment class by its slug
            if enrichment_slug in all_enrichments:
                enrichment = all_enrichments[enrichment_slug]
                # Resume from wherever it left off
                await enrichment.start_enrichment_in_process(datasette, db, job_id)
            else:
                print("Unknown enrichment: {}".format(enrichment_slug), file=sys.stderr)


async def restart_running_jobs(datasette):
    """
    Start the background task if it hasn't been started yet.
    Uses a lock to ensure the task is only started once.
    """
    if hasattr(datasette, "_restart_running_jobs_task_started"):
        return
    # Lock to avoid race condition if two requests come in at once
    async with _restart_running_jobs_lock:
        if not hasattr(datasette, "_restart_running_jobs_task_started"):
            datasette._restart_running_jobs_task_started = True
            asyncio.create_task(_restart_running_jobs_task(datasette))


@hookimpl
def asgi_wrapper(datasette):
    def wrap_with_task_starter(app):
        async def wrapped_app(scope, receive, send):
            await restart_running_jobs(datasette)
            await app(scope, receive, send)

        return wrapped_app

    return wrap_with_task_starter


@hookimpl
def extra_body_script(datasette, database, table, view_name):
    async def inner():
        if view_name != "table":
            return ""
        jobs = await jobs_for_table(datasette, database, table)
        if not jobs:
            return ""
        return {
            "module": True,
            "script": POLL_JS.replace("{{ database }}", database).replace(
                "{{ jobs }}", json.dumps(jobs)
            ),
        }

    return inner


@hookimpl
def register_permissions(datasette):
    if Permission is not None:
        return [
            Permission(
                name="enrichments",
                abbr=None,
                description="Enrich data in tables",
                takes_database=True,
                takes_resource=False,
                default=False,
            )
        ]
