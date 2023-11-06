import asyncio
from datasette import hookimpl
from datasette.database import Database
import httpx
import json
import math
import struct
from typing import List
from .views import enrich_data

from wtforms import Form, TextAreaField, PasswordField


from datasette.utils import await_me_maybe
from datasette.plugins import pm
from . import hookspecs


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

pm.add_hookspecs(hookspecs)


class Enrichment:
    batch_size = 100
    runs_in_process = False
    # Cancel run after this many errors
    default_max_errors = 5

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


class Uppercase(Enrichment):
    name = "Convert to uppercase"
    slug = "uppercase"
    description = "Convert selected columns to uppercase"
    runs_in_process = True

    async def enrich_batch(
        self,
        db: Database,
        table: str,
        rows: List[dict],
        pks: List[str],
        config: dict,
        job_id: int,
    ):
        columns = config.get("columns") or []
        if not columns:
            return
        wheres = " and ".join('"{}" = ?'.format(pk) for pk in pks)
        sets = ", ".join('"{}" = upper("{}")'.format(col, col) for col in columns)
        params = [[row[pk] for pk in pks] for row in rows]
        await db.execute_write_many(
            "update [{}] set {} where {}".format(table, sets, wheres), params
        )
        await asyncio.sleep(0.3)


from wtforms import SelectField
from wtforms.widgets import ListWidget, CheckboxInput
from wtforms.validators import DataRequired


class MultiCheckboxField(SelectField):
    widget = ListWidget(prefix_label=False)
    option_widget = CheckboxInput()


from string import Template


class SpaceTemplate(Template):
    # Allow spaces in braced placeholders: ${column name here}
    braceidpattern = r"[^\}]+"


class Embeddings(Enrichment):
    name = "OpenAI Embeddings"
    slug = "openai-embeddings"
    batch_size = 100
    description = (
        "Calculate embeddings for text columns in a table. Embeddings are numerical representations which "
        "can be used to power semantic search and find related content."
    )
    runs_in_process = True

    cost_per_1000_tokens_in_100ths_cent = 1

    async def get_config_form(self, db, table):
        choices = [(col, col) for col in await db.table_columns(table)]

        # Default template uses all string columns
        default = " ".join("${{{}}}".format(col[0]) for col in choices)

        class ConfigForm(Form):
            template = TextAreaField(
                "Template",
                description="A template to run against each row to generate text to embed. Use ${column-name} for columns.",
                default=default,
            )
            api_token = PasswordField(
                "OpenAI API token",
                validators=[DataRequired(message="The token is required.")],
            )
            # columns = MultiCheckboxField("Columns", choices=choices)

        return ConfigForm

    async def initialize(self, db, table, config):
        # Ensure table exists
        embeddings_table = "_embeddings_{}".format(table)
        if not await db.table_exists(embeddings_table):
            # Create it
            pk_names = await db.primary_keys(table)
            column_types = {
                c.name: c.type for c in await db.table_column_details(table)
            }
            sql = ["create table [{}] (".format(embeddings_table)]
            create_bits = []
            for pk in pk_names:
                create_bits.append("    [{}] {}".format(pk, column_types[pk]))
            create_bits.append("    _embedding blob")
            create_bits.append(
                "    PRIMARY KEY ({})".format(
                    ", ".join("[{}]".format(pk) for pk in pk_names)
                )
            )
            # If there's only one primary key, set up a foreign key constraint
            if len(pk_names) == 1:
                create_bits.append(
                    "    FOREIGN KEY ([{}]) REFERENCES [{}] ({})".format(
                        pk_names[0], table, pk_names[0]
                    )
                )
            sql.append(",\n".join(create_bits))
            sql.append(")")
            await db.execute_write("\n".join(sql))

    async def enrich_batch(
        self,
        db: Database,
        table: str,
        rows: List[dict],
        pks: List[str],
        config: dict,
        job_id: int,
    ):
        template = SpaceTemplate(config["template"][0])
        texts = [template.safe_substitute(row) for row in rows]
        token = config["api_token"][0]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"input": texts, "model": "text-embedding-ada-002"},
            )
            json_data = response.json()

        results = json_data["data"]

        # Record the cost too
        # json_data['usage']
        # {'prompt_tokens': 16, 'total_tokens': 16}
        cost_per_token_in_100ths_cent = self.cost_per_1000_tokens_in_100ths_cent / 1000
        total_cost_in_100ths_of_cents = (
            json_data["usage"]["total_tokens"] * cost_per_token_in_100ths_cent
        )
        # Round up to the nearest integer
        total_cost_rounded_up = math.ceil(total_cost_in_100ths_of_cents)
        await self.increment_cost(db, job_id, total_cost_rounded_up)

        embeddings_table = "_embeddings_{}".format(table)
        # Write results to the table
        for row, result in zip(rows, results):
            vector = result["embedding"]
            embedding = struct.pack("f" * len(vector), *vector)
            await db.execute_write(
                "insert or replace into [{embeddings_table}] ({pks}, _embedding) values ({pk_question_marks}, ?)".format(
                    embeddings_table=embeddings_table,
                    pks=", ".join("[{}]".format(pk) for pk in pks),
                    pk_question_marks=", ".join("?" for _ in pks),
                ),
                list(row[pk] for pk in pks) + [embedding],
            )


@hookimpl
def register_routes():
    return [
        (r"^/-/enrich/(?P<database>[^/]+)/(?P<table>[^/]+)$", enrich_data),
    ]


@hookimpl
def table_actions(datasette, actor, database, table, request):
    if actor and actor.get("id") == "root":
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
