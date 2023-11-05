import asyncio
from datasette import hookimpl
import httpx
import json
import struct
from .views import enrich_data


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

    async def initialize(self, db, table, config):
        pass

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
        print("start enrichment", job)

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
                await self.enrich_batch(
                    db, job["table_name"], rows, json.loads(job["config"])
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

    async def enrich_batch(self, db, table, rows, config):
        pks = await db.primary_keys(table)
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


class Embeddings(Enrichment):
    name = "OpenAI Embeddings"
    slug = "openai-embeddings"
    batch_size = 100
    description = (
        "Calculate embeddings for text columns in a table. Embeddings are numerical representations which "
        "can be used to power semantic search and find related content."
    )
    runs_in_process = True

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

    async def enrich_batch(self, db, table, rows, config):
        # TODO: Finish this
        texts = [eval_template(config["template"], row) for row in rows]
        token = config["token"]
        response = httpx.post(
            "https://api.openai.com/v1/embeddings",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"input": texts, "model": "text-embedding-ada-002"},
        )
        results = response.json()["data"]
        embeddings_table = "_embeddings_{}".format(table)
        # Write results to the table
        for row, result in zip(rows, results):
            embedding = struct.pack("f" * len(result), *result)
            await db.execute_write(
                f"insert into [{embeddings_table}] (rowid, embedding) values (?, ?)",
                (row["rowid"], embedding),
            )


def eval_template(template, row):
    for key, value in row.items():
        template = template.replace("${}".format(key), value)
    return template


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
