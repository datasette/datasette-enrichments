import asyncio
import hashlib
import json

import pytest
from datasette import hookimpl
from datasette.database import Database
from datasette.plugins import pm
from wtforms import Form, SelectField, StringField
from wtforms.widgets import CheckboxInput, ListWidget


class MultiCheckboxField(SelectField):
    widget = ListWidget(prefix_label=False)
    option_widget = CheckboxInput()


@pytest.fixture(autouse=True)
def load_uppercase_plugin():
    from datasette_secrets import Secret

    from datasette_enrichments import Enrichment

    class UppercaseDemo(Enrichment):
        name = "Convert to uppercase"
        slug = "uppercasedemo"
        description = "Convert selected columns to uppercase"

        async def initialize(self, datasette, db, table, config):
            datasette._initialize_called_with = (datasette, db, table, config)

        async def finalize(self, datasette, db, table, config):
            datasette._finalize_called_with = (datasette, db, table, config)

        async def get_config_form(self, db, table):
            choices = [(col, col) for col in await db.table_columns(table)]

            class ConfigForm(Form):
                columns = MultiCheckboxField("Columns", choices=choices)

            return ConfigForm

        async def enrich_batch(
            self,
            datasette,
            db: Database,
            table: str,
            rows: list[dict],
            pks: list[str],
            config: dict,
            job_id: int,
            actor_id: str | None = None,
        ):
            if getattr(datasette, "_trigger_enrich_batch_error", None):
                raise Exception("Error in enrich_batch()")  # noqa: TRY002
            columns = config.get("columns") or []
            if not columns:
                return
            wheres = " and ".join(f'"{pk}" = ?' for pk in pks)
            sets = ", ".join(f'"{col}" = upper("{col}")' for col in columns)
            params = [[row[pk] for pk in pks] for row in rows]
            await db.execute_write_many(
                f"update [{table}] set {sets} where {wheres}", params
            )
            # Wait 0.3s
            await asyncio.sleep(0.3)

    class SecretReplacePlugin(Enrichment):
        name = "Replace string with a secret"
        slug = "secretreplace"
        description = "Replace a string with a secret"
        secret = Secret(
            name="STRING_SECRET",
            description="The secret to use in the replacement",
        )

        async def get_config_form(self, db, table):
            choices = [(col, col) for col in await db.table_columns(table)]

            class ConfigForm(Form):
                column = SelectField("Column", choices=choices)
                string = StringField("String to be replaced")

            return ConfigForm

        async def enrich_batch(
            self,
            datasette,
            db: Database,
            table: str,
            rows: list[dict],
            pks: list[str],
            config: dict,
            job_id: int,
            actor_id: str | None = None,
        ):
            secret = await self.get_secret(datasette, config)
            for row in rows:
                await db.execute_write(
                    "update [{}] set [{}] = ? where {}".format(
                        table,
                        config["column"],
                        " and ".join(f'"{pk}" = ?' for pk in pks),
                    ),
                    [row[config["column"]].replace(config["string"], secret)]
                    + [row[pk] for pk in pks],
                )

    class HashRows(Enrichment):
        name = "Calculate a hash for each row"
        slug = "hashrows"
        description = "To demonstrate an enrichment with no config form"

        async def initialize(self, datasette, db, table, config):
            await db.execute_write(f"alter table [{table}] add column sha_256 text")

        async def enrich_batch(
            self,
            db: Database,
            table: str,
            rows: list[dict],
            pks: list[str],
            actor_id: str | None = None,
        ):
            for row in rows:
                to_hash = json.dumps(row, default=repr)
                sha_256 = hashlib.sha256(to_hash.encode()).hexdigest()
                await db.execute_write(
                    "update [{}] set sha_256 = ? where {}".format(
                        table,
                        " and ".join(f'"{pk}" = ?' for pk in pks),
                    ),
                    [sha_256] + [row[pk] for pk in pks],
                )

    class HasErrors(Enrichment):
        name = "8 success then 2 errors, repeated"
        slug = "haserrors"
        description = "To demonstrate an enrichment with errors"
        batch_size = 10

        async def enrich_batch(
            self,
            db: Database,
            table: str,
            rows: list[dict],
            pks: list[str],
            job_id: int,
            actor_id: str | None = None,
        ) -> int:
            assert len(pks) == 1
            pk = pks[0]
            success_count = len(rows)
            if len(rows) > 8:
                ids = [row[pk] for row in rows[8:]]
                success_count -= len(ids)
                await self.log_error(db, job_id, ids, "Error")
            return success_count

    class QueueControlledEnrichment(Enrichment):
        name = "Queue controlled enrichment"
        slug = "queue"
        description = "An enrichment that waits for results from a queue for each row"
        batch_size = 1

        async def initialize(self, datasette, db, table, config):
            datasette.enrichment_queue = asyncio.Queue()
            datasette.enrichment_processed_count = 0
            await db.execute_write(
                f"alter table [{table}] add column queue_result text"
            )

        async def enrich_batch(
            self,
            datasette,
            db: Database,
            table: str,
            rows: list[dict],
            pks: list[str],
            actor_id: str | None = None,
        ):
            row = rows[0]
            result = await datasette.enrichment_queue.get()
            if result == "pause":
                raise self.Pause("pause message")
            if result == "cancel":
                raise self.Cancel("cancel message")
            datasette.enrichment_processed_count += 1
            wheres = " and ".join(f'"{pk}" = ?' for pk in pks)
            await db.execute_write(
                f"""
                update [{table}]
                set queue_result = ?
                where {wheres}
                """,
                [result] + [row[pk] for pk in pks],
            )
            datasette.enrichment_queue.task_done()

    class EnrichmentsDemoPlugin:
        __name__ = "EnrichmentsDemoPlugin"

        @hookimpl
        def register_enrichments(self):
            return [
                UppercaseDemo(),
                SecretReplacePlugin(),
                HashRows(),
                HasErrors(),
                QueueControlledEnrichment(),
            ]

    pm.register(EnrichmentsDemoPlugin(), name="undo_EnrichmentsDemoPlugin")
    try:
        yield
    finally:
        pm.unregister(name="undo_EnrichmentsDemoPlugin")
