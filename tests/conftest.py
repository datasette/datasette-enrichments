import asyncio
from datasette.database import Database
import hashlib
import json
from typing import List
from wtforms import Form, SelectField, StringField
from wtforms.widgets import ListWidget, CheckboxInput
import pytest
from datasette.plugins import pm
from datasette import hookimpl


class MultiCheckboxField(SelectField):
    widget = ListWidget(prefix_label=False)
    option_widget = CheckboxInput()


@pytest.fixture(autouse=True)
def load_uppercase_plugin():
    from datasette_enrichments import Enrichment
    from datasette_secrets import Secret

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
            rows: List[dict],
            pks: List[str],
            config: dict,
            job_id: int,
        ):
            if getattr(datasette, "_trigger_enrich_batch_error", None):
                raise Exception("Error in enrich_batch()")
            columns = config.get("columns") or []
            if not columns:
                return
            wheres = " and ".join('"{}" = ?'.format(pk) for pk in pks)
            sets = ", ".join('"{}" = upper("{}")'.format(col, col) for col in columns)
            params = [[row[pk] for pk in pks] for row in rows]
            await db.execute_write_many(
                "update [{}] set {} where {}".format(table, sets, wheres), params
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
            rows: List[dict],
            pks: List[str],
            config: dict,
            job_id: int,
        ):
            secret = await self.get_secret(datasette, config)
            for row in rows:
                await db.execute_write(
                    "update [{}] set [{}] = ? where {}".format(
                        table,
                        config["column"],
                        " and ".join('"{}" = ?'.format(pk) for pk in pks),
                    ),
                    [row[config["column"]].replace(config["string"], secret)]
                    + [row[pk] for pk in pks],
                )

    class HashRows(Enrichment):
        name = "Calculate a hash for each row"
        slug = "hashrows"
        description = "To demonstrate an enrichment with no config form"

        async def initialize(self, datasette, db, table, config):
            await db.execute_write(
                "alter table [{}] add column sha_256 text".format(table)
            )

        async def enrich_batch(
            self,
            db: Database,
            table: str,
            rows: List[dict],
            pks: List[str],
        ):
            for row in rows:
                to_hash = json.dumps(row, default=repr)
                sha_256 = hashlib.sha256(to_hash.encode()).hexdigest()
                await db.execute_write(
                    "update [{}] set sha_256 = ? where {}".format(
                        table,
                        " and ".join('"{}" = ?'.format(pk) for pk in pks),
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
            rows: List[dict],
            pks: List[str],
            job_id: int,
        ) -> int:
            assert len(pks) == 1
            pk = pks[0]
            success_count = len(rows)
            if len(rows) > 8:
                ids = [row[pk] for row in rows[8:]]
                success_count -= len(ids)
                await self.log_error(db, job_id, ids, "Error")
            return success_count

    class EnrichmentsDemoPlugin:
        __name__ = "EnrichmentsDemoPlugin"

        @hookimpl
        def register_enrichments(self):
            return [UppercaseDemo(), SecretReplacePlugin(), HashRows(), HasErrors()]

    pm.register(EnrichmentsDemoPlugin(), name="undo_EnrichmentsDemoPlugin")
    try:
        yield
    finally:
        pm.unregister(name="undo_EnrichmentsDemoPlugin")
