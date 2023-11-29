import asyncio
from datasette.database import Database
from typing import List
from wtforms import Form, SelectField
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

    class UppercasePlugin:
        __name__ = "UppercasePlugin"

        @hookimpl
        def register_enrichments(self):
            return [UppercaseDemo()]

    pm.register(UppercasePlugin(), name="undo_uppercase")
    try:
        yield
    finally:
        pm.unregister(name="undo_uppercase")
