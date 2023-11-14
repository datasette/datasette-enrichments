from datasette_enrichments import Enrichment
from datasette.database import Database
from typing import List
from wtforms import Form, SelectField
from wtforms.widgets import ListWidget, CheckboxInput
import asyncio


class MultiCheckboxField(SelectField):
    widget = ListWidget(prefix_label=False)
    option_widget = CheckboxInput()


class Uppercase(Enrichment):
    name = "Convert to uppercase"
    slug = "uppercase"
    description = "Convert selected columns to uppercase"
    runs_in_process = True

    async def get_config_form(self, db, table):
        choices = [(col, col) for col in await db.table_columns(table)]

        class ConfigForm(Form):
            columns = MultiCheckboxField("Columns", choices=choices)

        return ConfigForm

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
