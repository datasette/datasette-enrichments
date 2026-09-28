import asyncio

from datasette.database import Database
from wtforms import Form, SelectMultipleField
from wtforms.widgets import CheckboxInput, ListWidget

from datasette_enrichments import Enrichment


class MultiCheckboxField(SelectMultipleField):
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
        datasette,
        db: Database,
        table: str,
        rows: list[dict],
        pks: list[str],
        config: dict,
        job_id: int,
        actor_id: str | None = None,
    ):
        columns = config.get("columns") or []
        if not columns:
            return
        wheres = " and ".join(f'"{pk}" = ?' for pk in pks)
        sets = ", ".join(f'"{col}" = upper("{col}")' for col in columns)
        params = [[row[pk] for pk in pks] for row in rows]
        await db.execute_write_many(
            f"update [{table}] set {sets} where {wheres}", params
        )
        await asyncio.sleep(0.3)
