from datasette_enrichments import Enrichment

from jinja2.sandbox import SandboxedEnvironment

from datasette.database import Database
from typing import List
from wtforms import Form, StringField, TextAreaField
from wtforms.validators import DataRequired
import sqlite_utils


class JinjaSandbox(Enrichment):
    name = "Construct a string using Jinja"
    slug = "jinja-sandbox"
    description = "Execute a template using Jinja and store the result"
    runs_in_process = True

    async def get_config_form(self, db, table):
        columns = await db.table_columns(table)

        # Default template uses all string columns
        default = " ".join('{{ row["COL"] }}'.replace("COL", col) for col in columns)

        class ConfigForm(Form):
            template = TextAreaField(
                "Template",
                description='A template to run against each row to generate text. Use {{ row["name"] }} for columns.',
                default=default,
            )
            output_column = StringField(
                "Output column name",
                description="The column to store the output in - will be created if it does not exist.",
                validators=[DataRequired(message="Column is required.")],
                default="template_output",
            )

        return ConfigForm

    async def initialize(self, datasette, db, table, config):
        # Ensure column exists
        output_column = config["output_column"][0]

        def add_column_if_not_exists(conn):
            db = sqlite_utils.Database(conn)
            if output_column not in db[table].columns_dict:
                db[table].add_column(output_column, str)

        await db.execute_write_fn(add_column_if_not_exists)

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
        env = SandboxedEnvironment(enable_async=True)
        template = env.from_string(config["template"][0])
        output_column = config["output_column"][0]
        for row in rows:
            output = await template.render_async({"row": row})
            await db.execute_write(
                "update [{table}] set [{output_column}] = ? where {wheres}".format(
                    table=table,
                    output_column=output_column,
                    wheres=" and ".join('"{}" = ?'.format(pk) for pk in pks),
                ),
                [output] + list(row[pk] for pk in pks),
            )
