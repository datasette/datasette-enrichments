from datasette_enrichments import Enrichment
from datasette.database import Database
from typing import List
import math
import struct
from string import Template
import httpx


from wtforms import SelectField, Form, TextAreaField, PasswordField
from wtforms.widgets import ListWidget, CheckboxInput
from wtforms.validators import DataRequired


class MultiCheckboxField(SelectField):
    widget = ListWidget(prefix_label=False)
    option_widget = CheckboxInput()


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

        return ConfigForm

    async def initialize(self, datasette, db, table, config):
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
        datasette,
        db: Database,
        table: str,
        rows: List[dict],
        pks: List[str],
        config: dict,
        job_id: int,
        actor_id: str = None,
    ):
        template = SpaceTemplate(config["template"])
        texts = [template.safe_substitute(row) for row in rows]
        token = config["api_token"]

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
