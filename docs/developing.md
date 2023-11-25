# Developing a new enrichment

Enrichments are implemented as Datasette plugins.

An enrichment plugin should implement the `register_enrichments()` plugin hook, which should return a list of instances of subclasses of the `Enrichment` base class.

The function can also return an awaitable function which returns that list of instances. This is useful if your plugin needs to do some asynchronous work before it can return the list of enrichments.

## The plugin hook

Your enrichment plugin should register new enrichments using the `register_enrichments()` plugin hook:

```python
from datasette import hookimpl

@hookimpl
def register_enrichments():
    return [MyEnrichment()]
```
`register_enrichment()` can optionally accept a `datasette` argument. This can then be used to read plugin configuration or run database queries.

The plugin hook can return an awaitable function if it needs to do some asynchronous work before it can return the list of enrichments, for example:

```python
@hookimpl
def register_enrichments(datasette):
    async def inner():
        db = datasette.get_database("mydb")
        settings = [
            row["setting"]
            for row in await db.execute(
                "select setting from special_settings"
            )
        ]
        return [
            MyEnrichment(setting)
            for setting in settings
        ]
    return inner
```

## Enrichment subclasses

Most of the code you write will be in a subclass of `Enrichment`:

```python
from datasette_enrichments import Enrichment

class MyEnrichment(Enrichment):
    name = "Name of My Enrichment"
    slug = "my-enrichment"
    description = "One line description of what it does"
```
The `name`, `slug` and `description` attributes are required. They are used to display information about the enrichment to users.

Try to ensure your `slug` is unique among all of the other enrichments your users might have installed.

You can also set a `batch_size` attribute. This defaults to 100 but you can set it to another value to control how many rows are passed to your `enrich_batch()` method at a time. You may want to set it to 1 to process rows one at a time.

### initialize()

Your class can optionally implement an `initialize()` method. This will be called once at the start of each enrichment run.

This method is often used to prepare the database - for example, adding a new table column that the enrichment will then populate.

```python
async def initialize(
    self,
    datasette: Datasette,
    db: Database,
    table: str,
    config: dict
):
```
- `datasette` is the [Datasette instance](https://docs.datasette.io/en/stable/internals.html#datasette-class).
- `db` is the [Database instance](https://docs.datasette.io/en/stable/internals.html#database-class) for the database that the enrichment is being run against.
- `table` is the name of the table.
- `config` is a dictionary of configuration options that the user set for the enrichment, using the configuration form (if one was provided).

### enrich_batch()

You must implement the following method:

```python
async def enrich_batch(
    self,
    datasette: Datasette,
    db: Database,
    table: str,
    rows: List[dict],
    pks: List[str],
    config: dict,
    job_id: int,
):
    # Enrichment logic goes here
```
This method will be called multiple times, each time with a different list of rows.

It should perform whatever enrichment logic is required, using the `db` object ([documented here](https://docs.datasette.io/en/stable/internals.html#database-class)) to write any results back to the database.

`enrich_batch()` is an `async def` method, so you can use `await` within the method to perform asynchronous operations such as HTTP calls ([using HTTPX](https://www.python-httpx.org/async/)) or database queries.

The arguments passed to `enrich_batch()` are as follows:

- `datasette` is the [Datasette instance](https://docs.datasette.io/en/stable/internals.html#datasette-class). You can use this to read plugin configuration, check permissions, render templates and more.
- `db` is the [Database instance](https://docs.datasette.io/en/stable/internals.html#database-class) for the database that the enrichment is being run against. You can use this to execute SQL queries against the database.
- `table` is the name of the table that the enrichment is being run against.
- `rows` is a list of dictionaries for the current batch, each representing a row from the table. These are the same shape as JSON dictionaries returned by the [Datasette JSON API](https://docs.datasette.io/en/stable/json_api.html). The batch size defaults to 100 but can be customized by your class.
- `pks` is a list of primary key column names for the table.
- `config` is a dictionary of configuration options that the user set for the enrichment, using the configuration form (if one was provided).
- `job_id` is a unique integer ID for the current job. This can be used to log additional information about the enrichment execution.

### get_config_form()

The `get_config_form()` method can optionally be implemented to return a [WTForms](https://wtforms.readthedocs.io/) form class that the user can use to configure the enrichment.

This example defines a form with two fields: a `template` text area field and an `output_column` single line input:
```python
from wtforms import Form, StringField, TextAreaField
from wtforms.validators import DataRequired

# ...
        async def get_config_form(self, db, table):

        class ConfigForm(Form):
            template = TextAreaField(
                "Template",
                description='Template to use',
                default=default,
            )
            output_column = StringField(
                "Output column name",
                description="The column to store the output in - will be created if it does not exist.",
                validators=[DataRequired(message="Column is required.")],
                default="template_output",
            )

        return ConfigForm
```
The valid dictionary that is produced by filling in this form will be passed as `config` to both the `initialize()` and `enrich_batch()` methods.
