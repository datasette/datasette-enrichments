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
The named parameters passed to `initialize()` are all optional. If you declare them they will be passed as follows:

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
) -> Optional[int]:
    # Enrichment logic goes here
```
Again, you can use just the subset of the named parameters that you need.

This method will be called multiple times, each time with a different list of rows.

It should perform whatever enrichment logic is required, using the `db` object ([documented here](https://docs.datasette.io/en/stable/internals.html#database-class)) to write any results back to the database.

`enrich_batch()` is an `async def` method, so you can use `await` within the method to perform asynchronous operations such as HTTP calls ([using HTTPX](https://www.python-httpx.org/async/)) or database queries.

The parameters available to `enrich_batch()` are as follows:

- `datasette` is the [Datasette instance](https://docs.datasette.io/en/stable/internals.html#datasette-class). You can use this to read plugin configuration, check permissions, render templates and more.
- `db` is the [Database instance](https://docs.datasette.io/en/stable/internals.html#database-class) for the database that the enrichment is being run against. You can use this to execute SQL queries against the database.
- `table` is the name of the table that the enrichment is being run against.
- `rows` is a list of dictionaries for the current batch, each representing a row from the table. These are the same shape as JSON dictionaries returned by the [Datasette JSON API](https://docs.datasette.io/en/stable/json_api.html). The batch size defaults to 100 but can be customized by your class.
- `pks` is a list of primary key column names for the table.
- `config` is a dictionary of configuration options that the user set for the enrichment, using the configuration form (if one was provided).
- `job_id` is a unique integer ID for the current job. This can be used to log additional information about the enrichment execution.

Your method can optionally return an integer count of the number of rows that were successfully processed. You should do this if you are using the `.log_error()` method described below to track errors - that way the progress bar will be updated with the correct number of rows processed.

If you do not return a count, the system will assume that a call to `enrich_batch()` which did not raise an exception processed all of the rows that were passed to it.

#### Pausing or cancelling the run

Code inside a `enrich_batch()` method can request that the run be paused or cancelled by raising special exceptions.

```python
async def enrich_batch(...):
    ...
    if no_tokens_left:
        raise self.Pause("Ran out of tokens")
```
Or to cancel the run entirely:
```python
raise self.Cancel("Code snippet did not compile")
```
Messages logged here will be visible on the job detail page.

### get_config_form()

The `get_config_form()` method can optionally be implemented to return a [WTForms](https://wtforms.readthedocs.io/) form class that the user can use to configure the enrichment.

This example defines a form with two fields: a `template` text area field and an `output_column` single line input:
```python
from wtforms import Form, StringField, TextAreaField
from wtforms.validators import DataRequired

# ...
    async def get_config_form(self):
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

The `get_config_form()` method can take the following optional named parameters:

- `datasette`: a [Datasette instance](https://docs.datasette.io/en/stable/internals.html#datasette-class)
- `db`: a [Database instance](https://docs.datasette.io/en/stable/internals.html#database-class)
- `table`: the string name of the table that the enrichment is being run against

### finalize()

Your class can optionally implement a `finalize()` method. This will be called once at the end of each enrichment run.

```python
async def finalize(self, datasette, db, table, config):
    # ...
```
Again, these named parameters are all optional:

- `datasette` is the [Datasette instance]
- `db` is the [Database instance](https://docs.datasette.io/en/stable/internals.html#database-class)
- `table` is the name of the table (a string)
- `config` is an optional dictionary of configuration options that the user set for the run

## Tracking errors

Errors that occur while running an enrichment are recorded in the `_enrichment_errors` table, with the following schema:

<!-- [[[cog
import cog, datasette_enrichments
cog.out(
    "```sql\n{}\n```".format(datasette_enrichments.CREATE_ERROR_TABLE_SQL.replace(
        ' if not exists', ''
    ))
)
]]] -->
```sql
create table _enrichment_errors (
    id integer primary key,
    job_id integer references _enrichment_jobs(id),
    created_at text,
    row_pks text, -- JSON list of row primary keys
    error text
)
```
<!-- [[[end]]] -->
If your `.enrich_batch()` raises any exception, all of the IDs in that batch will be marked as errored in this table.

Alternatively you can catch errors for individual rows within your `enrich_batch()` method and record them yourself using the `await self.log_error()` method, which has the following signature:

```
async def log_error(
    self, db: Database, job_id: int, ids: List[IdType], error: str
)
```
Call this with a reference to the current database, the job ID, a list of row IDs (which can be strings, integers or tuples for compound primary key tables) and the error message string.

If you set `log_traceback = True` on your `Enrichment` class a full stacktrace for the most recent exception will be recorded in the database table in addition to the string error message. This is useful during plugin development:

```python
class MyEnrichment(Enrichment):
    ...
    log_traceback = True
```

## Enrichments that use secrets such as API keys

Enrichments often need to access external APIs, for things like geocoding addresses or accessing Large Language Models. These APIs frequently need some kind of API key.

The enrichments system can work in combination with the [datasette-secrets](https://datasette.io/plugins/datasette-secrets) plugin to manage secret API keys needed for different services.

Examples of enrichments that use this mechanism include [datasette-enrichments-opencage](https://datasette.io/plugins/datasette-enrichments-opencage) and [datasette-enrichments-gpt](https://datasette.io/plugins/datasette-enrichments-gpt).

To define a secret that your plugin needs, add the following code:

```python
from datasette_enrichments import Enrichment
from datasette_secrets import Secret

# Then later in your enrichments class
class TrainEnthusiastsEnrichment(Enrichment):
    name = "Train Enthusiasts"
    slug = "train-enthusiasts"
    description = "Enrich with extra data from the Train Enthusiasts API"
    secret = Secret(
        name="TRAIN_ENTHUSIASTS_API_KEY",
        description="An API key from train-enthusiasts.doesnt.exist",
        obtain_url="https://train-enthusiasts.doesnt.exist/api-keys",
        obtain_label="Get an API key"
    )
```
Configuring your enrichment like this will result in the following behavior:

- If a user sets a `DATASETTE_SECRETS_TRAIN_ENTHUSIASTS_API_KEY` environment variable, that value will be used as the API key. You should mention this in your enrichment's documentation.
- Otherwise, if the user has configured [datasette-secrets](https://datasette.io/plugins/datasette-secrets) to store secrets in the database, admin users with the `manage-secrets` permission will get the option to set that secret as part of the Datasette web UI.
- If neither of the above are true, any time any user tries to run this enrichment they will be prompted to specify a secret which will be used for that run but will not be stored outside of memory used by the code that runs the enrichment.

With a secret configured in this way, your various class methods such as `initialize()`, `enrich_batch()` and `finalize()` can access the secret value using `await self.get_secret(datasette)` like this:

```python
api_key = await self.get_secret(datasette, config)
```
You must pass both the `datasette` and the `config` arguments that were passed to those methods.

## Writing tests for enrichments

Take a look at the [test suite for datasette-enrichments-opencage](https://github.com/datasette/datasette-enrichments-opencage/blob/main/tests/test_enrichments_opencage.py) for an example of how to test an enrichment.

You can use the `datasette_enrichments.wait_for_job()` utility function to avoid having to run a polling loop in your tests to wait for the enrichment to complete.

Here's an example test for an enrichment, using `datasette.client.post()` to start the enrichment running:

```python
from datasette.app import Datasette
from datasette_enrichments.utils import wait_for_job
import pytest
import sqlite3

@pytest.mark.asyncio
async def test_enrichment(tmpdir):
    db_path = str(tmpdir / "demo.db")
    datasette = Datasette([db_path])
    db = sqlite3.connect(db_path)
    with db:
        db.execute("create table if not exists news (body text)")
        for text in ("example a", "example b", "example c"):
            db.execute("insert into news (body) values (?)", [text])

    # Obtain ds_actor and ds_csrftoken cookies
    cookies = {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}
    csrftoken = (
        await datasette.client.get(
            "/-/enrich/demo/news/name-of-enrichment",
            cookies=cookies
        )
    ).cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken

    # Execute the enrichment
    response = await datasette.client.post(
        "/-/enrich/demo/news/name-of-enrichment",
        data={
            "source_column": "body",
            "csrftoken": cookies["ds_csrftoken"],
        },
        cookies=cookies,
    )
    assert response.status_code == 302

    # Get the job_id so we can wait for it to complete
    job_id = response.headers["Location"].split("=")[-1]
    await wait_for_job(datasette, job_id, database="demo", timeout=1)
    db = datasette.get_database("demo")
    jobs = await db.execute("select * from _enrichment_jobs")
    job = dict(jobs.first())
    assert job["status"] == "finished"
    assert job["enrichment"] == "name-of-enrichment"
    assert job["done_count"] == 3
    results = await db.execute("select * from news order by body")
    rows = [dict(r) for r in results.rows]
    assert rows == [
        {"body": "example a transformed"},
        {"body": "example b transformed"},
        {"body": "example c transformed"}
    ]
```

The full signature for `wait_for_job()` is:

```python
async def wait_for_job(
    datasette: Datasette,
    job_id: int,
    database: Optional[str] = None,
    timeout: Optional[int] = None,
):
```
The `timeout` argument to `wait_for_job()` is optional - this will cause the test to fail if the job does not complete within the specified number of seconds.

If you omit the `database` name the single first database will be used, which is often the right thing to do for tests.
