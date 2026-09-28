# Telemetry

Datasette 1.0a41 and later can emit [OpenTelemetry](https://opentelemetry.io/) traces and metrics. It instruments every HTTP request, every `db.query` and the write queue, as described in [Datasette's telemetry documentation](https://docs.datasette.io/en/latest/internals.html#internals-telemetry). Datasette Enrichments adds spans and metrics that describe enrichment jobs: which enrichment ran, how long each batch took, how many rows failed, why a job stopped and how much it cost.

Everything described here lives in the `datasette_enrichments` instrumentation scope, uses the `enrichments.*` prefix, and is declared in `datasette_enrichments/telemetry_registry.py`. A conformance test holds the plugin to that registry in both directions, and the {ref}`reference <telemetry_reference>` at the end of this page is generated from it.

## Turning it on

The plugin depends on `opentelemetry-api` only, just like Datasette core. It never installs a provider or an exporter. With no OpenTelemetry SDK configured every span is a no-op and every instrument does nothing, so telemetry costs nothing until you switch it on.

To switch it on, install the SDK, an exporter and the instrumentation agent, then run Datasette through `opentelemetry-instrument`:

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp

OTEL_SERVICE_NAME=datasette \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=otlp \
OTEL_LOGS_EXPORTER=none \
  opentelemetry-instrument datasette mydb.db
```

The enrichment spans and metrics then show up alongside core's. See [Turning tracing on](https://docs.datasette.io/en/latest/internals.html#internals-telemetry-turning-on) in the Datasette documentation for more options, including printing telemetry to the console and viewing traces locally, and [Telemetry for plugin authors](https://docs.datasette.io/en/latest/plugin_telemetry.html) for the conventions this plugin's spans and metrics follow.

## What a job looks like

An enrichment job runs in the background, long after the request that started it has returned. Each run of a job is therefore a **root span in a trace of its own**, with a [span link](https://opentelemetry.io/docs/concepts/signals/traces/#span-links) back to whatever started it. That is the shape Datasette recommends for background work.

Submitting a job through the enrichment form produces two traces:

```
POST /-/enrich/{database}/{table}/{enrichment}          SERVER (core)
│                                                       + enrichments.enrichment, enrichments.job_id
├── db.query ×N  (permission check, config form)        (core)
├── enrichments.initialize
├── GET /{database}/{table}.json  (counts the rows)     SERVER (core), datasette.internal_client=true
│   └── db.query ×N                                     (core)
├── db.query ×N  (create tables, insert the job)        (core)
└── db.query     (read the job back)                    (core)
          ┆
          ┆ link
          ▼
enrichments.job.run   trigger=enqueue                   root of a new trace
├── db.query     (claim the job: status = 'running')    (core)
├── db.query     (status check)                         (core)
├── enrichments.batch   batch.index=0
│   ├── GET /{database}/{table}.json  (fetch the rows)  SERVER (core), datasette.internal_client=true
│   │   └── db.query ×N                                 (core)
│   ├── db.query     (primary keys)                     (core)
│   ├── ... your enrich_batch(): its own db.query spans, instrumented HTTP clients, ...
│   ├── db.query     (record progress)                  (core)
│   └── db.query     (save the cursor, or mark the job finished)
├── db.query     (status check)                         (core)
├── enrichments.batch   batch.index=1
│   └── ...
└── enrichments.finalize
```

Writes also carry core's `db.write.queue_wait` and `db.write.execute` children, left out above.

The internal `GET ….json` requests nest under the POST and under each batch only because this plugin injects W3C trace context headers (`traceparent`) into its own `datasette.client` calls. This works around [a Datasette bug](https://github.com/simonw/datasette/issues/2969) where internal requests start a new trace. It uses the global propagator, so if you set `OTEL_PROPAGATORS=none` nothing is injected and each internal request shows up as a trace of its own, with its `db.query` spans, instead of inside the batch.

`finalize()` runs after the last batch's span has closed, so `enrichments.finalize` is a child of the run, not of the last batch.

### Resumed and restarted jobs

A job has one run span per run. Pausing and resuming a job, or restarting Datasette while a job is running, produces several runs, each in a new trace linked to its cause:

```
POST /-/enrich/{database}/-/jobs/{job_id}/resume        SERVER (core), + enrichments.job_id
└── db.query ×N  (update the status, read the job)      (core)
          ┆ link
          ▼
enrichments.job.run   trigger=resume                    root of a new trace
└── ... batches as above, carrying on from the saved cursor


enrichments.restart   restart.jobs=1 restart.unknown=0  root of a new trace
├── db.query ×N  (find jobs left 'running')             (core)
└── db.query     (read the job)                         (core)
          ┆ link
          ▼
enrichments.job.run   trigger=restart                   root of a new trace
└── ... batches as above, carrying on from the saved cursor
```

- `enrichments.restart` runs once when Datasette starts, and resumes every job a previous process left `running`. It is a root span too. On hosts without ASGI lifespan support, Datasette starts it from inside the first request, and the span links to that request rather than becoming its child. Jobs whose enrichment is no longer installed are counted in `enrichments.restart.unknown` and left alone.
- The Pause and Cancel buttons are plain requests whose spans carry `enrichments.job_id`. The running job notices the new status at its next status check, and its run ends with outcome `paused` or `cancelled`.
- If Resume is pressed while the paused run is still stopping, that run starts the next run itself as it exits. The new run still has `trigger=resume`, and it links to the run that stopped rather than to the Resume request.
- `enrichments.batch.index` starts again at 0 in every run.

### Details worth knowing

- **Empty fetches complete the job.** If a fetch returns no rows, the batch records `enrichments.batch.rows=0` and outcome `ok`, and the run ends `finished`. A filter that matches nothing gives a single batch like this. Datasette returns no `next` cursor for the last full page, so a trailing empty batch only happens when rows are deleted, or stop matching the filter, while the job is running.
- **`enrichments.run.batches` and `enrichments.run.rows` count only batches that had rows and ended `ok` or `error`.** Paused, cancelled and interrupted batches and empty fetches are not counted, and neither are earlier runs of the same job.
- **A batch that has succeeded stays `ok`.** Once `enrich_batch()` has returned, the batch's outcome is `ok` even if saving the cursor or marking the job finished then fails. That failure ends the run with outcome `error` instead.
- **An interrupted batch has no outcome.** If the process shuts down mid-batch, the batch span ends without `enrichments.batch.outcome`, and records no `enrichments.batch.duration` or `enrichments.rows`. The run ends with outcome `interrupted`.

## Outcomes

Every run ends with an `enrichments.run.outcome`:

| Outcome | Meaning | Span status |
|---|---|---|
| `finished` | The last batch was processed and `finalize()` returned. | `UNSET` |
| `paused` | `enrich_batch()` raised `self.Pause`, or the loop saw the job's status set to `paused` (the Pause button). | `UNSET` |
| `cancelled` | `enrich_batch()` raised `self.Cancel`, or the loop saw the job's status set to `cancelled` (the Cancel button). | `UNSET` |
| `stopped` | The loop saw some other status that is not `running`, or the job's row disappeared. | `UNSET` |
| `interrupted` | The run was cancelled, which means the process is shutting down. The job stays `running` and is resumed on the next start, unless the cancellation arrived while the job was being marked finished or during `finalize()`. | `UNSET` |
| `error` | An unexpected exception escaped the run, for example from a failing `finalize()`, a failed row fetch or a database error. The job's status becomes `error`. `error.type` records the exception class. | `ERROR` |

Every batch ends with an `enrichments.batch.outcome`, apart from an interrupted batch, which gets none:

| Outcome | Meaning | Span status |
|---|---|---|
| `ok` | `enrich_batch()` returned. That includes batches where it logged errors for some rows: `enrichments.batch.success_count` and the `enrichments.rows` metric show how many rows succeeded. | `UNSET` |
| `error` | An exception escaped the batch. `error.type` records the exception class and every row in the batch counts as an error. If `enrich_batch()` raised it, the error is logged and the run carries on with the next batch. Anything else, such as a failed row fetch, also ends the run with outcome `error`. | `ERROR` |
| `paused` / `cancelled` | `enrich_batch()` raised `self.Pause` / `self.Cancel`. Its rows are processed again on resume, so they are not counted in `enrichments.rows`. | `UNSET` |

A batch error raised by `enrich_batch()` never sets the run's status to `ERROR`: only an exception that escapes the batch some other way, such as a failed row fetch, does. `enrichments.initialize`, `enrichments.finalize` and `enrichments.restart` get status `ERROR` and `error.type` if the code they wrap raises. A failing `finalize()` also ends the run with outcome `error`. Error statuses on this plugin's spans never carry a description.

(telemetry_privacy)=
## Privacy

Enrichment jobs handle some of the most sensitive data in a Datasette instance, and telemetry is often exported to a third-party service. This plugin's own spans and metrics never record any of the following, in a span name, attribute, event or status, or in a metric attribute. Datasette core's spans have their own rules, listed further down.

- the enrichment's configuration: prompts, templates, and API keys or other secrets
- the filter that selected the rows
- row values or primary key values, including the cursor a job resumes from
- actor IDs
- exception messages or tracebacks: only the exception class name, as `error.type`
- the messages passed to `self.Pause()` or `self.Cancel()`, and the note saved when someone presses Pause, Resume or Cancel, which names them

Job IDs and database names are recorded on spans only, never as metric attributes. A test plants a unique string in each of these, runs jobs that exercise every span and metric, and checks that none of the strings appear anywhere in the telemetry, including in Datasette core's own spans.

Some things Datasette core records that you should know about. See [Privacy and safety](https://docs.datasette.io/en/latest/internals.html#internals-telemetry-privacy) in the Datasette documentation for the full list.

- **`_where=` filters are recorded.** Core's `db.query` spans record SQL text as `db.query.text`, with literals intact. Normal column filters such as `?name__contains=…` and the job's cursor are bound as parameters, which are never recorded. But a `_where=` clause becomes part of the SQL text of the query that fetches each batch, so any values typed into it are exported.
- **SQL text is recorded, including the SQL your enrichment runs.** It shows table and column names. Values passed as parameters are not recorded.
- **URL paths are recorded,** so database names, table names, enrichment slugs and job IDs appear on request spans. Query strings are not recorded.
- **Failed database calls record the exception message and traceback.** When a query or a write fails, core's `db.query` span gets status `ERROR` with the exception message as its description, plus an `exception` event carrying the message and traceback. That covers `execute()`, `execute_fn()` and every `execute_write*()` method, including exceptions raised inside your own `execute_write_fn()` callbacks. SQLite error messages can quote values from your SQL, and an exception your callback raises carries whatever message you gave it.
- **Exception messages can be recorded on a request span.** If an exception escapes Datasette's router, core's request span can carry its message in the status description. The router normally turns exceptions into a 500 response with no description.
- On a public instance, strip incoming `traceparent` headers at your proxy if callers should not be able to attach their own trace context.

(telemetry_reference)=
## Reference

Generated from `datasette_enrichments/telemetry_registry.py`.

<!-- [[[cog
import cog
from datasette_enrichments import telemetry_registry as reg


def cell(text):
    return " ".join(str(text).split()).replace("|", "\\|")


def names(entries):
    return ", ".join(
        f"`{entry}`" + (" (optional)" if getattr(entry, "optional", False) else "")
        for entry in entries
    ) or "-"


cog.outl("### Spans\n")
cog.outl("| Span | Kind | Attributes | Description |")
cog.outl("|---|---|---|---|")
for span in reg.SPANS:
    cog.outl(
        f"| `{span}` | `{span.kind.name}` | {names(span.attributes)} "
        f"| {cell(span.description)} |"
    )

cog.outl("\n### Metrics\n")
cog.outl(
    "Metric attributes are limited to the enrichment slug and closed sets of "
    "values, so the number of series stays bounded.\n"
)
cog.outl("| Metric | Instrument | Unit | Attributes | Description |")
cog.outl("|---|---|---|---|---|")
for metric in reg.METRICS:
    description = cell(metric.description)
    if metric.buckets:
        description += " Buckets: {}.".format(
            ", ".join(str(bucket) for bucket in metric.buckets)
        )
    cog.outl(
        f"| `{metric}` | {metric.kind} | `{metric.unit}` "
        f"| {names(metric.attributes)} | {description} |"
    )

cog.outl("\n### Attributes\n")
cog.outl(
    "`db.namespace` and `error.type` are shared with Datasette core; "
    "everything else is specific to this plugin. Besides the spans and "
    "metrics listed here, `enrichments.enrichment` and `enrichments.job_id` "
    "are added to core's request span for the request that submits a job, "
    "and `enrichments.job_id` to the request spans of the Pause, Resume and "
    "Cancel buttons.\n"
)
cog.outl("| Attribute | Values | Recorded on | Description |")
cog.outl("|---|---|---|---|")
for attribute in reg.ATTRIBUTES:
    values = (
        ", ".join(f"`{value}`" for value in sorted(attribute.values))
        if attribute.values
        else "-"
    )
    used_on = [
        entry for entry in (*reg.SPANS, *reg.METRICS) if attribute in entry.attributes
    ]
    cog.outl(
        f"| `{attribute}` | {values} | {names(used_on)} "
        f"| {cell(attribute.description)} |"
    )
]]] -->
### Spans

| Span | Kind | Attributes | Description |
|---|---|---|---|
| `enrichments.job.run` | `INTERNAL` | `enrichments.enrichment`, `enrichments.job_id`, `db.namespace`, `enrichments.trigger`, `enrichments.job.row_count`, `enrichments.run.outcome`, `enrichments.run.batches`, `enrichments.run.rows`, `error.type` (optional) | One run of an enrichment job's batch loop, from claiming the job to finishing, pausing, cancelling or failing. A job has several runs if it is paused and resumed, or resumed after a restart. Each run is a **root span in its own trace** with a link back to whatever started it - the enqueue or resume request, or the ``enrichments.restart`` span - because a run outlives its cause. Status is ``ERROR`` only for ``enrichments.run.outcome=error``. |
| `enrichments.batch` | `INTERNAL` | `enrichments.enrichment`, `enrichments.job_id`, `enrichments.batch.index`, `enrichments.batch.rows`, `enrichments.batch.success_count`, `enrichments.batch.outcome`, `error.type` (optional) | One batch within a run: fetching the rows, ``enrich_batch()`` and recording progress. A child of ``enrichments.job.run``; the row fetch's internal request and ``db.query`` spans, and any spans the enrichment itself emits (e.g. an instrumented HTTP client), nest under it. One span per batch, never one per row. Status is ``ERROR`` only for ``enrichments.batch.outcome=error``; the run carries on. |
| `enrichments.initialize` | `INTERNAL` | `enrichments.enrichment`, `db.namespace`, `error.type` (optional) | The enrichment's ``initialize()`` call, made while handling the request that submits a new job - a child of that request's span. Status ``ERROR`` if it raised. |
| `enrichments.finalize` | `INTERNAL` | `enrichments.enrichment`, `enrichments.job_id`, `error.type` (optional) | The enrichment's ``finalize()`` call once a job's last batch is done - a child of ``enrichments.job.run``. Status ``ERROR`` if it raised, which also makes the run's outcome ``error``. |
| `enrichments.restart` | `INTERNAL` | `enrichments.restart.jobs`, `enrichments.restart.unknown`, `error.type` (optional) | The startup task that resumes jobs a previous process left ``running``. A root span with a link to whatever was current when it started (on hosts without ASGI lifespan, the first request). Each job it resumes gets its own ``enrichments.job.run`` root span linked back to this one. Status ``ERROR`` if the task raised. |

### Metrics

Metric attributes are limited to the enrichment slug and closed sets of values, so the number of series stays bounded.

| Metric | Instrument | Unit | Attributes | Description |
|---|---|---|---|---|
| `enrichments.job.runs` | Counter | `{run}` | `enrichments.enrichment`, `enrichments.trigger`, `enrichments.run.outcome` | Job runs that ended, by enrichment, trigger and outcome. |
| `enrichments.job.runs.active` | UpDownCounter | `{run}` | `enrichments.enrichment` | Job runs in progress right now, by enrichment. |
| `enrichments.job.run.duration` | Histogram | `s` | `enrichments.enrichment`, `enrichments.run.outcome` | Duration of one job run, by enrichment and outcome. A job that is paused and resumed records one measurement per run. Buckets: 1, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200, 21600, 86400. |
| `enrichments.batch.duration` | Histogram | `s` | `enrichments.enrichment`, `enrichments.batch.outcome` | Duration of one batch - fetch, ``enrich_batch()`` and progress writes - by enrichment and outcome. Buckets: 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600. |
| `enrichments.rows` | Counter | `{row}` | `enrichments.enrichment`, `enrichments.row.result` | Rows processed, by enrichment and result. Rows of a paused or cancelled batch are not counted: they are processed again on resume. |
| `enrichments.cost` | Counter | `{hundredth_cent}` | `enrichments.enrichment` | Cost reported by enrichments through ``increment_cost()``, in hundredths of a cent, by enrichment. |

### Attributes

`db.namespace` and `error.type` are shared with Datasette core; everything else is specific to this plugin. Besides the spans and metrics listed here, `enrichments.enrichment` and `enrichments.job_id` are added to core's request span for the request that submits a job, and `enrichments.job_id` to the request spans of the Pause, Resume and Cancel buttons.

| Attribute | Values | Recorded on | Description |
|---|---|---|---|
| `enrichments.enrichment` | - | `enrichments.job.run`, `enrichments.batch`, `enrichments.initialize`, `enrichments.finalize`, `enrichments.job.runs`, `enrichments.job.runs.active`, `enrichments.job.run.duration`, `enrichments.batch.duration`, `enrichments.rows`, `enrichments.cost` | The enrichment's slug, e.g. ``openai``. Bounded by the installed enrichment plugins, so safe to use as a metric dimension. |
| `enrichments.job_id` | - | `enrichments.job.run`, `enrichments.batch`, `enrichments.finalize` | The job's id in the database's ``_enrichment_jobs`` table. Spans only, never on a metric. |
| `enrichments.trigger` | `enqueue`, `restart`, `resume` | `enrichments.job.run`, `enrichments.job.runs` | What started this run of the job: ``enqueue`` (a new job submitted through the enrichment form or API), ``resume`` (the Resume button on a paused job) or ``restart`` (the startup task resuming a job a previous process left ``running``). |
| `enrichments.run.outcome` | `cancelled`, `error`, `finished`, `interrupted`, `paused`, `stopped` | `enrichments.job.run`, `enrichments.job.runs`, `enrichments.job.run.duration` | How this run of the job ended. ``finished``: the last batch was processed. ``paused`` / ``cancelled``: either ``enrich_batch()`` raised ``self.Pause`` / ``self.Cancel``, or the loop saw the job's status changed to ``paused`` / ``cancelled`` (the Pause and Cancel buttons). ``stopped``: the loop saw some other non-``running`` status, or the job row disappeared. ``interrupted``: the run was cancelled with ``asyncio.CancelledError``, which means the process is shutting down; the job usually stays ``running`` and is resumed on the next start - unless the cancellation landed once the job was being marked ``finished`` or during ``finalize()``, in which case it is not run again. ``error``: an unexpected exception escaped the run, e.g. a failing ``finalize()``. Only ``error`` sets span status ``ERROR``. |
| `enrichments.batch.outcome` | `cancelled`, `error`, `ok`, `paused` | `enrichments.batch`, `enrichments.batch.duration` | How the batch ended. ``ok``: ``enrich_batch()`` returned, including when it logged errors for some rows, and the trailing fetch that returns no rows. ``error``: an exception escaped the batch and every row in the batch counts as an error. If ``enrich_batch()`` raised it, the error is logged and the run carries on; any other exception, e.g. a failed row fetch, also ends the run with ``error``. ``paused`` / ``cancelled``: ``enrich_batch()`` raised ``self.Pause`` / ``self.Cancel``; its rows are processed again on resume. Only ``error`` sets span status ``ERROR``. |
| `enrichments.row.result` | `error`, `success` | `enrichments.rows` | ``success`` for rows ``enrich_batch()`` counted as enriched, ``error`` for the rest of the batch - rows it logged errors for, or every row of a batch that raised. |
| `enrichments.job.row_count` | - | `enrichments.job.run` | Total rows the job targets, counted when the job was enqueued (``_enrichment_jobs.row_count``). |
| `enrichments.run.batches` | - | `enrichments.job.run` | Batches this run processed: batches with at least one row that ended ``ok`` or ``error``. A resumed job's earlier runs are not included. |
| `enrichments.run.rows` | - | `enrichments.job.run` | Rows this run processed, successful or not, across the batches counted by ``enrichments.run.batches``. |
| `enrichments.batch.index` | - | `enrichments.batch` | 0-based index of the batch within this run - it restarts at 0 when a job is resumed. |
| `enrichments.batch.rows` | - | `enrichments.batch` | Rows fetched for this batch. ``0`` for the trailing fetch that finds no rows left. |
| `enrichments.batch.success_count` | - | `enrichments.batch` | Rows ``enrich_batch()`` reported as enriched (its return value, or every row when it returns ``None``). Set when ``enrich_batch()`` returned. |
| `enrichments.restart.jobs` | - | `enrichments.restart` | Jobs left ``running`` by a previous process that the restart task tried to resume. A job whose loop is already running in this process is counted but left alone. |
| `enrichments.restart.unknown` | - | `enrichments.restart` | Jobs left ``running`` whose enrichment slug matches no installed enrichment, so they could not be resumed. |
| `db.namespace` | - | `enrichments.job.run`, `enrichments.initialize` | Name of the database being queried. |
| `error.type` | - | `enrichments.job.run`, `enrichments.batch`, `enrichments.initialize`, `enrichments.finalize`, `enrichments.restart` | The exception class name for a failed operation. On HTTP spans, also set to the status code as a string for 5xx responses. A 4xx response alone does not set this attribute or an error status. |
<!-- [[[end]]] -->
