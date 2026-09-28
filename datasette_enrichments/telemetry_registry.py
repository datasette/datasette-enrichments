"""
Every span, metric and attribute datasette-enrichments emits.

Built from Datasette's plugin telemetry kit (``datasette.telemetry_registry``).
``Attribute`` / ``SpanName`` / ``MetricName`` subclass ``str``, so a registry
entry *is* the name handed to OpenTelemetry, and a typo is an ``ImportError``
rather than a silently misnamed signal. Call sites import these constants
and never hard-code wire names.

Scope: ``datasette_enrichments``. Prefix: ``enrichments.*`` (``datasette.*`` is
reserved for core). Shared names reused from core: ``db.namespace``,
``error.type``.

Privacy: no enrichment config, filter querystrings, row or primary-key
values, cursors, actor ids, exception messages or Pause/Cancel reasons are
ever recorded - counts, sizes and closed enums instead. Job ids and database
names ride on spans only, never on a metric.
"""

from datasette.telemetry_registry import (
    COUNTER,
    DB_NAMESPACE,
    ERROR_TYPE,
    HISTOGRAM,
    UPDOWN_COUNTER,
    Attribute,
    MetricName,
    SpanName,
)
from opentelemetry.trace import SpanKind

# --- Histogram bucket boundaries ------------------------------------------
#
# Core's DURATION_BUCKETS top out at 10s - right for SQL, wrong here: an LLM
# batch takes tens of seconds and a job run can take hours.
BATCH_DURATION_BUCKETS = (0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)
RUN_DURATION_BUCKETS = (1, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200, 21600, 86400)

# --- Attributes -----------------------------------------------------------

ENRICHMENT = Attribute(
    "enrichments.enrichment",
    "The enrichment's slug, e.g. ``openai``. Bounded by the installed "
    "enrichment plugins, so safe to use as a metric dimension.",
)
JOB_ID = Attribute(
    "enrichments.job_id",
    "The job's id in the database's ``_enrichment_jobs`` table. Spans only, "
    "never on a metric.",
)
TRIGGER = Attribute(
    "enrichments.trigger",
    "What started this run of the job: ``enqueue`` (a new job submitted "
    "through the enrichment form or API), ``resume`` (the Resume button on a "
    "paused job) or ``restart`` (the startup task resuming a job a previous "
    "process left ``running``).",
    values={"enqueue", "resume", "restart"},
)
RUN_OUTCOME = Attribute(
    "enrichments.run.outcome",
    "How this run of the job ended. ``finished``: the last batch was "
    "processed. ``paused`` / ``cancelled``: either ``enrich_batch()`` raised "
    "``self.Pause`` / ``self.Cancel``, or the loop saw the job's status "
    "changed to ``paused`` / ``cancelled`` (the Pause and Cancel buttons). "
    "``stopped``: the loop saw some other non-``running`` status, or the job "
    "row disappeared. ``interrupted``: the run was cancelled with "
    "``asyncio.CancelledError``, which means the process is shutting down; "
    "the job usually stays ``running`` and is resumed on the next start - "
    "unless the cancellation landed once the job was being marked "
    "``finished`` or during ``finalize()``, in which case it is not run "
    "again. ``error``: "
    "an unexpected exception escaped the run, e.g. a failing ``finalize()``. "
    "Only ``error`` sets span status ``ERROR``.",
    values={"finished", "paused", "cancelled", "stopped", "interrupted", "error"},
)
BATCH_OUTCOME = Attribute(
    "enrichments.batch.outcome",
    "How the batch ended. ``ok``: ``enrich_batch()`` returned, including "
    "when it logged errors for some rows, and the trailing fetch that "
    "returns no rows. ``error``: an exception escaped the batch and every "
    "row in the batch counts as an error. If ``enrich_batch()`` raised it, "
    "the error is logged and the run carries on; any other exception, e.g. "
    "a failed row fetch, also ends the run with ``error``. "
    "``paused`` / ``cancelled``: ``enrich_batch()`` "
    "raised ``self.Pause`` / ``self.Cancel``; its rows are processed again "
    "on resume. Only ``error`` sets span status ``ERROR``.",
    values={"ok", "error", "paused", "cancelled"},
)
ROW_RESULT = Attribute(
    "enrichments.row.result",
    "``success`` for rows ``enrich_batch()`` counted as enriched, ``error`` "
    "for the rest of the batch - rows it logged errors for, or every row of "
    "a batch that raised.",
    values={"success", "error"},
)
ROW_COUNT = Attribute(
    "enrichments.job.row_count",
    "Total rows the job targets, counted when the job was enqueued "
    "(``_enrichment_jobs.row_count``).",
)
RUN_BATCHES = Attribute(
    "enrichments.run.batches",
    "Batches this run processed: batches with at least one row that ended "
    "``ok`` or ``error``. A resumed job's earlier runs are not included.",
)
RUN_ROWS = Attribute(
    "enrichments.run.rows",
    "Rows this run processed, successful or not, across the batches counted "
    "by ``enrichments.run.batches``.",
)
BATCH_INDEX = Attribute(
    "enrichments.batch.index",
    "0-based index of the batch within this run - it restarts at 0 when a "
    "job is resumed.",
)
BATCH_ROWS = Attribute(
    "enrichments.batch.rows",
    "Rows fetched for this batch. ``0`` for the trailing fetch that finds "
    "no rows left.",
)
BATCH_SUCCESS = Attribute(
    "enrichments.batch.success_count",
    "Rows ``enrich_batch()`` reported as enriched (its return value, or "
    "every row when it returns ``None``). Set when ``enrich_batch()`` "
    "returned.",
)
RESTART_JOBS = Attribute(
    "enrichments.restart.jobs",
    "Jobs left ``running`` by a previous process that the restart task "
    "tried to resume. A job whose loop is already running in this process is "
    "counted but left alone.",
)
RESTART_UNKNOWN = Attribute(
    "enrichments.restart.unknown",
    "Jobs left ``running`` whose enrichment slug matches no installed "
    "enrichment, so they could not be resumed.",
)

ATTRIBUTES = (
    ENRICHMENT,
    JOB_ID,
    TRIGGER,
    RUN_OUTCOME,
    BATCH_OUTCOME,
    ROW_RESULT,
    ROW_COUNT,
    RUN_BATCHES,
    RUN_ROWS,
    BATCH_INDEX,
    BATCH_ROWS,
    BATCH_SUCCESS,
    RESTART_JOBS,
    RESTART_UNKNOWN,
    DB_NAMESPACE,
    ERROR_TYPE,
)

# --- Spans ----------------------------------------------------------------

JOB_RUN = SpanName(
    "enrichments.job.run",
    "One run of an enrichment job's batch loop, from claiming the job to "
    "finishing, pausing, cancelling or failing. A job has several runs if it "
    "is paused and resumed, or resumed after a restart. Each run is a **root "
    "span in its own trace** with a link back to whatever started it - the "
    "enqueue or resume request, or the ``enrichments.restart`` span - "
    "because a run outlives its cause. Status is ``ERROR`` only for "
    "``enrichments.run.outcome=error``.",
    (
        ENRICHMENT,
        JOB_ID,
        DB_NAMESPACE,
        TRIGGER,
        ROW_COUNT,
        RUN_OUTCOME,
        RUN_BATCHES,
        RUN_ROWS,
        ERROR_TYPE,
    ),
    kind=SpanKind.INTERNAL,
)
BATCH = SpanName(
    "enrichments.batch",
    "One batch within a run: fetching the rows, ``enrich_batch()`` and "
    "recording progress. A child of ``enrichments.job.run``; the row fetch's "
    "internal request and ``db.query`` spans, and any spans the enrichment "
    "itself emits (e.g. an instrumented HTTP client), nest under it. One "
    "span per batch, never one per row. Status is ``ERROR`` only for "
    "``enrichments.batch.outcome=error``; the run carries on.",
    (
        ENRICHMENT,
        JOB_ID,
        BATCH_INDEX,
        BATCH_ROWS,
        BATCH_SUCCESS,
        BATCH_OUTCOME,
        ERROR_TYPE,
    ),
    kind=SpanKind.INTERNAL,
)
INITIALIZE = SpanName(
    "enrichments.initialize",
    "The enrichment's ``initialize()`` call, made while handling the request "
    "that submits a new job - a child of that request's span. Status "
    "``ERROR`` if it raised.",
    (ENRICHMENT, DB_NAMESPACE, ERROR_TYPE),
    kind=SpanKind.INTERNAL,
)
FINALIZE = SpanName(
    "enrichments.finalize",
    "The enrichment's ``finalize()`` call once a job's last batch is done - "
    "a child of ``enrichments.job.run``. Status ``ERROR`` if it raised, "
    "which also makes the run's outcome ``error``.",
    (ENRICHMENT, JOB_ID, ERROR_TYPE),
    kind=SpanKind.INTERNAL,
)
RESTART = SpanName(
    "enrichments.restart",
    "The startup task that resumes jobs a previous process left "
    "``running``. A root span with a link to whatever was current when it "
    "started (on hosts without ASGI lifespan, the first request). Each job "
    "it resumes gets its own ``enrichments.job.run`` root span linked back "
    "to this one. Status ``ERROR`` if the task raised.",
    (RESTART_JOBS, RESTART_UNKNOWN, ERROR_TYPE),
    kind=SpanKind.INTERNAL,
)

SPANS = (JOB_RUN, BATCH, INITIALIZE, FINALIZE, RESTART)

# --- Metrics --------------------------------------------------------------
#
# Only bounded attributes: ENRICHMENT and the enum attributes. Never JOB_ID
# or DB_NAMESPACE.

RUNS = MetricName(
    "enrichments.job.runs",
    COUNTER,
    "{run}",
    "Job runs that ended, by enrichment, trigger and outcome.",
    (ENRICHMENT, TRIGGER, RUN_OUTCOME),
)
RUNS_ACTIVE = MetricName(
    "enrichments.job.runs.active",
    UPDOWN_COUNTER,
    "{run}",
    "Job runs in progress right now, by enrichment.",
    (ENRICHMENT,),
)
RUN_DURATION = MetricName(
    "enrichments.job.run.duration",
    HISTOGRAM,
    "s",
    "Duration of one job run, by enrichment and outcome. A job that is "
    "paused and resumed records one measurement per run.",
    (ENRICHMENT, RUN_OUTCOME),
    buckets=RUN_DURATION_BUCKETS,
)
BATCH_DURATION = MetricName(
    "enrichments.batch.duration",
    HISTOGRAM,
    "s",
    "Duration of one batch - fetch, ``enrich_batch()`` and progress writes - "
    "by enrichment and outcome.",
    (ENRICHMENT, BATCH_OUTCOME),
    buckets=BATCH_DURATION_BUCKETS,
)
ROWS = MetricName(
    "enrichments.rows",
    COUNTER,
    "{row}",
    "Rows processed, by enrichment and result. Rows of a paused or cancelled "
    "batch are not counted: they are processed again on resume.",
    (ENRICHMENT, ROW_RESULT),
)
COST = MetricName(
    "enrichments.cost",
    COUNTER,
    "{hundredth_cent}",
    "Cost reported by enrichments through ``increment_cost()``, in "
    "hundredths of a cent, by enrichment.",
    (ENRICHMENT,),
)

METRICS = (RUNS, RUNS_ACTIVE, RUN_DURATION, BATCH_DURATION, ROWS, COST)
