"""
OpenTelemetry integration for datasette-enrichments.

Depends on ``opentelemetry-api`` only, like Datasette core: this module never
creates a ``TracerProvider`` or ``MeterProvider``, never configures an
exporter, and must never import ``opentelemetry.sdk`` (a test imports the
package in a fresh subprocess and checks). With no provider installed every
span is a ``NonRecordingSpan`` and every instrument a no-op - turning
telemetry on is the operator's move (``opentelemetry-instrument datasette
...``), exactly as with core.

The tracer and meter live under their own ``datasette_enrichments``
instrumentation scope, versioned with the plugin. Every wire name comes from
``telemetry_registry``.

Instruments are module-level: OpenTelemetry's ``_ProxyMeter`` forwards to a
provider installed later. The ``ProxyTracer`` resolves and caches a concrete
tracer on first use, so the test harness installs its provider in a
session-scoped autouse fixture before any span is created.

Privacy: spans are started with ``record_exception=False`` and
``set_status_on_exception=False``. Exception messages can contain row data or
API responses, so only the exception class name (``error.type``) is ever
recorded, and error statuses carry no description.
"""

import asyncio
import time
from contextlib import contextmanager
from importlib import metadata

from datasette.telemetry import linked_root_span_kwargs
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace
from opentelemetry.trace import Status, StatusCode

from . import telemetry_registry as reg


def _version():
    try:
        return metadata.version("datasette-enrichments")
    except metadata.PackageNotFoundError:
        return None


# No schema URL: every attribute here is plugin-specific, and a wrong schema
# URL is worse than none.
tracer = otel_trace.get_tracer("datasette_enrichments", _version())
meter = otel_metrics.get_meter("datasette_enrichments", _version())


# --- Small helpers --------------------------------------------------------


def clamp(value, allowed, default):
    """Clamp an attribute value to its registry enum.

    Enum attributes declare a closed ``values=`` set in the registry; this is
    the call-site half of that promise - anything unexpected becomes
    ``default`` instead of minting a new metric series.
    """
    return value if value in allowed else default


def error_type(exception):
    "The ``error.type`` value for an exception: its class name, never the message."
    return type(exception).__qualname__


def set_attrs(span, mapping):
    "Set each attribute in ``mapping`` whose value is not None, if the span records."
    if not span.is_recording():
        return
    for key, value in mapping.items():
        if value is not None:
            span.set_attribute(key, value)


def _is_interruption(exception):
    # CancelledError is process shutdown (or a test cancelling the job task);
    # GeneratorExit is the coroutine being closed without finishing. Neither
    # is a failure of the enrichment.
    return isinstance(exception, (asyncio.CancelledError, GeneratorExit))


def _start_span(name, **kwargs):
    return tracer.start_as_current_span(
        name, record_exception=False, set_status_on_exception=False, **kwargs
    )


def _mark_error(span):
    if span.is_recording():
        span.set_status(Status(StatusCode.ERROR))


# --- Instruments ----------------------------------------------------------
#
# Unit and buckets always come off the registry entry so the create_*() call
# cannot drift from it (the kit's assert_metrics_conform checks them).


def _histogram(entry):
    return meter.create_histogram(
        entry,
        unit=entry.unit,
        description=entry.description,
        explicit_bucket_boundaries_advisory=entry.buckets,
    )


def _counter(entry):
    return meter.create_counter(entry, unit=entry.unit, description=entry.description)


def _up_down_counter(entry):
    return meter.create_up_down_counter(
        entry, unit=entry.unit, description=entry.description
    )


runs = _counter(reg.RUNS)
runs_active = _up_down_counter(reg.RUNS_ACTIVE)
run_duration = _histogram(reg.RUN_DURATION)
batch_duration = _histogram(reg.BATCH_DURATION)
rows = _counter(reg.ROWS)
cost = _counter(reg.COST)


# --- Job runs -------------------------------------------------------------


class RunRecorder:
    """What one ``enrichments.job.run`` span learns as the run progresses.

    Handed out by ``job_run_span()``. The call site reports how the run ended
    with ``set_outcome()``, ``stopped_with_status()`` or ``fail()``;
    ``batch_span(run)`` accumulates ``batches`` and ``rows``.
    """

    def __init__(self, enrichment, job_id):
        self.enrichment = enrichment
        self.job_id = job_id
        self.outcome = None
        self.error_type = None
        self.batches = 0
        self.rows = 0
        self.batch_spans_started = 0

    def set_outcome(self, outcome):
        "One of ``enrichments.run.outcome``'s values; anything else becomes ``stopped``."
        self.outcome = outcome

    def stopped_with_status(self, status):
        """The run stopped because the job's status is not ``running``:
        ``paused`` and ``cancelled`` map to themselves, anything else
        (including ``None`` for a missing row) to ``stopped``."""
        self.outcome = status if status in ("paused", "cancelled") else "stopped"

    def fail(self, exception):
        """Classify an exception that ended the run - one the call site
        caught itself, or one escaping the ``with`` block."""
        if _is_interruption(exception):
            self.outcome = "interrupted"
            self.error_type = None
        else:
            self.outcome = "error"
            self.error_type = error_type(exception)


@contextmanager
def job_run_span(enrichment, job, trigger, link_kwargs=None):
    """``enrichments.job.run`` around one run of a job's batch loop, plus the
    ``runs``, ``runs.active`` and ``run.duration`` metrics.

    ``enrichment`` is the enrichment's slug, ``job`` the ``_enrichment_jobs``
    row as a dict, ``trigger`` one of ``enqueue``/``resume``/``restart``.
    ``link_kwargs`` is ``datasette.telemetry.linked_root_span_kwargs()``,
    captured where the run was scheduled, so the run becomes a root span
    linked to its cause.

    Yields a ``RunRecorder``. An exception escaping the block is classified by
    ``fail()`` and re-raised. On every exit path the outcome (clamped;
    ``stopped`` when none was reported), batch and row totals are set on the
    span, status ``ERROR`` is set for ``error`` only, and ``runs.active``
    goes back down.
    """
    trigger = clamp(trigger, reg.TRIGGER.values, "enqueue")
    recorder = RunRecorder(enrichment, job["id"])
    active_attributes = {reg.ENRICHMENT: enrichment}
    started = time.perf_counter()
    runs_active.add(1, active_attributes)
    try:
        with _start_span(reg.JOB_RUN, **(link_kwargs or {})) as span:
            set_attrs(
                span,
                {
                    reg.ENRICHMENT: enrichment,
                    reg.JOB_ID: job["id"],
                    reg.DB_NAMESPACE: job.get("database_name"),
                    reg.TRIGGER: trigger,
                    reg.ROW_COUNT: job.get("row_count"),
                },
            )
            try:
                yield recorder
            except BaseException as exception:
                recorder.fail(exception)
                raise
            finally:
                recorder.outcome = clamp(
                    recorder.outcome, reg.RUN_OUTCOME.values, "stopped"
                )
                set_attrs(
                    span,
                    {
                        reg.RUN_OUTCOME: recorder.outcome,
                        reg.RUN_BATCHES: recorder.batches,
                        reg.RUN_ROWS: recorder.rows,
                        reg.ERROR_TYPE: recorder.error_type,
                    },
                )
                if recorder.outcome == "error":
                    _mark_error(span)
    finally:
        outcome = clamp(recorder.outcome, reg.RUN_OUTCOME.values, "stopped")
        runs.add(
            1,
            {
                reg.ENRICHMENT: enrichment,
                reg.TRIGGER: trigger,
                reg.RUN_OUTCOME: outcome,
            },
        )
        run_duration.record(
            time.perf_counter() - started,
            {reg.ENRICHMENT: enrichment, reg.RUN_OUTCOME: outcome},
        )
        runs_active.add(-1, active_attributes)


# --- Batches --------------------------------------------------------------


class BatchRecorder:
    """What one ``enrichments.batch`` span learns.

    Call ``fetched(len(rows))`` once the rows are in, then
    ``succeeded(success_count)`` when ``enrich_batch()`` returns,
    ``set_outcome("paused" | "cancelled")`` for Pause/Cancel, or
    ``fail(exception)`` for an exception the call site caught itself.
    """

    def __init__(self):
        self.rows = None
        self.success_count = None
        self.outcome = None
        self.error_type = None
        self.interrupted = False

    def fetched(self, row_count):
        self.rows = row_count

    def succeeded(self, success_count):
        self.success_count = success_count
        self.outcome = "ok"

    def set_outcome(self, outcome):
        "One of ``enrichments.batch.outcome``'s values; anything else becomes ``ok``."
        self.outcome = outcome

    def fail(self, exception):
        if _is_interruption(exception):
            # enrichments.batch.outcome has no value for this: the attribute
            # and the batch metrics are left off an interrupted batch
            self.interrupted = True
        else:
            self.outcome = "error"
            self.error_type = error_type(exception)


def _record_rows(enrichment, recorder):
    if not recorder.rows:
        return
    if recorder.outcome == "ok":
        success = recorder.success_count
        if success is None:
            success = recorder.rows
        # A buggy enrich_batch() could return more than len(rows), or a
        # negative number
        success = min(max(success, 0), recorder.rows)
        failed = recorder.rows - success
    elif recorder.outcome == "error":
        success, failed = 0, recorder.rows
    else:
        # paused / cancelled: those rows are processed again on resume
        return
    if success:
        rows.add(success, {reg.ENRICHMENT: enrichment, reg.ROW_RESULT: "success"})
    if failed:
        rows.add(failed, {reg.ENRICHMENT: enrichment, reg.ROW_RESULT: "error"})


@contextmanager
def batch_span(run, index=None):
    """``enrichments.batch`` around one batch of ``run`` (a ``RunRecorder``),
    plus the ``batch.duration`` and ``rows`` metrics.

    ``index`` defaults to the number of batch spans already started in this
    run. Yields a ``BatchRecorder``. On exit the outcome is clamped (``ok``
    when none was reported), status ``ERROR`` is set for ``error`` only, rows
    are counted as ``success``/``error`` for ``ok`` and ``error`` batches,
    and a batch with rows that ended ``ok`` or ``error`` adds to the run's
    ``batches`` and ``rows`` totals.
    """
    if index is None:
        index = run.batch_spans_started
    run.batch_spans_started += 1
    recorder = BatchRecorder()
    started = time.perf_counter()
    with _start_span(reg.BATCH) as span:
        set_attrs(
            span,
            {
                reg.ENRICHMENT: run.enrichment,
                reg.JOB_ID: run.job_id,
                reg.BATCH_INDEX: index,
            },
        )
        try:
            yield recorder
        except BaseException as exception:
            recorder.fail(exception)
            raise
        finally:
            elapsed = time.perf_counter() - started
            if recorder.interrupted:
                set_attrs(span, {reg.BATCH_ROWS: recorder.rows})
            else:
                outcome = clamp(recorder.outcome, reg.BATCH_OUTCOME.values, "ok")
                recorder.outcome = outcome
                set_attrs(
                    span,
                    {
                        reg.BATCH_ROWS: recorder.rows,
                        reg.BATCH_SUCCESS: recorder.success_count,
                        reg.BATCH_OUTCOME: outcome,
                        reg.ERROR_TYPE: recorder.error_type,
                    },
                )
                if outcome == "error":
                    _mark_error(span)
                batch_duration.record(
                    elapsed,
                    {reg.ENRICHMENT: run.enrichment, reg.BATCH_OUTCOME: outcome},
                )
                _record_rows(run.enrichment, recorder)
                if recorder.rows and outcome in ("ok", "error"):
                    run.batches += 1
                    run.rows += recorder.rows


# --- initialize() / finalize() -------------------------------------------


class SpanRecorder:
    "Handed out by ``initialize_span()`` / ``finalize_span()``."

    def __init__(self, span):
        self.span = span

    def fail(self, exception):
        """Mark the span failed for an exception the call site caught itself.
        An exception escaping the ``with`` block does this automatically."""
        if _is_interruption(exception):
            return
        set_attrs(self.span, {reg.ERROR_TYPE: error_type(exception)})
        _mark_error(self.span)


@contextmanager
def _call_span(name, attributes):
    with _start_span(name) as span:
        set_attrs(span, attributes)
        recorder = SpanRecorder(span)
        try:
            yield recorder
        except BaseException as exception:
            recorder.fail(exception)
            raise


def initialize_span(enrichment, database):
    """``enrichments.initialize`` around the enrichment's ``initialize()``
    call. ``enrichment`` is the slug, ``database`` the database name."""
    return _call_span(
        reg.INITIALIZE, {reg.ENRICHMENT: enrichment, reg.DB_NAMESPACE: database}
    )


def finalize_span(enrichment, job_id):
    """``enrichments.finalize`` around the enrichment's ``finalize()`` call.
    ``enrichment`` is the slug."""
    return _call_span(reg.FINALIZE, {reg.ENRICHMENT: enrichment, reg.JOB_ID: job_id})


# --- Restart --------------------------------------------------------------


class RestartRecorder:
    "Counts for ``enrichments.restart``: increment ``jobs`` and ``unknown``."

    def __init__(self):
        self.jobs = 0
        self.unknown = 0


@contextmanager
def restart_span():
    """``enrichments.restart`` around the task that resumes jobs left
    ``running``. Always a root span linked to whatever is current when it
    starts: on hosts without ASGI lifespan that is the first request, which
    the restart must not become a child of. Yields a ``RestartRecorder``."""
    recorder = RestartRecorder()
    with _start_span(reg.RESTART, **linked_root_span_kwargs()) as span:
        try:
            yield recorder
        except BaseException as exception:
            if not _is_interruption(exception):
                _mark_error(span)
            raise
        finally:
            set_attrs(
                span,
                {
                    reg.RESTART_JOBS: recorder.jobs,
                    reg.RESTART_UNKNOWN: recorder.unknown,
                },
            )


# --- Cost -----------------------------------------------------------------


def record_cost(enrichment, amount):
    "Add ``amount`` hundredths of a cent to ``enrichments.cost`` for this slug."
    if amount is not None and amount > 0:
        cost.add(amount, {reg.ENRICHMENT: enrichment})
