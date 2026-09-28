import asyncio
import inspect
import json
import sqlite3
import subprocess
import sys
import textwrap
import time

import pytest
import pytest_asyncio

pytest.importorskip("opentelemetry.sdk")

from datasette import telemetry_registry as core
from datasette.app import Datasette
from datasette.telemetry_testing import (
    assert_metrics_conform,
    assert_metrics_covered,
    assert_no_forbidden_values,
    assert_package_never_imports_sdk,
    assert_spans_conform,
    assert_spans_covered,
)
from opentelemetry.trace import SpanKind, StatusCode

from datasette_enrichments import telemetry_registry as reg


def test_package_never_imports_the_sdk():
    # Front-loaded by conftest's pytest_collection_modifyitems
    assert_package_never_imports_sdk(
        "datasette_enrichments",
        "datasette_enrichments.telemetry",
        "datasette_enrichments.telemetry_registry",
    )


def test_import_without_provider_is_noop():
    """With no provider installed every span is non-recording and every
    instrument a no-op, and the helpers must not raise. Runs in a fresh
    interpreter because the test session has a provider installed.
    Front-loaded by conftest, like the SDK-import test."""
    script = textwrap.dedent(
        """
        from opentelemetry import trace

        import datasette_enrichments  # noqa: F401
        from datasette_enrichments import telemetry

        assert type(trace.get_tracer_provider()).__name__ == "ProxyTracerProvider"

        job = {"id": 1, "database_name": "data", "row_count": 2}
        with telemetry.job_run_span("uppercasedemo", job, "enqueue") as run:
            assert not trace.get_current_span().is_recording()
            with telemetry.batch_span(run) as batch:
                assert not trace.get_current_span().is_recording()
                batch.fetched(2)
                batch.succeeded(2)
            with telemetry.batch_span(run) as batch:
                batch.fetched(2)
                batch.fail(ValueError("secret row data"))
            with telemetry.finalize_span("uppercasedemo", 1):
                pass
            run.set_outcome("finished")
        with telemetry.initialize_span("uppercasedemo", "data"):
            pass
        with telemetry.restart_span() as restart:
            restart.jobs += 1
        telemetry.record_cost("uppercasedemo", 5)
        assert (run.batches, run.rows, run.outcome) == (2, 4, "finished")
        print("NO_PROVIDER_OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "NO_PROVIDER_OK" in result.stdout


# --- Instrumented jobs ----------------------------------------------------

SCOPE = "datasette_enrichments"


def ours(otel_spans, name=None):
    "Finished spans from this plugin's scope, optionally just those named ``name``."
    return [
        s
        for s in otel_spans.get_finished_spans()
        if s.instrumentation_scope.name == SCOPE and (name is None or s.name == name)
    ]


def for_job(job_id):
    "Span predicate: the span carries ``enrichments.job_id == job_id``."
    return lambda span: (span.attributes or {}).get(reg.JOB_ID) == job_id


async def wait_until(condition, description, timeout=5.0):
    "Poll ``condition`` (sync or async) until it returns something truthy."
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(
                f"Timed out after {timeout}s waiting for {description}"
            )
        await asyncio.sleep(0.01)


async def wait_for_span(otel_spans, name, predicate=lambda s: True, timeout=5.0):
    """The first finished span of ours named ``name`` matching ``predicate``.

    wait_for_job() returns when mark_job_complete() fires, which is BEFORE the
    run span's ``with`` block exits - so poll for the finished span instead.
    The run's metrics are recorded synchronously right after its span ends, so
    once the span is visible here they are recorded too."""
    found = await wait_until(
        lambda: [s for s in ours(otel_spans, name) if predicate(s)],
        f"a finished {name} span",
        timeout,
    )
    return found[0]


def job_spans(otel_spans, name, job_id):
    return [s for s in ours(otel_spans, name) if for_job(job_id)(s)]


def batches_of(otel_spans, run):
    "The run's batch spans, ordered by index."
    return sorted(
        (
            s
            for s in ours(otel_spans, reg.BATCH)
            if s.parent is not None and s.parent.span_id == run.context.span_id
        ),
        key=lambda s: s.attributes[reg.BATCH_INDEX],
    )


def server_spans(otel_spans, method, path):
    return [
        s
        for s in otel_spans.get_finished_spans()
        if s.kind == SpanKind.SERVER
        and s.attributes.get(core.HTTP_REQUEST_METHOD) == method
        and s.attributes.get(core.URL_PATH) == path
    ]


def assert_linked_to(span, cause):
    "``span`` is a root in its own trace, with a single link to ``cause``."
    assert span.parent is None
    assert span.context.trace_id != cause.context.trace_id
    assert len(span.links) == 1
    link = span.links[0].context
    assert (link.trace_id, link.span_id) == (
        cause.context.trace_id,
        cause.context.span_id,
    )


def assert_text_nowhere(spans, text):
    "``text`` appears in no attribute, event or status description."
    for span in spans:
        assert span.status.description is None or text not in span.status.description
        for value in (span.attributes or {}).values():
            assert text not in str(value), (span.name, value)
        for event in span.events:
            assert text not in event.name
            for value in (event.attributes or {}).values():
                assert text not in str(value), (span.name, event.name, value)


def active_runs(otel_metrics, slug):
    """Current ``runs.active`` for one enrichment, from the last collect().

    The UpDownCounter is CUMULATIVE, so its series persist across tests: always
    filter by enrichment and compare with the value from before the workload."""
    points = otel_metrics.points(reg.RUNS_ACTIVE, {reg.ENRICHMENT: slug})
    assert len(points) <= 1
    return points[0].value if points else 0


def actor_cookies(datasette, actor_id="root"):
    return {"ds_actor": datasette.sign({"a": {"id": actor_id}}, "actor")}


async def enqueue(
    datasette, table, slug, data=None, query="", database="data", actor_id="root"
):
    "Submit a job through the enrichment form; returns the new job's id."
    response = await datasette.client.post(
        f"/-/enrich/{database}/{table}/{slug}{query}",
        cookies=actor_cookies(datasette, actor_id),
        data=data or {},
    )
    assert response.status_code == 302, response.text
    return int(response.headers["location"].split("=")[-1])


async def job_action(datasette, job_id, action, database="data", actor_id="root"):
    "Press the Pause, Resume or Cancel button on a job."
    response = await datasette.client.post(
        f"/-/enrich/{database}/-/jobs/{job_id}/{action}",
        cookies=actor_cookies(datasette, actor_id),
        data={},
    )
    assert response.status_code == 302


def first_batch_in_flight(datasette):
    # The countbatches enrichment is blocked on datasette._enrich_gate
    return getattr(datasette, "_enrich_in_flight", 0) == 1


@pytest.mark.asyncio
async def test_job_run_is_linked_root_of_enqueue_request(datasette, otel_spans):
    job_id = await enqueue(datasette, "t", "uppercasedemo", {"columns": "s"})
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    (post,) = server_spans(otel_spans, "POST", "/-/enrich/data/t/uppercasedemo")
    assert_linked_to(run, post)
    assert run.attributes[reg.ENRICHMENT] == "uppercasedemo"
    assert run.attributes[reg.TRIGGER] == "enqueue"
    assert run.attributes[reg.RUN_OUTCOME] == "finished"
    assert run.attributes[reg.RUN_ROWS] == 2
    assert run.attributes[reg.RUN_BATCHES] == 1
    assert run.attributes[reg.ROW_COUNT] == 2
    assert run.attributes[reg.DB_NAMESPACE] == "data"
    assert run.attributes[reg.JOB_ID] == job_id
    assert reg.ERROR_TYPE not in run.attributes
    assert run.status.status_code == StatusCode.UNSET
    assert len(job_spans(otel_spans, reg.JOB_RUN, job_id)) == 1


@pytest.mark.asyncio
async def test_batch_spans_are_children_of_run(datasette, otel_spans):
    job_id = await enqueue(datasette, "has_50_rows", "countbatches")
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    batches = batches_of(otel_spans, run)
    # Datasette returns no next cursor for the last full page of 10, so there
    # is no trailing empty batch here
    assert [b.attributes[reg.BATCH_INDEX] for b in batches] == [0, 1, 2, 3, 4]
    for batch in batches:
        assert batch.context.trace_id == run.context.trace_id
        assert batch.attributes[reg.JOB_ID] == job_id
        assert batch.attributes[reg.ENRICHMENT] == "countbatches"
        assert batch.attributes[reg.BATCH_ROWS] == 10
        assert batch.attributes[reg.BATCH_SUCCESS] == 10
        assert batch.attributes[reg.BATCH_OUTCOME] == "ok"
        assert batch.status.status_code == StatusCode.UNSET
    assert run.attributes[reg.RUN_BATCHES] == 5
    assert run.attributes[reg.RUN_ROWS] == 50

    # The row fetch's internal request nests under its batch, in the run's
    # trace - thanks to the injected trace headers
    everything = otel_spans.get_finished_spans()
    in_trace = [s for s in everything if s.context.trace_id == run.context.trace_id]
    internal = [
        s
        for s in in_trace
        if s.kind == SpanKind.SERVER and s.attributes.get(core.INTERNAL_CLIENT)
    ]
    assert sorted(s.parent.span_id for s in internal) == sorted(
        b.context.span_id for b in batches
    )
    children = {}
    for span in in_trace:
        if span.parent is not None:
            children.setdefault(span.parent.span_id, []).append(span)

    def descendants(span):
        for child in children.get(span.context.span_id, []):
            yield child
            yield from descendants(child)

    for request in internal:
        assert any(d.name == core.DB_QUERY for d in descendants(request))
    # finalize() is a child of the run, not of the last batch
    (finalize,) = job_spans(otel_spans, reg.FINALIZE, job_id)
    assert finalize.parent.span_id == run.context.span_id


@pytest.mark.asyncio
async def test_trailing_empty_batch(datasette, otel_spans):
    # The first batch returns a next cursor, but the remaining rows are gone by
    # the time the loop fetches again: a trailing batch with no rows
    datasette._enrich_gate = asyncio.Event()
    job_id = await enqueue(datasette, "has_50_rows", "countbatches")
    await wait_until(lambda: first_batch_in_flight(datasette), "first batch")
    with datasette._test_db:
        datasette._test_db.execute("delete from has_50_rows where id > 10")
    datasette._enrich_gate.set()
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    first, empty = batches_of(otel_spans, run)
    assert first.attributes[reg.BATCH_ROWS] == 10
    assert empty.attributes[reg.BATCH_INDEX] == 1
    assert empty.attributes[reg.BATCH_ROWS] == 0
    assert empty.attributes[reg.BATCH_OUTCOME] == "ok"
    assert reg.BATCH_SUCCESS not in empty.attributes
    assert run.attributes[reg.RUN_OUTCOME] == "finished"
    # Only batches with rows count towards the run's totals
    assert run.attributes[reg.RUN_BATCHES] == 1
    assert run.attributes[reg.RUN_ROWS] == 10


@pytest.mark.asyncio
async def test_initialize_span_and_request_span_attributes(datasette, otel_spans):
    job_id = await enqueue(datasette, "t", "uppercasedemo", {"columns": "s"})
    (post,) = server_spans(otel_spans, "POST", "/-/enrich/data/t/uppercasedemo")
    assert post.attributes[reg.ENRICHMENT] == "uppercasedemo"
    assert post.attributes[reg.JOB_ID] == job_id
    (initialize,) = ours(otel_spans, reg.INITIALIZE)
    assert initialize.parent.span_id == post.context.span_id
    assert initialize.attributes[reg.ENRICHMENT] == "uppercasedemo"
    assert initialize.attributes[reg.DB_NAMESPACE] == "data"
    assert initialize.status.status_code == StatusCode.UNSET
    # enqueue()'s internal count request nests under the POST too
    counts = [
        s
        for s in otel_spans.get_finished_spans()
        if s.kind == SpanKind.SERVER
        and s.attributes.get(core.INTERNAL_CLIENT)
        and s.parent is not None
        and s.parent.span_id == post.context.span_id
    ]
    assert len(counts) == 1
    # Let the job finish inside this test
    await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))


@pytest.mark.asyncio
async def test_batch_error_marks_batch_not_run(datasette, otel_spans, otel_metrics):
    datasette._trigger_enrich_batch_error = True
    job_id = await enqueue(datasette, "t", "uppercasedemo", {"columns": "s"})
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    batches = batches_of(otel_spans, run)
    assert batches
    for batch in batches:
        assert batch.attributes[reg.BATCH_OUTCOME] == "error"
        assert batch.attributes[reg.ERROR_TYPE] == "Exception"
        assert batch.status.status_code == StatusCode.ERROR
        assert reg.BATCH_SUCCESS not in batch.attributes
    assert run.attributes[reg.RUN_OUTCOME] == "finished"
    assert run.attributes[reg.RUN_BATCHES] == 1
    assert run.attributes[reg.RUN_ROWS] == 2
    assert reg.ERROR_TYPE not in run.attributes
    assert run.status.status_code == StatusCode.UNSET
    assert_text_nowhere(otel_spans.get_finished_spans(), "Error in enrich_batch")
    otel_metrics.collect()
    labels = {reg.ENRICHMENT: "uppercasedemo"}
    assert otel_metrics.point(reg.ROWS, {**labels, reg.ROW_RESULT: "error"}).value == 2
    assert not otel_metrics.points(reg.ROWS, {**labels, reg.ROW_RESULT: "success"})
    assert (
        otel_metrics.point(
            reg.BATCH_DURATION, {**labels, reg.BATCH_OUTCOME: "error"}
        ).count
        == 1
    )


@pytest.mark.asyncio
async def test_partial_row_errors(datasette, otel_spans, otel_metrics):
    job_id = await enqueue(datasette, "has_50_rows", "haserrors")
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    batches = batches_of(otel_spans, run)
    assert len(batches) == 5
    for batch in batches:
        assert batch.attributes[reg.BATCH_OUTCOME] == "ok"
        assert batch.attributes[reg.BATCH_ROWS] == 10
        assert batch.attributes[reg.BATCH_SUCCESS] == 8
        assert batch.status.status_code == StatusCode.UNSET
    assert run.attributes[reg.RUN_OUTCOME] == "finished"
    otel_metrics.collect()
    labels = {reg.ENRICHMENT: "haserrors"}
    # len(rows) - success_count = 2 errors per batch
    assert otel_metrics.point(reg.ROWS, {**labels, reg.ROW_RESULT: "error"}).value == (
        2 * 5
    )
    assert (
        otel_metrics.point(reg.ROWS, {**labels, reg.ROW_RESULT: "success"}).value
        == 8 * 5
    )


@pytest.mark.asyncio
async def test_pause_and_cancel_exceptions_outcomes(datasette, otel_spans):
    job_id = await enqueue(datasette, "has_50_rows", "queue")
    await wait_until(
        lambda: hasattr(datasette, "enrichment_queue"), "initialize() to run"
    )
    queue = datasette.enrichment_queue
    await queue.put("0")
    await queue.put("pause")
    paused = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    assert paused.attributes[reg.TRIGGER] == "enqueue"
    assert paused.attributes[reg.RUN_OUTCOME] == "paused"
    assert paused.status.status_code == StatusCode.UNSET
    assert [
        b.attributes[reg.BATCH_OUTCOME] for b in batches_of(otel_spans, paused)
    ] == [
        "ok",
        "paused",
    ]
    # Paused rows are processed again on resume, so they do not count
    assert paused.attributes[reg.RUN_BATCHES] == 1
    assert paused.attributes[reg.RUN_ROWS] == 1

    await job_action(datasette, job_id, "resume")
    await queue.put("cancel")
    cancelled = await wait_for_span(
        otel_spans,
        reg.JOB_RUN,
        lambda s: for_job(job_id)(s) and s.attributes[reg.TRIGGER] == "resume",
    )
    assert cancelled.attributes[reg.RUN_OUTCOME] == "cancelled"
    assert cancelled.status.status_code == StatusCode.UNSET
    (batch,) = batches_of(otel_spans, cancelled)
    assert batch.attributes[reg.BATCH_INDEX] == 0
    assert batch.attributes[reg.BATCH_OUTCOME] == "cancelled"
    assert batch.status.status_code == StatusCode.UNSET
    spans = otel_spans.get_finished_spans()
    assert_text_nowhere(spans, "pause message")
    assert_text_nowhere(spans, "cancel message")


@pytest.mark.asyncio
async def test_ui_pause_then_resume(datasette, otel_spans):
    datasette._enrich_gate = asyncio.Event()
    job_id = await enqueue(datasette, "has_50_rows", "countbatches")
    await wait_until(lambda: first_batch_in_flight(datasette), "first batch")
    await job_action(datasette, job_id, "pause")
    # Let the batch finish: the loop then sees 'paused' and stops
    datasette._enrich_gate.set()
    paused = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    assert paused.attributes[reg.TRIGGER] == "enqueue"
    assert paused.attributes[reg.RUN_OUTCOME] == "paused"
    assert paused.attributes[reg.RUN_BATCHES] == 1
    assert paused.status.status_code == StatusCode.UNSET
    (pause_post,) = server_spans(
        otel_spans, "POST", f"/-/enrich/data/-/jobs/{job_id}/pause"
    )
    assert pause_post.attributes[reg.JOB_ID] == job_id

    await job_action(datasette, job_id, "resume")
    resumed = await wait_for_span(
        otel_spans,
        reg.JOB_RUN,
        lambda s: for_job(job_id)(s) and s.attributes[reg.TRIGGER] == "resume",
    )
    (resume_post,) = server_spans(
        otel_spans, "POST", f"/-/enrich/data/-/jobs/{job_id}/resume"
    )
    assert_linked_to(resumed, resume_post)
    assert resumed.context.trace_id != paused.context.trace_id
    assert resumed.attributes[reg.RUN_OUTCOME] == "finished"
    assert resumed.attributes[reg.RUN_BATCHES] == 4
    assert resumed.attributes[reg.RUN_ROWS] == 40
    # Batch indexes restart at 0 in the new run
    assert [b.attributes[reg.BATCH_INDEX] for b in batches_of(otel_spans, resumed)] == [
        0,
        1,
        2,
        3,
    ]
    assert len(job_spans(otel_spans, reg.JOB_RUN, job_id)) == 2


async def _insert_running_job(datasette, enrichment):
    # Straight into the database: a datasette.client request would launch the
    # background tasks, including the restart scan, too early
    from datasette_enrichments import ensure_tables

    db = datasette.get_database("data")
    await ensure_tables(db)
    return (
        await db.execute_write(
            """
            insert into _enrichment_jobs (
                status, enrichment, database_name, table_name, filter_querystring,
                config, next_cursor, row_count, done_count, error_count
            ) values ('running', ?, 'data', 'has_50_rows', '', '{}', '40', 50, 40, 0)
            """,
            (enrichment,),
        )
    ).lastrowid


@pytest.mark.asyncio
async def test_restart_span_and_restarted_run(datasette, otel_spans):
    job_id = await _insert_running_job(datasette, "countbatches")
    await _insert_running_job(datasette, "no-such-enrichment")
    await datasette.start_background_tasks()
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    (restart,) = ours(otel_spans, reg.RESTART)
    assert restart.parent is None
    assert restart.attributes[reg.RESTART_JOBS] == 1
    assert restart.attributes[reg.RESTART_UNKNOWN] == 1
    assert restart.status.status_code == StatusCode.UNSET
    assert_linked_to(run, restart)
    assert run.attributes[reg.TRIGGER] == "restart"
    assert run.attributes[reg.RUN_OUTCOME] == "finished"
    # Resumed from the cursor: rows 41-50
    assert run.attributes[reg.RUN_ROWS] == 10


@pytest.mark.asyncio
async def test_finalize_span(datasette, otel_spans):
    job_id = await enqueue(datasette, "t", "uppercasedemo", {"columns": "s"})
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    (finalize,) = job_spans(otel_spans, reg.FINALIZE, job_id)
    assert finalize.parent.span_id == run.context.span_id
    assert finalize.attributes[reg.ENRICHMENT] == "uppercasedemo"
    assert reg.ERROR_TYPE not in finalize.attributes
    assert finalize.status.status_code == StatusCode.UNSET


@pytest.mark.asyncio
async def test_finalize_error_makes_run_error(datasette, otel_spans):
    job_id = await enqueue(datasette, "t", "finalizeraises")
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    (finalize,) = job_spans(otel_spans, reg.FINALIZE, job_id)
    assert finalize.parent.span_id == run.context.span_id
    assert finalize.attributes[reg.ERROR_TYPE] == "RuntimeError"
    assert finalize.status.status_code == StatusCode.ERROR
    assert run.attributes[reg.RUN_OUTCOME] == "error"
    assert run.attributes[reg.ERROR_TYPE] == "RuntimeError"
    assert run.status.status_code == StatusCode.ERROR
    # The batch itself succeeded
    (batch,) = batches_of(otel_spans, run)
    assert batch.attributes[reg.BATCH_OUTCOME] == "ok"
    assert_text_nowhere(otel_spans.get_finished_spans(), "finalize() failed")


@pytest.mark.asyncio
async def test_interrupted_run(datasette, otel_spans, otel_metrics):
    otel_metrics.collect()
    before = active_runs(otel_metrics, "countbatches")
    datasette._enrich_gate = asyncio.Event()  # never set
    job_id = await enqueue(datasette, "has_50_rows", "countbatches")
    await wait_until(lambda: first_batch_in_flight(datasette), "first batch")
    otel_metrics.collect()
    assert active_runs(otel_metrics, "countbatches") == before + 1

    (task,) = datasette._enrichment_job_tasks.values()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()

    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    assert run.attributes[reg.RUN_OUTCOME] == "interrupted"
    assert reg.ERROR_TYPE not in run.attributes
    assert run.status.status_code == StatusCode.UNSET
    (batch,) = batches_of(otel_spans, run)
    # No batch outcome has a value for an interrupted batch, so none is set
    assert reg.BATCH_OUTCOME not in batch.attributes
    assert batch.attributes[reg.BATCH_ROWS] == 10
    assert batch.status.status_code == StatusCode.UNSET
    otel_metrics.collect()
    assert active_runs(otel_metrics, "countbatches") == before
    assert (
        otel_metrics.point(
            reg.RUNS,
            {
                reg.ENRICHMENT: "countbatches",
                reg.TRIGGER: "enqueue",
                reg.RUN_OUTCOME: "interrupted",
            },
        ).value
        == 1
    )


@pytest.mark.asyncio
async def test_run_metrics(datasette, otel_spans, otel_metrics):
    slug = "countbatches"
    otel_metrics.collect()
    before = active_runs(otel_metrics, slug)
    job_id = await enqueue(datasette, "has_50_rows", slug)
    await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))

    otel_metrics.collect()
    labels = {reg.ENRICHMENT: slug}
    finished = {reg.RUN_OUTCOME: "finished"}
    assert (
        otel_metrics.point(
            reg.RUNS, {**labels, **finished, reg.TRIGGER: "enqueue"}
        ).value
        == 1
    )
    assert otel_metrics.point(reg.RUN_DURATION, {**labels, **finished}).count == 1
    assert (
        otel_metrics.point(
            reg.BATCH_DURATION, {**labels, reg.BATCH_OUTCOME: "ok"}
        ).count
        == 5
    )
    assert (
        otel_metrics.point(reg.ROWS, {**labels, reg.ROW_RESULT: "success"}).value == 50
    )
    assert not otel_metrics.points(reg.ROWS, {**labels, reg.ROW_RESULT: "error"})
    assert active_runs(otel_metrics, slug) == before
    # Job ids and database names never become metric dimensions
    for name in reg.METRICS:
        for point in otel_metrics.snapshot.get(name, []):
            assert reg.JOB_ID not in point.attributes
            assert reg.DB_NAMESPACE not in point.attributes


@pytest.mark.asyncio
async def test_cost_metric(datasette, otel_spans, otel_metrics):
    job_id = await enqueue(datasette, "has_50_rows", "costdemo")
    await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    otel_metrics.collect()
    assert otel_metrics.point(reg.COST, {reg.ENRICHMENT: "costdemo"}).value == 5 * 5
    # The same total the job row records
    assert (
        datasette._test_db.execute(
            "select cost_100ths_cent from _enrichment_jobs where id = ?", (job_id,)
        ).fetchone()[0]
        == 25
    )


@pytest.mark.asyncio
async def test_resume_racing_a_stopping_loop_keeps_paused(
    datasette, otel_spans, monkeypatch
):
    # The loop sees 'paused' and stops; before it deregisters, a Resume sets
    # 'running' but does nothing more because the task is still registered.
    # The stopping run is still 'paused', and the loop restarts the job itself
    # as a resume.
    import datasette_enrichments

    real_forget = datasette_enrichments._forget_job_task
    resumed = []

    def forget_after_resume(registry, key, task):
        if not resumed:
            with datasette._test_db:
                datasette._test_db.execute(
                    "update _enrichment_jobs set status = 'running'"
                )
            resumed.append(task)
        return real_forget(registry, key, task)

    monkeypatch.setattr(datasette_enrichments, "_forget_job_task", forget_after_resume)
    datasette._enrich_gate = asyncio.Event()
    job_id = await enqueue(datasette, "has_50_rows", "countbatches")
    await wait_until(lambda: first_batch_in_flight(datasette), "first batch")
    await job_action(datasette, job_id, "pause")
    datasette._enrich_gate.set()
    first = await wait_for_span(
        otel_spans,
        reg.JOB_RUN,
        lambda s: for_job(job_id)(s) and s.attributes[reg.TRIGGER] == "enqueue",
    )
    second = await wait_for_span(
        otel_spans,
        reg.JOB_RUN,
        lambda s: for_job(job_id)(s) and s.attributes[reg.TRIGGER] == "resume",
    )
    assert resumed
    assert first.attributes[reg.RUN_OUTCOME] == "paused"
    assert_linked_to(second, first)
    assert second.attributes[reg.RUN_OUTCOME] == "finished"
    assert len(job_spans(otel_spans, reg.JOB_RUN, job_id)) == 2


@pytest.mark.asyncio
async def test_restart_from_first_request_is_linked_root(datasette, otel_spans):
    # On hosts without ASGI lifespan, background tasks launch from inside the
    # first request's span. The restart span must link to it, not be its child.
    job_id = await _insert_running_job(datasette, "countbatches")
    response = await datasette.client.get("/-/versions.json")
    assert response.status_code == 200
    run = await wait_for_span(otel_spans, reg.JOB_RUN, for_job(job_id))
    (restart,) = ours(otel_spans, reg.RESTART)
    (first_request,) = server_spans(otel_spans, "GET", "/-/versions.json")
    assert_linked_to(restart, first_request)
    assert restart.attributes[reg.RESTART_JOBS] == 1
    assert_linked_to(run, restart)
    assert run.attributes[reg.TRIGGER] == "restart"


def test_non_numeric_success_count_does_not_crash(otel_spans, otel_metrics):
    from datasette_enrichments import telemetry

    job = {"id": 1, "database_name": "data", "row_count": 20}
    with telemetry.job_run_span("stringcount", job, "enqueue") as run:
        with telemetry.batch_span(run) as batch:
            batch.fetched(10)
            batch.succeeded("8")  # coerced, as SQLite would
        with telemetry.batch_span(run) as batch:
            batch.fetched(10)
            batch.succeeded("lots")  # not a number: every row succeeded
        run.set_outcome("finished")
    first, second = ours(otel_spans, reg.BATCH)
    assert first.attributes[reg.BATCH_SUCCESS] == 8
    assert reg.BATCH_SUCCESS not in second.attributes
    assert second.attributes[reg.BATCH_OUTCOME] == "ok"
    otel_metrics.collect()
    labels = {reg.ENRICHMENT: "stringcount"}
    assert otel_metrics.point(
        reg.ROWS, {**labels, reg.ROW_RESULT: "success"}
    ).value == (18)
    assert otel_metrics.point(reg.ROWS, {**labels, reg.ROW_RESULT: "error"}).value == 2


def test_failure_after_enrich_batch_keeps_batch_ok(otel_spans, otel_metrics):
    # e.g. the next_cursor write fails after enrich_batch() returned: the rows
    # were enriched, so the batch stays ok and the run records the error
    from datasette_enrichments import telemetry

    job = {"id": 1, "database_name": "data", "row_count": 10}
    with (
        pytest.raises(RuntimeError),
        telemetry.job_run_span("latefailure", job, "enqueue") as run,
        telemetry.batch_span(run) as batch,
    ):
        batch.fetched(10)
        batch.succeeded(10)
        raise RuntimeError("cursor write failed")
    (batch_span,) = ours(otel_spans, reg.BATCH)
    (run_span,) = ours(otel_spans, reg.JOB_RUN)
    assert batch_span.attributes[reg.BATCH_OUTCOME] == "ok"
    assert reg.ERROR_TYPE not in batch_span.attributes
    assert batch_span.status.status_code == StatusCode.UNSET
    assert run_span.attributes[reg.RUN_OUTCOME] == "error"
    assert run_span.attributes[reg.ERROR_TYPE] == "RuntimeError"
    assert run_span.status.status_code == StatusCode.ERROR
    otel_metrics.collect()
    labels = {reg.ENRICHMENT: "latefailure"}
    assert (
        otel_metrics.point(reg.ROWS, {**labels, reg.ROW_RESULT: "success"}).value == 10
    )
    assert not otel_metrics.points(reg.ROWS, {**labels, reg.ROW_RESULT: "error"})


def test_restart_span_failure_records_error_type(otel_spans):
    from datasette_enrichments import telemetry

    with pytest.raises(KeyError), telemetry.restart_span():
        raise KeyError("secret-ish")
    (restart,) = ours(otel_spans, reg.RESTART)
    assert restart.attributes[reg.ERROR_TYPE] == "KeyError"
    assert restart.status.status_code == StatusCode.ERROR
    assert_text_nowhere([restart], "secret-ish")


# --- Registry conformance and privacy -------------------------------------
#
# One broad workload that makes every registered span and metric fire, with a
# distinct sentinel string planted in every piece of user data a job touches.
# Then a single collect() - the counters and histograms are DELTA, so an
# earlier collect() would hide what it drained - and the kit's conformance
# assertions in both directions, plus the privacy walk across EVERY scope:
# a sentinel leaking through core's request or db.query spans is still a leak.

SENTINEL_CONFIG = "SENTINEL_CONFIG_7f3a"
SENTINEL_FILTER = "SENTINEL_FILTER_2b9e"
SENTINEL_ROW = "SENTINEL_ROW_5c1d"
SENTINEL_PK = "SENTINEL_PK_8e4f"
SENTINEL_ACTOR = "SENTINEL_ACTOR_3a6b"
SENTINEL_EXC = "SENTINEL_EXC_9d2c"
SENTINEL_PAUSE = "SENTINEL_PAUSE_1f7e"
SENTINEL_CANCEL = "SENTINEL_CANCEL_6b3a"
SENTINEL_SECRET = "SENTINEL_SECRET_4e8d"
FORBIDDEN = {
    SENTINEL_CONFIG,
    SENTINEL_FILTER,
    SENTINEL_ROW,
    SENTINEL_PK,
    SENTINEL_ACTOR,
    SENTINEL_EXC,
    SENTINEL_PAUSE,
    SENTINEL_CANCEL,
    SENTINEL_SECRET,
}
# Every row matches this filter; a normal column filter, so core binds the
# value as a parameter. (A _where= literal would reach core's db.query.text -
# documented in docs/telemetry.md, deliberately not planted here.)
SENTINEL_QUERY = f"?s__not={SENTINEL_FILTER}"
PEOPLE_ROWS = 25


def person_id(i):
    # A text primary key, so batch cursors (next_cursor, _next=) and the ids
    # passed to log_error() carry the sentinel
    return f"{SENTINEL_PK}_{i:03}"


@pytest_asyncio.fixture
async def private_datasette(tmpdir, monkeypatch):
    # The secretreplace enrichment's API key, read from the environment
    monkeypatch.setenv("DATASETTE_SECRETS_STRING_SECRET", SENTINEL_SECRET)
    path = str(tmpdir / "private.db")
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("create table people (id text primary key, s text)")
        for i in range(PEOPLE_ROWS):
            # secretreplace swaps SENTINEL_CONFIG for the secret in these
            conn.execute(
                "insert into people (id, s) values (?, ?)",
                (person_id(i), f"{SENTINEL_ROW} {i} {SENTINEL_CONFIG}"),
            )
    datasette = Datasette(
        [path],
        # Only the sentinel actor may run enrichments
        config={"permissions": {"enrichments": {"id": SENTINEL_ACTOR}}},
    )
    datasette._test_db = conn
    await datasette.invoke_startup()
    yield datasette
    tasks = list(getattr(datasette, "_enrichment_job_tasks", {}).values())
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    conn.close()


async def _insert_private_running_job(datasette, enrichment, next_cursor):
    # A job a previous process left 'running', carrying every sentinel it can
    from datasette_enrichments import ensure_tables

    db = datasette.get_database("private")
    await ensure_tables(db)
    return (
        await db.execute_write(
            """
            insert into _enrichment_jobs (
                status, enrichment, database_name, table_name,
                filter_querystring, config, next_cursor, row_count,
                done_count, error_count, actor_id
            ) values (
                'running', :enrichment, 'private', 'people', :filter, :config,
                :next_cursor, :row_count, 0, 0, :actor_id
            )
            """,
            {
                "enrichment": enrichment,
                "filter": SENTINEL_QUERY[1:],
                "config": json.dumps({"prompt": SENTINEL_CONFIG}),
                "next_cursor": next_cursor,
                "row_count": PEOPLE_ROWS,
                "actor_id": SENTINEL_ACTOR,
            },
        )
    ).lastrowid


def run_of(job_id, trigger=None):
    "Span predicate: the run span of ``job_id``, optionally with ``trigger``."
    return lambda span: (
        for_job(job_id)(span)
        and (trigger is None or span.attributes[reg.TRIGGER] == trigger)
    )


@pytest.mark.asyncio
async def test_registry_conformance_and_privacy(
    private_datasette, otel_spans, otel_metrics
):
    datasette = private_datasette

    async def submit(slug, data=None):
        return await enqueue(
            datasette,
            "people",
            slug,
            data,
            query=SENTINEL_QUERY,
            database="private",
            actor_id=SENTINEL_ACTOR,
        )

    async def press(job_id, action):
        await job_action(
            datasette, job_id, action, database="private", actor_id=SENTINEL_ACTOR
        )

    # 1. Restart: a running job with a known slug, resuming from a sentinel
    # cursor, and one with an unknown slug. Inserted BEFORE any
    # datasette.client request: the first request launches the background
    # tasks, including the restart scan, which would otherwise find nothing.
    restarted = await _insert_private_running_job(
        datasette, "countbatches", person_id(PEOPLE_ROWS - 6)
    )
    await _insert_private_running_job(datasette, "no-such-enrichment", None)
    await datasette.start_background_tasks()
    restart = await wait_for_span(otel_spans, reg.RESTART)
    assert restart.attributes[reg.RESTART_JOBS] == 1
    assert restart.attributes[reg.RESTART_UNKNOWN] == 1
    run = await wait_for_span(otel_spans, reg.JOB_RUN, run_of(restarted, "restart"))
    assert run.attributes[reg.RUN_ROWS] == 5

    # 2. Batches that succeed, with the config, filter, rows, primary keys,
    # actor and secret all carrying sentinels
    secret_job = await submit(
        "secretreplace", {"column": "s", "string": SENTINEL_CONFIG}
    )
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(secret_job))
    # The secret really was used
    (row,) = datasette._test_db.execute(
        "select s from people where id = ?", (person_id(0),)
    ).fetchall()
    assert row == (f"{SENTINEL_ROW} 0 {SENTINEL_SECRET}",)
    # The config and actor reached the job row through the form POST, not
    # just through the directly inserted restart fixture
    config, actor_id = datasette._test_db.execute(
        "select config, actor_id from _enrichment_jobs where id = ?", (secret_job,)
    ).fetchone()
    assert SENTINEL_CONFIG in config
    assert actor_id == SENTINEL_ACTOR

    # 3. A batch that raises, then a Pause exception; resumed through the UI,
    # then a Cancel exception - every message a sentinel
    queue_job = await submit("queue")
    queue = datasette.enrichment_queue
    await queue.put("0")
    await queue.put(f"raise:{SENTINEL_EXC}")
    await queue.put(f"pause:{SENTINEL_PAUSE}")
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(queue_job, "enqueue"))
    await press(queue_job, "resume")
    await queue.put(f"cancel:{SENTINEL_CANCEL}")
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(queue_job, "resume"))

    # 4. Partial row errors, logged by the enrichment itself
    errors_job = await submit("haserrors")
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(errors_job))

    # 5. Pause from the UI mid-batch, then Resume
    datasette._enrich_gate = asyncio.Event()
    paused_job = await submit("countbatches")
    await wait_until(lambda: first_batch_in_flight(datasette), "first batch")
    await press(paused_job, "pause")
    datasette._enrich_gate.set()
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(paused_job, "enqueue"))
    await press(paused_job, "resume")
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(paused_job, "resume"))

    # 6. finalize() raises
    finalize_job = await submit("finalizeraises")
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(finalize_job))

    # 7. Cost
    cost_job = await submit("costdemo")
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(cost_job))

    # 8. A run interrupted mid-batch, as at shutdown
    datasette._enrich_gate = asyncio.Event()  # never set
    interrupted_job = await submit("countbatches")
    await wait_until(lambda: first_batch_in_flight(datasette), "first batch")
    task = datasette._enrichment_job_tasks[("private", interrupted_job)]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await wait_for_span(otel_spans, reg.JOB_RUN, run_of(interrupted_job))

    # Every job loop has exited, so no span is still open - and so unchecked -
    # when the spans are gathered
    await wait_until(
        lambda: all(t.done() for t in datasette._enrichment_job_tasks.values()),
        "every job task to finish",
    )
    finished = otel_spans.get_finished_spans()
    otel_metrics.collect()

    # Every sentinel really was planted: the database holds each one (the job
    # rows, progress messages, error log and enriched rows), so the privacy
    # walk below cannot pass vacuously
    stored = "\n".join(datasette._test_db.iterdump())
    assert {sentinel for sentinel in FORBIDDEN if sentinel not in stored} == set()

    # The workload reached every outcome it claims to
    runs = ours(otel_spans, reg.JOB_RUN)
    assert {s.attributes[reg.RUN_OUTCOME] for s in runs} == {
        "finished",
        "paused",
        "cancelled",
        "interrupted",
        "error",
    }
    assert {s.attributes[reg.TRIGGER] for s in runs} == {"enqueue", "resume", "restart"}
    assert {
        s.attributes[reg.BATCH_OUTCOME]
        for s in ours(otel_spans, reg.BATCH)
        if reg.BATCH_OUTCOME in s.attributes
    } == {"ok", "error", "paused", "cancelled"}
    # haserrors logs 2 errors in each full batch of 10: 25 rows -> 4 errors
    assert (
        otel_metrics.point(
            reg.ROWS, {reg.ENRICHMENT: "haserrors", reg.ROW_RESULT: "error"}
        ).value
        == 4
    )

    assert_spans_conform(reg.SPANS, finished, scope_name=SCOPE)
    assert_spans_covered(reg.SPANS, finished, scope_name=SCOPE)
    assert_metrics_conform(reg.METRICS, otel_metrics, scope_name=SCOPE)
    assert_metrics_covered(reg.METRICS, otel_metrics, scope_name=SCOPE)
    # No scope_name: core's request, db.query and write spans are checked too
    assert_no_forbidden_values(
        FORBIDDEN, finished_spans=finished, collector=otel_metrics
    )
