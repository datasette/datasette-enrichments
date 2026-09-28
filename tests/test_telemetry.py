import asyncio
import inspect
import subprocess
import sys
import textwrap
import time

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette import telemetry_registry as core
from datasette.telemetry_testing import assert_package_never_imports_sdk
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


def root_cookies(datasette):
    return {"ds_actor": datasette.sign({"a": {"id": "root"}}, "actor")}


async def enqueue(datasette, table, slug, data=None, query=""):
    "Submit a job through the enrichment form; returns the new job's id."
    response = await datasette.client.post(
        f"/-/enrich/data/{table}/{slug}{query}",
        cookies=root_cookies(datasette),
        data=data or {},
    )
    assert response.status_code == 302
    return int(response.headers["location"].split("=")[-1])


async def job_action(datasette, job_id, action):
    response = await datasette.client.post(
        f"/-/enrich/data/-/jobs/{job_id}/{action}",
        cookies=root_cookies(datasette),
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
