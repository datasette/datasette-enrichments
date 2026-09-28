import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette.telemetry_testing import assert_package_never_imports_sdk


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
