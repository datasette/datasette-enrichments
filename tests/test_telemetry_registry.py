"""Static checks on the telemetry registry. These need no OpenTelemetry SDK.

The dynamic half - one broad workload checked against the registry in both
directions, plus the sentinel privacy walk - is
``test_registry_conformance_and_privacy`` in ``test_telemetry.py``.
"""

from itertools import pairwise

from datasette.telemetry_registry import DB_NAMESPACE, ERROR_TYPE, HISTOGRAM

from datasette_enrichments.telemetry_registry import (
    ATTRIBUTES,
    ENRICHMENT,
    JOB_ID,
    METRICS,
    SPANS,
)

PREFIX = "enrichments."
# Names shared with Datasette core rather than prefixed, so backends line
# them up with core's own spans
CORE_SHARED = {str(DB_NAMESPACE), str(ERROR_TYPE)}

# --- Wire names pinned as literals ----------------------------------------
#
# Renaming a signal breaks every dashboard and alert built on it, so a
# rename has to be a deliberate edit here - together with the docs
# reference, which cog regenerates from the registry.

EXPECTED_SPANS = {
    "enrichments.job.run",
    "enrichments.batch",
    "enrichments.initialize",
    "enrichments.finalize",
    "enrichments.restart",
}

EXPECTED_METRICS = {
    "enrichments.job.runs",
    "enrichments.job.runs.active",
    "enrichments.job.run.duration",
    "enrichments.batch.duration",
    "enrichments.rows",
    "enrichments.cost",
}

EXPECTED_ATTRIBUTES = {
    "enrichments.enrichment",
    "enrichments.job_id",
    "enrichments.trigger",
    "enrichments.run.outcome",
    "enrichments.batch.outcome",
    "enrichments.row.result",
    "enrichments.job.row_count",
    "enrichments.run.batches",
    "enrichments.run.rows",
    "enrichments.batch.index",
    "enrichments.batch.rows",
    "enrichments.batch.success_count",
    "enrichments.restart.jobs",
    "enrichments.restart.unknown",
    "db.namespace",
    "error.type",
}


def test_wire_names_are_pinned():
    assert {str(span) for span in SPANS} == EXPECTED_SPANS
    assert {str(metric) for metric in METRICS} == EXPECTED_METRICS
    assert {str(attribute) for attribute in ATTRIBUTES} == EXPECTED_ATTRIBUTES


def test_every_name_is_prefixed():
    # datasette.* is reserved for core; everything of ours lives under
    # enrichments.*, apart from the two names deliberately shared with core
    for entry in (*SPANS, *METRICS):
        assert str(entry).startswith(PREFIX), entry
    for attribute in ATTRIBUTES:
        assert str(attribute).startswith(PREFIX) or str(attribute) in CORE_SHARED, (
            attribute
        )
    for entry in (*SPANS, *METRICS, *ATTRIBUTES):
        assert not str(entry).startswith("datasette."), entry


def test_span_and_metric_attributes_are_registered():
    registered = {str(attribute) for attribute in ATTRIBUTES}
    for entry in (*SPANS, *METRICS):
        for attribute in entry.attributes:
            assert str(attribute) in registered, f"{entry}: unlisted {attribute}"


def test_metric_dimensions_are_bounded():
    # Every metric attribute is the enrichment slug (bounded by the installed
    # plugins) or a closed enum. Job ids and database names would mint a new
    # series per job or per database, so they ride on spans only.
    for metric in METRICS:
        for attribute in metric.attributes:
            assert str(attribute) == str(ENRICHMENT) or attribute.values, (
                f"{metric}: {attribute} is neither the enrichment nor an enum"
            )
            assert str(attribute) not in {str(JOB_ID), str(DB_NAMESPACE)}, (
                f"{metric}: {attribute} must never be a metric dimension"
            )


def test_every_histogram_declares_buckets():
    for metric in METRICS:
        if metric.kind == HISTOGRAM:
            assert metric.buckets, f"{metric} declares no buckets"
            assert all(a < b for a, b in pairwise(metric.buckets)), (
                f"{metric} buckets are not strictly increasing: {metric.buckets}"
            )
        else:
            assert metric.buckets is None, f"{metric} is not a histogram"


def test_every_entry_is_documented():
    for entry in (*SPANS, *METRICS, *ATTRIBUTES):
        assert entry.description and entry.description.strip(), (
            f"{entry} has no description"
        )


def test_no_duplicate_names():
    for group in (SPANS, METRICS, ATTRIBUTES):
        names = [str(entry) for entry in group]
        assert len(names) == len(set(names)), f"duplicates in {names}"
