from datasette import Response, NotFound, Forbidden
from datasette.utils import (
    async_call_with_supported_arguments,
    path_with_removed_args,
    MultiParams,
    tilde_decode,
)
import json
from .utils import get_with_auth
import urllib.parse


async def check_permissions(datasette, request, database):
    if not await datasette.permission_allowed(
        request.actor, "enrichments", resource=database, default=False
    ):
        raise Forbidden("Permission denied for enrichments")


async def job_view(datasette, request):
    "Page showing details of an enrichment job"
    from . import (
        get_enrichments,
        ensure_tables,
        CUSTOM_ELEMENT_JS,
        ms_since_2025_to_datetime,
    )

    job_id = request.url_vars["job_id"]
    database = request.url_vars["database"]
    await check_permissions(datasette, request, database)

    db = datasette.get_database(database)
    await ensure_tables(db)

    job = (
        await db.execute(
            "select * from _enrichment_jobs where id = ? and database_name = ?",
            (job_id, database),
        )
    ).first()
    if not job:
        raise NotFound("Job not found")

    enrichments = await get_enrichments(datasette)
    enrichment = enrichments.get(
        job["enrichment"]
    )  # May be None if plugin not installed

    messages = [
        dict(row)
        for row in (
            await db.execute(
                """
            select timestamp_ms_2025, message
            from _enrichment_progress
            where job_id = ?
            and message is not null
            order by id
            """,
                (job_id,),
            )
        ).rows
    ]
    for message in messages:
        message["timestamp"] = ms_since_2025_to_datetime(message["timestamp_ms_2025"])

    job = dict(job)
    config = json.loads(job["config"])
    return Response.html(
        await datasette.render_template(
            "enrichment_job.html",
            {
                "database": database,
                "job": job,
                "config": config,
                "enrichment": enrichment,
                "custom_element": CUSTOM_ELEMENT_JS,
                "messages": messages,
            },
            request,
        )
    )


async def list_jobs_view(datasette, request):
    database = request.url_vars["database"]
    await check_permissions(datasette, request, database)
    db = datasette.get_database(database)
    where = ["database_name = :database_name"]
    params = {"database_name": database}
    table = request.args.get("table")
    if table:
        where.append("table_name = :table_name")
        params["table_name"] = table
    sql = "select * from _enrichment_jobs where {where} order by id desc".format(
        where=" and ".join(where)
    )
    if await db.table_exists("_enrichment_jobs"):
        jobs = [dict(row) for row in (await db.execute(sql, params)).rows]
    else:
        jobs = []
    return Response.html(
        await datasette.render_template(
            "enrichment_jobs.html",
            {
                "database": database,
                "table": table,
                "jobs": jobs,
            },
            request,
        )
    )


async def enrichment_view(datasette, request):
    from . import get_enrichments

    database = request.url_vars["database"]
    table = tilde_decode(request.url_vars["table"])
    slug = request.url_vars["enrichment"]

    await check_permissions(datasette, request, database)

    enrichments = await get_enrichments(datasette)
    enrichment = enrichments.get(slug)
    if enrichment is None:
        raise NotFound("Enrichment not found")

    query_string = request.query_string
    # Parse query string
    bits = urllib.parse.parse_qsl(query_string)
    # Remove _sort
    bits = [bit for bit in bits if bit[0] != "_sort"]
    # Add extras
    bits.extend(
        [
            ("_extra", "human_description_en"),
            ("_extra", "count"),
            ("_extra", "columns"),
            # This one makes it work for Datasette < 1.0:
            ("_shape", "objects"),
        ]
    )
    # re-encode
    query_string = urllib.parse.urlencode(bits)
    filtered_data = (
        await get_with_auth(
            datasette,
            datasette.urls.table(database, table, "json") + "?" + query_string,
        )
    ).json()
    if "count" not in filtered_data:
        # Fix for Datasette < 1.0
        filtered_data["count"] = filtered_data["filtered_table_rows_count"]

    # If an enrichment is selected, use that UI

    if request.method == "POST":
        return await enrich_data_post(datasette, request, enrichment, filtered_data)

    form_class = await async_call_with_supported_arguments(
        enrichment._get_config_form,
        datasette=datasette,
        db=datasette.get_database(database),
        table=table,
    )
    if form_class:
        form = form_class()
    else:
        form = None

    return Response.html(
        await datasette.render_template(
            ["enrichment-{}.html".format(enrichment.slug), "enrichment.html"],
            {
                "database": database,
                "table": table,
                "filtered_data": filtered_data,
                "enrichment": enrichment,
                "enrichment_form": form,
            },
            request,
        )
    )


async def enrichment_picker(datasette, request):
    from . import get_enrichments

    database = request.url_vars["database"]
    table = tilde_decode(request.url_vars["table"])

    await check_permissions(datasette, request, database)

    enrichments = await get_enrichments(datasette)

    query_string = request.query_string
    # Parse query string
    bits = urllib.parse.parse_qsl(query_string)
    # Remove _sort
    bits = [bit for bit in bits if bit[0] != "_sort"]
    # Add extras
    bits.extend(
        [("_extra", "human_description_en"), ("_extra", "count"), ("_extra", "columns")]
    )
    # re-encode
    query_string = urllib.parse.urlencode(bits)
    url = datasette.urls.table(database, table, "json") + "?" + query_string
    response = await get_with_auth(datasette, url)
    if response.status_code != 200:
        return Response.text(
            "Error fetching data from {}: {}".format(url, response.text),
            status=500,
        )
    filtered_data = response.json()
    if "count" not in filtered_data:
        # Fix for Datasette < 1.0
        filtered_data["count"] = filtered_data["filtered_table_rows_count"]

    enrichments_and_paths = []
    for enrichment in enrichments.values():
        enrichments_and_paths.append(
            {
                "enrichment": enrichment,
                "path": path_with_removed_args(
                    request=request,
                    args={"_sort"},
                    path="{}/{}".format(request.path, enrichment.slug),
                ),
            }
        )

    return Response.html(
        await datasette.render_template(
            "enrichment_picker.html",
            {
                "database": database,
                "table": table,
                "filtered_data": filtered_data,
                "enrichments_and_paths": enrichments_and_paths,
            },
            request,
        )
    )


COLUMN_PREFIX = "column."


async def enrich_data_post(datasette, request, enrichment, filtered_data):
    database = request.url_vars["database"]
    await check_permissions(datasette, request, database)

    db = datasette.get_database(database)
    table = tilde_decode(request.url_vars["table"])

    # Enqueue the enrichment to be run
    filters = []
    for key in request.args:
        if key not in ("_enrichment", "_sort", "_enrichment_job"):
            for value in request.args.getlist(key):
                filters.append((key, value))
    filter_querystring = urllib.parse.urlencode(filters)

    # Roll our own form parsing because .post_vars() eliminates duplicate names
    body = await request.post_body()
    post_vars = MultiParams(urllib.parse.parse_qs(body.decode("utf-8")))

    form_class = await async_call_with_supported_arguments(
        enrichment._get_config_form, datasette=datasette, db=db, table=table
    )
    if form_class:
        form = form_class(post_vars)
    else:
        form = None

    if form and not form.validate():
        return Response.html(
            await datasette.render_template(
                ["enrichment-{}.html".format(enrichment.slug), "enrichment.html"],
                {
                    "database": database,
                    "table": table,
                    "filtered_data": filtered_data,
                    "enrichment": enrichment,
                    "enrichment_form": form,
                },
                request,
            )
        )

    config = {}
    if form:
        config = {field.name: field.data for field in form}

    # Call initialize method, which can create tables etc
    await async_call_with_supported_arguments(
        enrichment.initialize, datasette=datasette, db=db, table=table, config=config
    )

    job_id = await enrichment.enqueue(
        datasette,
        db,
        table,
        filter_querystring,
        config,
        request.actor.get("id") if request.actor else None,
    )

    # Set message and redirect to table
    datasette.add_message(
        request,
        "Enrichment started: {} for {} row{}".format(
            enrichment.name,
            filtered_data["count"],
            "s" if filtered_data["count"] != 1 else "",
        ),
        datasette.INFO,
    )
    return Response.redirect(
        datasette.urls.table(db.name, table) + "?_enrichment_job={}".format(job_id)
    )


async def job_progress_view(datasette, request):
    from . import get_enrichments, ensure_tables

    job_id = request.url_vars["job_id"]
    database = request.url_vars["database"]
    db = datasette.get_database(database)
    await ensure_tables(db)
    job = dict(
        (
            await db.execute("select * from _enrichment_jobs where id = ?", (job_id,))
        ).first()
    )
    if not job:
        raise NotFound("Job not found")
    enrichments = await get_enrichments(datasette)
    enrichment = enrichments.get(job["enrichment"])
    title = "Job {}: {}".format(
        job_id, enrichment.name if enrichment else job["enrichment"]
    )
    # Build sections
    progress_chunks = (
        await db.execute(
            """
        select success_count, error_count
        from _enrichment_progress
        where job_id = ? order by id
    """,
            (job_id,),
        )
    ).rows
    sections = []
    current = None
    current_total = 0
    for row in progress_chunks:
        section_type = "success" if row["success_count"] else "error"
        if section_type != current and current_total:
            sections.append({"type": current, "count": current_total})
            current_total = 0
        current = section_type
        current_total += row["success_count"] + row["error_count"]
    if current_total:
        sections.append({"type": current, "count": current_total})

    is_complete = (job["status"] in ("cancelled", "finished")) or (
        job["done_count"] >= job["row_count"]
    )

    return Response.json(
        {
            "total": job["row_count"],
            "title": title,
            "url": datasette.urls.path(
                "/-/enrich/{}/-/jobs/{}".format(database, job_id)
            ),
            "is_complete": is_complete,
            "sections": sections,
        }
    )


async def pause_job(db, job_id, message):
    from . import set_job_status

    await set_job_status(
        db, job_id, "paused", allowed_statuses=("running",), message=message
    )


async def resume_job(datasette, db, job_id, message):
    from . import set_job_status, get_enrichments

    await set_job_status(
        db, job_id, "running", allowed_statuses=("paused",), message=message
    )
    all_enrichments = await get_enrichments(datasette)
    job = dict(
        (
            await db.execute("select * from _enrichment_jobs where id = ?", (job_id,))
        ).first()
    )
    enrichment = all_enrichments[job["enrichment"]]
    await enrichment.start_enrichment_in_process(datasette, db, job_id)


async def cancel_job(db, job_id, message):
    from . import set_job_status

    await set_job_status(
        db,
        job_id,
        "cancelled",
        allowed_statuses=("running", "paused", "pending"),
        message=message,
    )


async def update_job_status_view(datasette, request, action):
    db = datasette.get_database(request.url_vars["database"])
    message = ""
    if request.actor and request.actor.get("id"):
        message = "by {}".format(request.actor.get("id"))
    job_id = int(request.url_vars["job_id"])
    if request.method != "POST":
        return Response("POST required", status=400)
    try:
        if action == "pause":
            await pause_job(db, job_id, message)
        elif action == "resume":
            await resume_job(datasette, db, job_id, message)
        elif action == "cancel":
            await cancel_job(db, job_id, message)
    except ValueError as ve:
        return Response(str(ve), status=400)
    return Response.redirect(
        datasette.urls.path(
            "/-/enrich/{}/-/jobs/{}".format(request.url_vars["database"], job_id)
        )
    )


async def pause_job_view(datasette, request):
    return await update_job_status_view(datasette, request, "pause")


async def resume_job_view(datasette, request):
    return await update_job_status_view(datasette, request, "resume")


async def cancel_job_view(datasette, request):
    return await update_job_status_view(datasette, request, "cancel")
