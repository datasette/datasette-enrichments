from datasette import Response, NotFound, Forbidden
from datasette.utils import (
    async_call_with_supported_arguments,
    path_with_removed_args,
    MultiParams,
    tilde_decode,
    tilde_encode,
)
from .utils import get_with_auth
import urllib.parse


async def check_permissions(datasette, request, database):
    if not await datasette.permission_allowed(
        request.actor, "enrichments", resource=database, default=False
    ):
        raise Forbidden("Permission denied for enrichments")


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

    form = (
        await async_call_with_supported_arguments(
            enrichment._get_config_form,
            datasette=datasette,
            db=datasette.get_database(database),
            table=table,
        )
    )()

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
        if key not in ("_enrichment", "_sort"):
            for value in request.args.getlist(key):
                filters.append((key, value))
    filter_querystring = urllib.parse.urlencode(filters)

    # Roll our own form parsing because .post_vars() eliminates duplicate names
    body = await request.post_body()
    post_vars = MultiParams(urllib.parse.parse_qs(body.decode("utf-8")))

    Form = await async_call_with_supported_arguments(
        enrichment._get_config_form, datasette=datasette, db=db, table=table
    )

    form = Form(post_vars)

    if not form.validate():
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
