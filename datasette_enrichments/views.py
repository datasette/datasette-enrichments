from datasette import Response, NotFound, Forbidden
from datasette.utils import path_with_added_args, MultiParams
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
    table = request.url_vars["table"]
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
        [("_extra", "human_description_en"), ("_extra", "count"), ("_extra", "columns")]
    )
    # re-encode
    query_string = urllib.parse.urlencode(bits)
    filtered_data = (
        await get_with_auth(
            datasette,
            datasette.urls.table(database, table, "json") + "?" + query_string,
        )
    ).json()

    # If an enrichment is selected, use that UI

    if request.method == "POST":
        return await enrich_data_post(datasette, request, enrichment, filtered_data)

    form = (await enrichment.get_config_form(datasette.get_database(database), table))()

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
    table = request.url_vars["table"]

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
    response = await get_with_auth(
        datasette,
        datasette.urls.table(database, table, "json") + "?" + query_string,
    )
    filtered_data = response.json()

    enrichments_and_paths = []
    for enrichment in enrichments.values():
        enrichments_and_paths.append(
            {
                "enrichment": enrichment,
                "path": path_with_added_args(
                    request=request,
                    args={"_enrichment": enrichment.slug, "_sort": None},
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
    table = request.url_vars["table"]

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

    Form = await enrichment.get_config_form(db, table)

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

    # Initialize any necessary tables
    await enrichment.initialize(datasette, db, table, config)

    await enrichment.enqueue(
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
    return Response.redirect(datasette.urls.table(db.name, table))
