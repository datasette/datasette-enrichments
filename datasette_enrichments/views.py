from datasette import Response
from datasette.utils import path_with_added_args
import urllib.parse


enrichments = [
    # {
    #     "slug": "openai-embeddings",
    #     "name": "OpenAI Embeddings",
    #     "description": "Calculate embeddings for text columns in a table. Embeddings are numerical representations which can then be used to power semantic search and find related content.",
    # },
    # {
    #     "slug": "youtube-whisperx",
    #     "name": "Transcribe YouTube with WhisperX",
    #     "description": "Use WhisperX to generate a transcript for a YouTube video.",
    # },
    # {
    #     "slug": "opencage-geocode",
    #     "name": "Geocode with OpenCage",
    #     "description": "Use the OpenCage geocoder to turn addresses into latitude longitude pairs.",
    # },
    # {
    #     "slug": "textract-ocr",
    #     "name": "OCR with AWS Textract",
    #     "description": "Use AWS Textract to extract text from linked images and PDFs.",
    # },
    # {
    #     "slug": "comprehend-entities",
    #     "name": "Extract entities with AWS Comprehend",
    #     "description": "Use AWS Comprehend to extract entities - people, businesses, locations and more - from text.",
    # },
    # {
    #     "slug": "openai-gpt3",
    #     "name": "OpenAI GPT-3",
    #     "description": "Evaluate prompts using a GPT-3 language model and store the results.",
    # },
    {
        "slug": "uppercase",
        "name": "Convert to uppercase",
        "description": "Convert selected columns to uppercase",
    },
]


async def enrich_data(datasette, request):
    database = request.url_vars["database"]
    table = request.url_vars["table"]

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
    stuff = (
        await datasette.client.get(
            datasette.urls.table(database, table, "json") + "?" + query_string
        )
    ).json()

    # If an enrichment is selected, use that UI
    enrichment_slug = request.args.get("_enrichment")
    enrichment = None
    if enrichment_slug:
        for e in enrichments:
            if e["slug"] == enrichment_slug:
                enrichment = e
                break

    if request.method == "POST":
        return await enrich_data_post(datasette, request, enrichment, stuff)

    return Response.html(
        await datasette.render_template(
            "enrich_data.html",
            {
                "database": database,
                "table": table,
                "stuff": stuff,
                "enrichments": [
                    dict(
                        enrichment,
                        path=path_with_added_args(
                            request, {"_enrichment": enrichment["slug"]}
                        ),
                    )
                    for enrichment in enrichments
                ],
                "enrichment": enrichment,
                "columns": stuff["columns"],
            },
            request,
        )
    )


COLUMN_PREFIX = "column."


async def enrich_data_post(datasette, request, enrichment, stuff):
    db = datasette.get_database(request.url_vars["database"])
    table = request.url_vars["table"]
    from . import Uppercase

    enrichment = Uppercase()
    # Initialize any necessary tables
    await enrichment.initialize(db, table, {})

    # Enqueue the enrichment to be run
    filters = []
    for key in request.args:
        if key not in ("_enrichment", "_sort"):
            for value in request.args.getlist(key):
                filters.append((key, value))
    filter_querystring = urllib.parse.urlencode(filters)

    post_vars = await request.post_vars()

    columns = [
        key[len(COLUMN_PREFIX) :] for key in post_vars if key.startswith(COLUMN_PREFIX)
    ]

    await enrichment.enqueue(
        datasette, db, table, filter_querystring, {"columns": columns}
    )

    # Set message and redirect to table
    datasette.add_message(
        request,
        "Enrichment started: {} for {} row{}".format(
            enrichment.name, stuff["count"], "s" if stuff["count"] != 1 else ""
        ),
        datasette.INFO,
    )
    return Response.redirect(datasette.urls.table(db.name, table))
