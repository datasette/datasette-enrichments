import secrets


async def get_with_auth(datasette, *args, **kwargs):
    if not hasattr(datasette, "_secret_enrichments_token"):
        datasette._secret_enrichments_token = secrets.token_hex(16)
    headers = kwargs.pop("headers", None) or {}
    headers["x-datasette-enrichments"] = datasette._secret_enrichments_token
    kwargs["headers"] = headers
    return await datasette.client.get(*args, **kwargs)
