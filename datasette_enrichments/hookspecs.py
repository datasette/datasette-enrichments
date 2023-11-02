from pluggy import HookspecMarker

hookspec = HookspecMarker("datasette")


@hookspec
def register_enrichments(datasette):
    "Return a list of Enrichment instances, or an awaitable function returning that list"
