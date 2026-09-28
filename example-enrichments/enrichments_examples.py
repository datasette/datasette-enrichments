from datasette import hookimpl
from jinja_sandbox import JinjaSandbox
from openai_embeddings import Embeddings
from uppercase import Uppercase


@hookimpl
def register_enrichments():
    return [Uppercase(), Embeddings(), JinjaSandbox()]
