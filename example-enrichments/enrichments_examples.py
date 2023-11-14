from datasette import hookimpl
from uppercase import Uppercase
from openai_embeddings import Embeddings


@hookimpl
def register_enrichments():
    return [Uppercase(), Embeddings()]
