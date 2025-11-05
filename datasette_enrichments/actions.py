from datasette.permissions import Action
from datasette.resources import DatabaseResource

ENRICHMENTS_ACTION = Action(
    name="enrichments",
    abbr=None,
    description="Ability to perform enrichments on a database",
    resource_class=DatabaseResource,
)
