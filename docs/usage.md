(usage)=
# Running an enrichment

Enrichments are run against data in a Datasette table.

They can be executed against every row in a table, or you can filter that table first and then run the enrichment against the filtered results.

Start on the table page and filter it to just the set of rows that you want to enrich. Then click the cog icon near the table heading and select "Enrich selected data".

This will present a list of available enrichments, provided by plugins that have been installed.

Select the enrichment you want to run to see a form with options to configure for that enrichment.

Enter your settings and click "Enrich data" to start the enrichment running.

Enrichments can take between several seconds and several minutes to run, depending on the number of rows and the complexity of the enrichment.

(usage-jobs)=
## Managing enrichment jobs

Each enrichment run triggers a **job** that is managed by Datasette.

Jobs are tracked in the `_enrichment_jobs` table in the same database as the table that is being enriched.

You can view all of the jobs that are running or have run against a particular database using the database action menu's Enrichment jobs item. The table action menu has a similar item for viewing jobs that have run against that table.

Each job gets its own page. This page shows how many rows have been processed and if there have been any errors.

The job page also includes buttons for pausing, resuming and cancelling an enrichment job.

When the Datasette server restarts any running jobs will be automatically continued from where they left off.

