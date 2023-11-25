(usage)=
# Running an enrichment

Enrichments are run against data in a Datasette table.

They can be executed against every row in a table, or you can filter that table first and then run the enrichment against the filtered results.

Start on the table page and filter it to just the set of rows that you want to enrich. Then click the cog icon near the table heading and select "Enrich selected data".

This will present a list of available enrichments, provided by plugins that have been installed.

Select the enrichment you want to run to see a form with options to configure for that enrichment.

Enter your settings and click "Enrich data" to start the enrichment running.

Enrichments can take between several seconds and several minutes to run, depending on the number of rows and the complexity of the enrichment.

