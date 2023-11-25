# Datasette Enrichments

Datasette Enrichments is a plugin for [Datasette](https://datasette.io/) that adds support for enriching data in different ways.

An **enrichment** is a bulk operation that can by applied to a set of rows from a table, executing code for each of those rows.

Potential use-cases for enrichments include:

- Geocoding an address and populating a latitude and longitude column
- Executing a template to generate output based on the values in each row
- Fetching data from a URL and populating a column with the result
- Executing OCR against a linked image or PDF file

Each enrichment is implemented as its own plugin.

The Datasette ecosystem includes a growing number of enrichment plugins. They are also intended to be easy to write.

## Table of contents

```{toctree}
---
maxdepth: 3
---
setup
usage
permissions
developing
```