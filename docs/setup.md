# Installation and setup

To install the Datasette Enrichments plugin, run this:
```bash
datasette install datasette-enrichments
```
You need to install additional plugins for enrichments that you want to use before this plugin becomes useful. Try [datasette-enrichments-jinja](https://github.com/datasette/datasette-enrichments-jinja) for example:

```bash
datasette install datasette-enrichments-jinja
```
Users  with the `enrichments` permission (or the `--root` user) will then be able to select rows for enrichment using the cog actions menu on the table page.

Once you have installed an enrichment you can {ref}`run it against some data<usage>`.