# Configuring permissions

Enrichments are only available to users with the `enrichments` permission.

By default the `root` user has this permission. You can execute Datasette like this to sign in with that user:

```bash
datasette mydb.db --root
```
Then click the link provided to sign in as root.

To use enrichments in a deployed instance of Datasette, you can use an authentication plugin such as [datasette-auth-github](https://datasette.io/plugins/datasette-auth-github) or [datasette-auth-passwords](https://datasette.io/plugins/datasette-auth-passwords) to authenticate users, then grant the `enrichments` permission to those users using [permissions in datasette.yaml](https://docs.datasette.io/en/latest/authentication.html#access-permissions-in-datasette-yaml), or one of the permissions plugins.
