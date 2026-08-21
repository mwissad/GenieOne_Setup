# Troubleshooting

## `unknown field: genie_spaces`

Upgrade the Databricks CLI. Genie Space bundle resources require a CLI version
that supports the direct bundle engine.

## Authentication fails

Authenticate the target profile, then rerun the installer:

```bash
databricks auth login https://YOUR-WORKSPACE-HOST --profile YOUR_PROFILE
```

## Catalog or warehouse check fails

Confirm the names and permissions:

```bash
databricks catalogs get YOUR_CATALOG --profile YOUR_PROFILE
databricks warehouses get YOUR_WAREHOUSE_ID --profile YOUR_PROFILE
```

## Group not found

The default consumer group is `users`. To use a dedicated group:

```bash
./install.sh ... --can-run-group al-ghurair-procurement-team
```

The group must already exist in the target workspace.

## Invalid catalog or schema name

For safe SQL and Genie rendering, the installer accepts identifiers containing
letters, numbers, and underscores, with a letter or underscore first.

## Partial deployment

Fix the reported permission or configuration error and rerun the same install
command. Bundle resources are updated in place and the data tables are
recreated deterministically.
