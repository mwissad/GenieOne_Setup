# Troubleshooting

## `unknown field: genie_spaces` or CLI too old

Upgrade the Databricks CLI to **v1.3.0 or newer**. Genie Space bundle
resources require the direct bundle engine.

```bash
# macOS
brew tap databricks/tap
brew upgrade databricks/tap/databricks
databricks version
```

## Authentication fails after a CLI upgrade

A v1.x CLI can reject a token cached by v0.x. Re-sign in:

```bash
databricks auth login https://YOUR-WORKSPACE-HOST --profile YOUR_PROFILE
```

If you need the existing session for one retry:

```bash
DATABRICKS_AUTH_STORAGE=plaintext ./install.sh \
  --profile YOUR_PROFILE \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --catalog YOUR_CATALOG
```

Then re-login so you are not depending on plaintext token storage.

## Genie space deploy fails because tables are missing

The installer now deploys and runs the data job before creating Genie spaces.
Rerun `./install.sh` with the same arguments. Do not deploy the spaces by
hand until `YOUR_CATALOG.YOUR_SCHEMA` contains the 16 demo tables.

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
