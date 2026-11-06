# Lab: Govern Iceberg tables with Polaris

## 1. Connecting Spark to the catalogs

The Hadoop catalog (`lab`) discovers namespaces using the S3 warehouse.
The REST catalog (`polaris`) sends requests to Polaris.
Initially, Hadoop listed `silver`, while Polaris had no namespaces.

The Polaris client credential is configured in Spark and used to
authenticate with Polaris. Separate AWS credentials let Spark access S3.

## 2. Registering the existing orders table

I registered:
s3a://user-l-deduit-ece/warehouse/silver/orders/metadata/v6.metadata.json

Both catalogs returned 2,830 orders. Registration left all 25 objects
under warehouse/silver/orders/ unchanged, including their sizes and ETags.
It created a catalog reference in Polaris without copying the data.

The Hadoop and Polaris catalogs maintain independent metadata pointers.
After registration, writes must go through Polaris to avoid divergence.

## 3. Creating users through Polaris

I created polaris.silver.users from lab.silver.users.
Both tables contained 50 users.

Polaris reported this location:
s3://user-l-deduit-ece/warehouse/silver/users

This follows the catalog warehouse and namespace/table layout.
In this environment, it overlaps the directory of the earlier Hadoop
users table, but the two catalogs have independent metadata pointers.

Registration reused existing metadata and data files.
Creating users through Polaris wrote new data and Iceberg metadata.

## 4. Schema changes

As dev, I added country STRING to polaris.silver.users.
The change was recorded in a new numbered metadata JSON file in the
users metadata directory, not in the orders v6.metadata.json file.

The relevant schema-management privilege is TABLE_FULL_METADATA,
included in dev's broader CATALOG_MANAGE_METADATA grant.
The analyst rejection identified the operation as ADD_TABLE_SCHEMA.

## 5. Analyst permissions

The analyst successfully read orders grouped by product.
INSERT and ALTER TABLE were rejected with Forbidden responses.

catalog_reader grants:
- TABLE_READ_DATA
- TABLE_READ_PROPERTIES
- CATALOG_READ_PROPERTIES

It does not grant TABLE_WRITE_DATA, required for inserting data, or
the schema-management privileges needed for ALTER TABLE.

Namespace listing was also denied with LIST_NAMESPACES.
The script records listing restrictions separately from direct reads.

After the rejected INSERT, dev still observed:
- 2,830 orders;
- six snapshots;
- latest snapshot: 7139067422734504314.

The rejected ALTER TABLE did not add a phone column.

Polaris controls catalog access and commits. Spark uses separate S3
credentials here, so Polaris cannot prevent direct bucket access.
A rejected commit does not, by itself, prove that no temporary or
unreferenced storage files were written.

## Exercise 1: Remove country

As dev, I dropped country and verified:
- country was absent from the schema;
- snapshot IDs were unchanged;
- referenced data file paths were unchanged.

Dropping a column changes Iceberg schema metadata without rewriting
the Parquet files or creating a new data snapshot.

New numbered metadata JSON files were recorded. Repeated dev runs
added and removed country again, producing further metadata versions.
latest_schema_id in metadata_log_entries describes the latest
snapshot's schema; it does not necessarily represent the current
table schema after a metadata-only change.

## Exercise 2: Namespace-scoped access

I created a separate authorization chain:
silver_only -> silver_only_role -> silver_reader

The role has CATALOG_READ_PROPERTIES at catalog scope.
The following privileges are restricted to namespace silver:
- NAMESPACE_LIST
- NAMESPACE_READ_PROPERTIES
- TABLE_LIST
- TABLE_READ_PROPERTIES
- TABLE_READ_DATA

As dev, I created gold.access_check and successfully read its test row.
Using a fresh Spark process as silver_only:
- listing tables in silver succeeded;
- reading orders returned 2,830 rows;
- reading users returned 50 rows;
- reading gold.access_check failed with Forbidden for LOAD_TABLE.

This validates isolation through Polaris. The Hadoop catalog still
listed gold using the separate S3 credentials, illustrating the
difference between catalog authorization and storage authorization.

## Metadata and cleanup

I downloaded warehouse metadata files before cleanup.
New files included Polaris users metadata JSON versions, manifest
files, snapshot metadata, and metadata for gold.access_check.
The registered orders table retained its existing metadata.

Cleanup removed polaris.silver.users, polaris.gold.access_check and
the gold namespace. It preserved polaris.silver.orders with 2,830 rows
for the Trino lab. Spark sessions were stopped.

## Scripts

- lab_polaris.py: dev, analyst and silver_only checks.
- polaris/setup_silver_reader.py: restricted principal and role setup.
- lab_polaris_cleanup.py: final cleanup and orders validation.

Credentials are stored outside the Git repository.
Run the dev preparation before analyst and silver_only checks.
Run cleanup last: rerunning the dev script recreates the test tables.
