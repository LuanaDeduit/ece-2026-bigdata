# dbt lab — Answers

## Bronze sources

### Why keep the bronze files unchanged?
They preserve the original data. If a transformation bug is discovered later, we can fix the SQL and rebuild the silver layer from the same source.

### Which format and layout would suit hourly ingestion over several years?
Use Parquet, partitioned by date, with appropriately sized files. Queries can read only the required columns and partitions.

## Views and data types

### What happens when a view is queried? When is it preferable to a table?
A view stores SQL. Its transformation runs when queried, using the current source data. It is useful when fresh results and low storage usage matter more than repeated computation.

### How much space does a UUID use?
The UUID type uses 16 bytes. Its standard text representation contains 36 ASCII characters, requiring 36 bytes for the text, plus storage overhead depending on the system.

## Data quality

### How should invalid birthdates be handled?
For a customer-age dashboard, replace invalid birthdates with NULL and exclude them from age calculations. Report how many dates are invalid.

For sales by region, keep the orders: an invalid birthdate does not invalidate a sale. Validate the location separately.

Our model replaces invalid birthdates with NULL and records the original date's validity in birthdate_is_valid.

### How can inconsistent state/ZIP combinations be detected?
Compare each combination with an external ZIP-to-state reference.

We loaded GeoNames data as a dbt seed and added a warning test. It identifies mismatches for ZIP codes present in the reference. Unknown ZIP codes remain unverified; the reference is not a complete official USPS validation service.

A separate USPS-based states seed validates state, territory and military-region codes.

## Reference data

### Where should reference data come from?
Use a reliable, documented source. USPS is an authoritative source for US postal information. GeoNames provides an accessible reference suitable for this lab, with coverage limitations.

### Should reference data be a dbt seed?
Yes, for a small, relatively stable reference. Larger or frequently updated datasets are better managed through a dedicated ingestion process.

### Should validation happen in the silver layer?
Yes. The silver layer checks and cleans data while preserving the original bronze files.

### What happens when the reference changes?
Update and version the reference, reload the seed, and rerun the affected models and tests. Previously valid or invalid records may receive different results.

## Materialization experiment

| Question | View | Table |
| --- | --- | --- |
| Where is it stored? | SQL definition in DuckDB | Transformed data in DuckDB |
| When does transformation run? | At query time | During dbt run/build |
| What if the source CSV changes? | Next query uses updated source data | Requires a rebuild |
| Storage requirement | Low for the view itself | Higher because data is stored |
| Repeated source reads | Yes | Avoided when querying the table |

We built the models as tables and verified BASE TABLE in information_schema.tables. We then restored the view configuration, rebuilt the models and verified VIEW.

## Validation results

The first successful build reported PASS=20, WARN=1 and ERROR=0.
The warning identified 20 users with state/ZIP combinations inconsistent with the GeoNames reference.

Both silver models were exported to S3 as Parquet. Bidirectional EXCEPT ALL comparisons returned zero differences between each view and its export.
