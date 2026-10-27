# Spark Silver lab — Answers and results

## Environment and execution

Spark 4.1.1, Scala 2.13.17, Hadoop AWS 3.4.2 and Iceberg 1.11.0.
The script uses the AWS default profile without embedding credentials.

Run from the repository root:

    spark-submit --master 'local[1]' --driver-memory 512m \
      --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
      lab_silver.py

The container initially restarted with OOMKilled (exit 137).
Its memory limit was increased to 4 GiB while keeping its memory
request at 400 MiB. The pipeline then completed successfully.

A service restart stops the local Spark job and destroys its
in-memory state. Credentials may also need to be configured again.
Expired temporary credentials cause subsequent S3 requests to fail.

Temporary credentials require an access key, a secret key and a
session token. SimpleAWSCredentialsProvider cannot authenticate
a temporary session without its token. The script selects the
temporary provider when the profile supplies a session token.

## Bronze and Silver transformations

Bronze contains 50 users and 2829 orders.
The CSV reader uses multiLine=True to preserve multiline addresses.

Users: names are trimmed, sex is uppercased, email is lowercased,
birthdate is cast to date, and addresses are split into fields.
Six users have no extracted city, represented as NULL.

Orders: identifiers are strings, dates become timestamps,
quantities become integers, and products are trimmed and lowercased.

Three duplicate orders were injected:
2829 original rows -> 2832 rows -> 2829 deduplicated rows.

A window partitions by order_id and keeps the latest ordered_at.
DataFrame and SQL results were checked with exceptAll in both
directions and matched exactly. Total quantity was 8615.

The state query counts users and missing cities per state.
For example, AE had 3 users with 3 missing cities, and AP had
2 users with 2 missing cities.

For exploration I would use the DataFrame API when working in
Python. For a shared model maintained by SQL analysts, I would
choose SQL. Both use Spark's query optimizer.

regexp_extract returns an empty string on a non-match.
State and ZIP extraction need similar handling if malformed
addresses are possible. Empty strings pass IS NOT NULL checks
and can produce misleading joins, counts and groups.
NULL itself also forms a group in GROUP BY; COUNT(column)
ignores NULL, while COUNT(*) includes its row.

## Parquet output

Paths:
- silver/spark/users.parquet/
- silver/spark/orders.parquet/

Each directory contained one part-file in the observed run,
plus a success marker. Part-file counts depend on output
partitions and execution planning, not on the folder suffix.

coalesce(1) reduces writing to one task. On large datasets this
loses parallelism and can create a bottleneck.

Read-back validation found 50 users and 2829 orders.
Bidirectional exceptAll checks confirmed unchanged content.

## Iceberg output

Tables: lab.silver.users and lab.silver.orders.
Warehouse: s3a://user-l-deduit-ece/warehouse.

The data directories contain Parquet files. Metadata JSON files
describe schemas and table versions. Avro manifests describe
data files, and manifest lists describe snapshots.
version-hint.text helps locate the current metadata version.

A plain Parquet overwrite is not an atomic table transaction:
a concurrent reader may encounter missing files or an incomplete
set of files. Iceberg publishes a committed snapshot so readers
can use a consistent set of data files.

This Hadoop catalog relies on filesystem commit semantics.
Object-store rename may not be atomic, so concurrent commits
can conflict or publish inconsistent state. A suitable catalog
service adds atomic metadata-pointer updates and conflict checks.
This lab used a single writer.

## Comparison with dbt

| Metric | dbt | Spark |
| --- | --- | --- |
| Users | 50 | 50 |
| Orders before append exercise | 2829 | 2829 |
| Total quantity | 8615 | 8615 |

DuckDB observed these schema differences:
- dbt order_id and user_id: UUID; Spark: VARCHAR.
- dbt ordered_at: TIMESTAMP WITH TIME ZONE; Spark: TIMESTAMP.
- Both: INTEGER quantity and VARCHAR product.

Spark identifiers therefore need explicit UUID validation if
consumers require it. Timestamp interpretation must also be
agreed across engines; the Spark session was configured to UTC.

A single dbt object is convenient for simple consumption.
Spark part-file directories support parallel reading and writing.
Both can be read by engines that understand their layout.

This script uses assertions for counts, duplicates and equality.
Equivalent dbt quality guarantees would additionally require
explicit checks for nulls, allowed values and foreign-key matches.

DataFrame dependencies build a logical execution graph.
Python execution order and temporary views connect SQL steps.
Unlike dbt ref/source, these do not provide the same project-level
dependency management and documentation.

No controlled dbt-versus-Spark build-time benchmark was recorded.
Startup, S3 I/O, caching, tests, repeated actions and hardware must
be aligned before comparing durations.

For a daily 10 GB batch, DuckDB with dbt is a reasonable starting
point on a suitable machine. For 10 TB, I would choose distributed
Spark. For a SQL-only analyst team, dbt is generally easier to
maintain; dbt can also target distributed engines.

## Exercise 1 — Birthdate validation

The first order timestamp is computed per user, then joined to
users. birthdate_is_valid compares birthdate with that date.

DataFrame and SQL results matched:
- 45 valid birthdates.
- 5 birthdates later than the first order.

The flag identifies the inconsistency without inventing a
replacement birthdate. A dbt implementation can express the
same rule as a model column and/or a data test.

## Exercise 2 — ZIP extraction

All three methods returned identical ZIP codes for 50 users.

Recorded benchmark, seconds:

| Method | Minimum | Mean |
| --- | --- | --- |
| Native regexp_extract | 0.0963 | 0.1209 |
| Python UDF | 0.1130 | 0.2008 |
| pandas UDF | 0.1518 | 0.1625 |

timeit.repeat was used instead of notebook %%timeit.
Each method was warmed up and measured three times.
collect() forced ZIP evaluation; count() alone could allow Spark
to skip computing that column.

These timings include S3 reads, scheduling and result collection.
On only 50 rows, overhead dominates and results vary between runs.
The native expression was fastest in the recorded run.

## Exercise 3 — Cache

Both Bronze DataFrames were cached and materialized with count.
Repeated counts returned 50 and 2829.
Plans showed InMemoryTableScan.

Storage UI showed:
- Users: 1 partition, 100% cached, 9.2 KiB in memory.
- Orders: 1 partition, 100% cached, 144.4 KiB in memory.
- Both: 0 B on disk.

Later actions can reuse these cached blocks rather than reread S3.
The cache lasts only for the running Spark application.

## Exercise 4 — Append and time travel

One synthetic order was appended to lab.silver.orders:
- Before append: 2829.
- After append: 2830.
- Historical snapshot 210022541243965938: 2829.
- Append snapshot: 7139067422734504314.

VERSION AS OF successfully read the earlier Iceberg snapshot.

The separate Parquet experiment directory
silver/spark/orders_append_exercise also contained 2830 rows
after append, but provides no built-in snapshot history.

The baseline silver/spark/orders.parquet output remains at 2829.
The Iceberg orders table remains at 2830 after the exercise.
Rerunning the script rebuilds the baseline before appending again.
