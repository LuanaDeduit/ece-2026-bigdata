# Spark RDD lab — Answers and results

## Execution

The lab was implemented in lab_rdd.py and run with:

    spark-submit --master 'local[2]' --driver-memory 512m \
      --conf spark.sql.shuffle.partitions=2 lab_rdd.py

Add --ui to keep Spark running while inspecting its UI.
Two local threads were used to fit the Onyxia environment.

## 1. Partitions

Both input RDDs had 2 partitions. For textFile, the default
minimum is normally min(defaultParallelism, 2). Hadoop input
splits and file size also affect the actual partition count.

repartition(8) produced 8 partitions. It uses a shuffle.
coalesce(n) normally reduces partitions without a shuffle;
coalesce(n, shuffle=True) explicitly enables one.

## 2. Lazy evaluation

Transformations such as filter and map build dependencies.
Actions such as first, count and take trigger computation.
In particular, first() already reads enough data to find the header.

The second uncached count triggers another job and recomputes
the filtering. Caching orders_data before the first count would
allow subsequent actions to reuse its cached partitions.
A cached count still runs a job, but avoids recomputing the input.

## 3. Multiline CSV

users.csv contains 101 physical lines but only 50 users.
textFile treats each physical line as a separate element.
The CSV reader with multiLine=True understands quoted fields
containing newlines and reconstructs complete records.

A line-oriented reader is safe when record boundaries align
with physical line boundaries.

Filtering lines by their initial UUID recovered 50 user starts.
It did not reconstruct addresses. Only uuid and username were
used for the join.

## 4. Manual parsing

An extra comma in product would make split(",") return too
many fields. Unpacking would fail when an action evaluates
the affected record.

A CSV reader handles delimiters inside correctly quoted fields.
With a schema, field conversion occurs during parsing when
data is read. Malformed-record handling depends on the selected
mode, such as PERMISSIVE or FAILFAST. DataFrames do not enforce
all business rules automatically.

## 5. Aggregation

reduceByKey combines quantities within each partition before
the shuffle. groupByKey transfers the individual values,
grouped by key, without first replacing them with partial sums.

Observed shuffle writes in the Spark UI:

| Operation | Stage | Shuffle write |
| --- | --- | --- |
| reduceByKey | 7 | 2.5 KiB |
| groupByKey | 9 | 6.1 KiB |

Both methods returned the same totals.
reduceByKey transferred about 2.4 times less data in this run.

Operations needing the complete collection of values per key,
such as arbitrary processing of all sorted values, can justify
groupByKey. A simple reduction cannot directly express every
such operation. However, groupByKey is not universally required:
aggregateByKey, combineByKey or a DataFrame operation may offer
a better alternative.

## 6. Deduplication

reduceByKey groups matching order_id values through a shuffle
and applies the custom function to keep the latest order.

There were 2829 orders before and after deduplication:
no duplicate order_id was present in this dataset.

The window expression states the ordering and selection intent
more directly. The RDD version requires a custom comparison.
Comparing dates as strings assumes a consistent sortable format;
parsing timestamps is safer for mixed formats or time zones.

## 7. Join

The inner join produced 2829 results, each containing
(user_id, (order, username)).

A shuffle can be avoided when both RDDs already have a compatible
partitioner, including the same partition count and partition
function, with matching keys in corresponding partitions.
The same number of partitions alone is insufficient.

An inner join drops unmatched orders. A leftOuterJoin preserves
them and returns None for the missing user.

collect() transfers every result to the driver. On a large
dataset, this can exhaust driver memory. take(n) is suitable
for inspecting a small sample.

## 8. Exercises

### aggregateByKey

The accumulator stores (order_count, total_quantity).
The average is total_quantity / order_count.

Validation across all users:
- Order count: 2829.
- Total quantity: 8615.

### Missing user

One synthetic order with a nonexistent user was added to a copy.
The original RDD was unchanged.

- Orders including the synthetic order: 2830.
- Unmatched orders after leftOuterJoin: 1.

### DataFrame comparison

The DataFrame aggregation is:

    orders_df.groupBy("user_id").sum("quantity")

It expresses the aggregation in one line. The RDD equivalent
needs a map to key-value pairs followed by reduceByKey.

The DataFrame and RDD totals matched exactly.
explain() displays Spark's query execution plan.
toDebugString() displays RDD dependencies and shuffle boundaries.

### Cache measurements

The aggregated RDD is named totals_reduce in the script.
It contains 50 elements, one per user.

| Aggregated RDD count | Time |
| --- | --- |
| Without cache, first | 0.2541 s |
| Without cache, second | 0.2486 s |
| Filling cache | 0.2372 s |
| Reading cache | 0.1633 s |

An earlier run measured the parsed orders RDD:

| Parsed orders count | Time |
| --- | --- |
| Without cache, first | 0.2601 s |
| Without cache, second | 0.2455 s |
| Filling cache | 0.2005 s |
| Reading cache | 0.3306 s |

Caching improved the aggregated count in the recorded run,
but did not improve the parsed-order count in the earlier run.
These small datasets are sensitive to scheduling overhead
and runtime variation.

Earlier aggregation actions had already produced shuffle files.
The uncached aggregated counts may reuse those files, so these
measurements do not represent a completely cold pipeline.

The Storage tab confirmed that the parsed orders RDD was fully
cached: 2 partitions, 100%, 168.0 KiB in memory and 0 B on disk.
