from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")

print("Users partitions:", users_lines.getNumPartitions())
print("Orders partitions:", orders_lines.getNumPartitions())

header = orders_lines.first()
orders_data = orders_lines.filter(
    lambda line: line != header and line.strip() != ""
)

print("Orders:", orders_data.count())

# Repartitioning and lineage
orders_repartitioned = orders_lines.repartition(8)
print("Repartitioned:", orders_repartitioned.getNumPartitions())
print(orders_data.toDebugString().decode())
print("Orders, second count:", orders_data.count())

# Multiline user records
import re

print("First physical lines:", users_lines.take(6))
print("Physical user lines:", users_lines.count())
uuid_pattern = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}"
)
users_records = users_lines.filter(
    lambda line: uuid_pattern.match(line) is not None
)
print("Users:", users_records.count())


# Parse orders and compare aggregations
from operator import add

def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()
print("Parsed orders:", orders_rdd.count())

user_quantity = orders_rdd.map(
    lambda order: (order["user_id"], order["quantity"])
)

totals_reduce = user_quantity.reduceByKey(add)
totals_group = user_quantity.groupByKey().mapValues(sum)

print("reduceByKey:", totals_reduce.take(5))
print("groupByKey:", totals_group.take(5))
print("Same totals:", sorted(totals_reduce.collect()) == sorted(totals_group.collect()))


# Deduplicate orders
def most_recent(a, b):
    return a if a["date"] > b["date"] else b

orders_dedup = (
    orders_rdd
    .map(lambda order: (order["order_id"], order))
    .reduceByKey(most_recent)
    .values()
)

print("Orders before deduplication:", orders_rdd.count())
print("Orders after deduplication:", orders_dedup.count())


# Join orders with users
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(
    lambda order: (order["user_id"], order)
)

enriched = orders_kv.join(users_kv)

print("Joined orders:", enriched.count())
print("Join examples:", enriched.take(3))


# Exercise 1: aggregateByKey
def add_quantity(acc, quantity):
    return acc[0] + 1, acc[1] + quantity

def merge_totals(a, b):
    return a[0] + b[0], a[1] + b[1]

user_stats = user_quantity.aggregateByKey(
    (0, 0), add_quantity, merge_totals
)

user_averages = user_stats.mapValues(
    lambda stats: (stats[0], stats[1], stats[1] / stats[0])
)

print("User statistics (count, total, average):", user_averages.take(5))
print("Total order count:", user_stats.values().map(lambda x: x[0]).sum())
print("Total quantity:", user_stats.values().map(lambda x: x[1]).sum())


# Exercise 2: detect an unmatched order
fake_order = {
    "order_id": "test-missing-user-order",
    "user_id": "missing-user",
    "date": "2026-01-01",
    "quantity": 1,
    "product": "bread",
}

orders_with_fake = orders_rdd.union(sc.parallelize([fake_order], 1))
orders_with_fake_kv = orders_with_fake.map(
    lambda order: (order["user_id"], order)
)

left_joined = orders_with_fake_kv.leftOuterJoin(users_kv)
unmatched = left_joined.filter(lambda item: item[1][1] is None)

print("Orders including fake:", orders_with_fake.count())
print("Unmatched orders:", unmatched.count())
print("Unmatched example:", unmatched.take(1))


# Exercise 3: DataFrame comparison
orders_df = spark.createDataFrame(orders_rdd)
totals_df = orders_df.groupBy("user_id").sum("quantity")

totals_df.show(5, truncate=False)
totals_df.explain()

print("RDD execution lineage:")
print(totals_reduce.toDebugString().decode())

df_results = sorted(
    (row["user_id"], row["sum(quantity)"])
    for row in totals_df.collect()
)
print("DataFrame matches RDD:", df_results == sorted(totals_reduce.collect()))


# Exercise 4: cache timing
from time import perf_counter

orders_rdd.unpersist(blocking=True)

def timed_count(rdd, label):
    start = perf_counter()
    count = rdd.count()
    elapsed = perf_counter() - start
    print(f"{label}: count={count}, time={elapsed:.4f}s")

for name, rdd in [
    ("orders_rdd", orders_rdd),
    ("user_quantity", user_quantity),
]:
    rdd.unpersist(blocking=True)
    timed_count(rdd, name + " without cache, first")
    timed_count(rdd, name + " without cache, second")
    rdd.cache()
    timed_count(rdd, name + " filling cache")
    timed_count(rdd, name + " reading cache")
    rdd.unpersist(blocking=True)

spark.stop()
