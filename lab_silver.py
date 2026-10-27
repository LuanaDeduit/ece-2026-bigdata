import json
import os
import subprocess
from pyspark.sql import SparkSession

BUCKET = os.environ.get("LAB_BUCKET_NAME", "user-l-deduit-ece")

credentials = json.loads(subprocess.check_output(
    [
        "aws", "configure", "export-credentials",
        "--profile", "default", "--format", "process",
    ],
    text=True,
))

token = credentials.get("SessionToken", "")
provider = (
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    if token else
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

builder = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "https://s3.seaweedfs.adm.adaltas.cloud",
    )
    .config("spark.hadoop.fs.s3a.access.key", credentials["AccessKeyId"])
    .config("spark.hadoop.fs.s3a.secret.key", credentials["SecretAccessKey"])
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
)
if token:
    builder = builder.config("spark.hadoop.fs.s3a.session.token", token)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    users_bronze = spark.read.csv(
        f"s3a://{BUCKET}/bronze/users.csv",
        header=True, inferSchema=True, multiLine=True,
    )
    orders_bronze = spark.read.csv(
        f"s3a://{BUCKET}/bronze/orders.csv",
        header=True, inferSchema=True, multiLine=True,
    )

    users_bronze.printSchema()
    orders_bronze.printSchema()
    users_bronze.select("address").show(3, truncate=False)

    print("Bronze users:", users_bronze.count())
    print("Bronze orders:", orders_bronze.count())

    # Silver users
    from pyspark.sql import functions as F

    users_typed = users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    ).withColumn(
        "street", F.element_at("address_lines", 1)
    ).withColumn(
        "address_line_2", F.element_at("address_lines", 2)
    )

    stg_users = users_typed.select(
        "user_id", "username", "name", "sex", "email", "birthdate", "street",
        F.regexp_extract(
            "address_line_2", r"^(.+), [A-Z]{2} [0-9]{5}$", 1
        ).alias("city"),
        F.regexp_extract(
            "address_line_2", r"([A-Z]{2}) ([0-9]{5})$", 1
        ).alias("state"),
        F.regexp_extract(
            "address_line_2", r"([A-Z]{2}) ([0-9]{5})$", 2
        ).alias("zip_code"),
    )

    stg_users = stg_users.withColumn(
        "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
    )

    stg_users.show(5, truncate=False)
    print("Silver users:", stg_users.count())
    print("Users without city:", stg_users.filter(F.col("city").isNull()).count())

    # Silver orders
    from pyspark.sql.window import Window

    orders_typed = orders_bronze.select(
        F.col("uuid").cast("string").alias("order_id"),
        F.col("user_uuid").cast("string").alias("user_id"),
        F.col("date").cast("timestamp").alias("ordered_at"),
        F.col("quantity").cast("int").alias("quantity"),
        F.lower(F.trim("product")).alias("product"),
    )

    orders_with_duplicates = orders_typed.unionByName(orders_typed.limit(3))

    dedup_window = Window.partitionBy("order_id").orderBy(
        F.col("ordered_at").desc()
    )
    stg_orders = (
        orders_with_duplicates
        .withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    print("Original orders:", orders_typed.count())
    print("Orders with duplicates:", orders_with_duplicates.count())
    print("Silver orders:", stg_orders.count())

    orders_with_duplicates.createOrReplaceTempView("orders_for_sql")
    stg_orders_sql = spark.sql("""
        SELECT order_id, user_id, ordered_at, quantity, product
        FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY order_id ORDER BY ordered_at DESC
                   ) AS rn
            FROM orders_for_sql
        )
        WHERE rn = 1
    """)

    assert stg_orders.exceptAll(stg_orders_sql).count() == 0
    assert stg_orders_sql.exceptAll(stg_orders).count() == 0
    assert stg_orders.groupBy("order_id").count().filter("count > 1").count() == 0
    print("DataFrame and SQL match: True")
    print("Total quantity:", stg_orders.agg(F.sum("quantity")).first()[0])

    stg_users.createOrReplaceTempView("stg_users")
    spark.sql("""
        SELECT state, count(*) AS users,
               sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
        FROM stg_users
        GROUP BY state
        ORDER BY users DESC, state
    """).show(10)

    # Write and validate silver Parquet
    users_path = f"s3a://{BUCKET}/silver/spark/users.parquet"
    orders_path = f"s3a://{BUCKET}/silver/spark/orders.parquet"

    stg_users.write.mode("overwrite").parquet(users_path)
    stg_orders.write.mode("overwrite").parquet(orders_path)

    users_silver = spark.read.parquet(users_path)
    orders_silver = spark.read.parquet(orders_path)

    assert users_silver.count() == users_bronze.count()
    assert orders_silver.count() == stg_orders.count()

    assert users_silver.exceptAll(stg_users).count() == 0
    assert stg_users.exceptAll(users_silver).count() == 0
    assert orders_silver.exceptAll(stg_orders).count() == 0
    assert stg_orders.exceptAll(orders_silver).count() == 0

    print("Parquet users:", users_silver.count())
    print("Parquet orders:", orders_silver.count())
    print("Parquet round-trip matches: True")

    # Write and inspect Iceberg tables
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

    stg_users.writeTo("lab.silver.users").createOrReplace()
    stg_orders.writeTo("lab.silver.orders").createOrReplace()

    users_iceberg = spark.table("lab.silver.users")
    orders_iceberg = spark.table("lab.silver.orders")

    assert users_iceberg.count() == users_bronze.count()
    assert orders_iceberg.count() == stg_orders.count()

    print("Iceberg users:", users_iceberg.count())
    print("Iceberg orders:", orders_iceberg.count())

    spark.sql("""
        SELECT snapshot_id, operation, committed_at
        FROM lab.silver.orders.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    spark.sql("""
        SELECT file_path, file_format, record_count, file_size_in_bytes
        FROM lab.silver.orders.files
    """).show(truncate=False)

    print("Iceberg validation: True")

    # Exercise 1: birthdate validation
    first_orders = stg_orders.groupBy("user_id").agg(
        F.min("ordered_at").alias("first_order_at")
    )

    users_birthdate_df = (
        stg_users.join(first_orders, "user_id", "left")
        .withColumn(
            "birthdate_is_valid",
            F.col("birthdate") <= F.to_date("first_order_at"),
        )
    )

    stg_orders.createOrReplaceTempView("stg_orders")
    users_birthdate_sql = spark.sql("""
        SELECT u.*, f.first_order_at,
               u.birthdate <= CAST(f.first_order_at AS DATE)
                   AS birthdate_is_valid
        FROM stg_users u
        LEFT JOIN (
            SELECT user_id, min(ordered_at) AS first_order_at
            FROM stg_orders
            GROUP BY user_id
        ) f ON u.user_id = f.user_id
    """).select(*users_birthdate_df.columns)

    assert users_birthdate_df.exceptAll(users_birthdate_sql).count() == 0
    assert users_birthdate_sql.exceptAll(users_birthdate_df).count() == 0

    print("Birthdate DataFrame and SQL match: True")
    users_birthdate_df.groupBy("birthdate_is_valid").count().show()
    users_birthdate_df.filter(
        F.col("birthdate_is_valid") == False
    ).select("user_id", "birthdate", "first_order_at").show(truncate=False)

    # Exercise 2: ZIP extraction benchmark
    import re
    import timeit
    import pandas as pd

    zip_pattern = r"([A-Z]{2}) ([0-9]{5})$"

    @F.udf("string")
    def zip_python(address):
        if address is None:
            return None
        match = re.search(zip_pattern, address)
        return match.group(2) if match else ""

    @F.pandas_udf("string")
    def zip_pandas(addresses: pd.Series) -> pd.Series:
        extracted = addresses.str.extract(zip_pattern, expand=True)[1]
        return extracted.fillna("").where(addresses.notna(), None)

    zip_methods = {
        "native": users_bronze.select(
            "uuid", F.regexp_extract("address", zip_pattern, 2).alias("zip_code")
        ),
        "python_udf": users_bronze.select(
            "uuid", zip_python("address").alias("zip_code")
        ),
        "pandas_udf": users_bronze.select(
            "uuid", zip_pandas("address").alias("zip_code")
        ),
    }

    reference = sorted(
        (row["uuid"], row["zip_code"])
        for row in zip_methods["native"].collect()
    )

    for name, frame in zip_methods.items():
        result = sorted(
            (row["uuid"], row["zip_code"]) for row in frame.collect()
        )
        assert result == reference, name
        timings = timeit.repeat(lambda: frame.collect(), repeat=3, number=1)
        print(
            f"ZIP {name}: rows={len(result)}, "
            f"min={min(timings):.4f}s, "
            f"mean={sum(timings) / len(timings):.4f}s"
        )

    print("ZIP methods match: True")

    # Exercise 3: cache bronze DataFrames
    users_bronze.cache()
    orders_bronze.cache()

    print("Cached bronze users:", users_bronze.count(), flush=True)
    print("Cached bronze orders:", orders_bronze.count(), flush=True)

    print("Repeated counts:", users_bronze.count(), orders_bronze.count(), flush=True)
    users_bronze.select("uuid").explain()
    orders_bronze.select("quantity").explain()

    import sys
    if "--ui" in sys.argv:
        input("Cache ready. Inspect Spark UI, then press Enter.")

    # NEXT_STEPS
finally:
    spark.stop()
