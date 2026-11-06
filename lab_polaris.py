import json
import os
import subprocess
from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")
credential = os.environ[f"POLARIS_{PRINCIPAL.upper()}_CREDENTIAL"]

aws = json.loads(subprocess.check_output(
    ["aws", "configure", "export-credentials",
     "--profile", "default", "--format", "process"],
    text=True,
))
token = aws.get("SessionToken") or ""
provider = (
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    if token else
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

spark = (
    SparkSession.builder
    .appName(f"lab-polaris-{PRINCIPAL}")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.s3a.endpoint",
            "https://s3.seaweedfs.adm.adaltas.cloud")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", aws["AccessKeyId"])
    .config("spark.hadoop.fs.s3a.secret.key", aws["SecretAccessKey"])
    .config("spark.hadoop.fs.s3a.session.token", token)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse",
            f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri",
            f"{os.environ['POLARIS_URI']}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse",
            os.environ["POLARIS_CATALOG"])
    .config("spark.sql.catalog.polaris.credential", credential)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("Polaris principal:", PRINCIPAL, flush=True)
    print("S3 connector:",
          spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl"),
          flush=True)

    print("Hadoop catalog namespaces:", flush=True)
    spark.sql("SHOW NAMESPACES IN lab").show()

    print("Polaris catalog namespaces:", flush=True)
    if PRINCIPAL == "dev":
        spark.sql("SHOW NAMESPACES IN polaris").show()
    else:
        print("Namespace listing skipped for analyst.", flush=True)


    # Register the existing orders table
    if PRINCIPAL == "dev":
        def orders_objects():
            result = subprocess.check_output(
                ["aws", "s3api", "list-objects-v2", "--profile", "default",
                 "--bucket", BUCKET,
                 "--prefix", "warehouse/silver/orders/", "--output", "json"],
                text=True,
            )
            return {
                (obj["Key"], obj["Size"], obj["ETag"])
                for obj in json.loads(result).get("Contents", [])
            }

        spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
        before = orders_objects()

        if not spark.catalog.tableExists("polaris.silver.orders"):
            version = subprocess.check_output(
                ["aws", "s3", "--profile", "default", "cp",
                 f"s3://{BUCKET}/warehouse/silver/orders/metadata/version-hint.text",
                 "-"],
                text=True,
            ).strip()
            if not version.isdigit():
                raise ValueError("Invalid metadata version")

            metadata = (
                f"s3a://{BUCKET}/warehouse/silver/orders/"
                f"metadata/v{version}.metadata.json"
            )
            print("Registering metadata:", metadata, flush=True)
            spark.sql(f"""
                CALL polaris.system.register_table(
                    table => 'silver.orders',
                    metadata_file => '{metadata}'
                )
            """).show()
        else:
            print("Orders already registered; registration skipped.", flush=True)

        after = orders_objects()
        print("S3 objects before:", len(before), flush=True)
        print("S3 objects after:", len(after), flush=True)
        print("S3 objects unchanged:", before == after, flush=True)
        assert before == after

        hadoop_count = spark.table("lab.silver.orders").count()
        polaris_count = spark.table("polaris.silver.orders").count()
        print("Hadoop orders:", hadoop_count, flush=True)
        print("Polaris orders:", polaris_count, flush=True)
        assert hadoop_count == polaris_count

        spark.sql("SHOW TABLES IN polaris.silver").show()


    # Create users through Polaris and add country
    if PRINCIPAL == "dev":
        spark.sql("""
            SELECT snapshot_id, operation, committed_at
            FROM polaris.silver.orders.snapshots
            ORDER BY committed_at
        """).show(truncate=False)

        if not spark.catalog.tableExists("polaris.silver.users"):
            spark.table("lab.silver.users").writeTo(
                "polaris.silver.users"
            ).create()
            print("Users table created through Polaris.", flush=True)
        else:
            print("Users table already exists.", flush=True)

        source_count = spark.table("lab.silver.users").count()
        users_count = spark.table("polaris.silver.users").count()
        print("Hadoop users:", source_count, flush=True)
        print("Polaris users:", users_count, flush=True)
        assert source_count == users_count

        print("Polaris users location:", flush=True)
        spark.sql("DESCRIBE TABLE EXTENDED polaris.silver.users").filter(
            "col_name = 'Location'"
        ).show(truncate=False)

        if "country" not in spark.table("polaris.silver.users").columns:
            spark.sql(
                "ALTER TABLE polaris.silver.users ADD COLUMN country STRING"
            )

        print("Users schema after adding country:", flush=True)
        spark.sql("DESCRIBE polaris.silver.users").show(truncate=False)
        assert "country" in spark.table("polaris.silver.users").columns


    # Check analyst permissions
    if PRINCIPAL == "analyst":
        try:
            spark.sql("SHOW TABLES IN polaris.silver").show()
        except Exception as exc:
            if "ForbiddenException" not in str(exc):
                raise
            print("Table listing denied; testing direct SELECT next.",
                  flush=True)
        spark.sql("""
            SELECT product, count(*) AS orders
            FROM polaris.silver.orders
            GROUP BY product
            ORDER BY product
        """).show()

        checks = [
            ("INSERT", """
                INSERT INTO polaris.silver.orders
                SELECT * FROM polaris.silver.orders LIMIT 1
            """),
            ("ALTER TABLE", """
                ALTER TABLE polaris.silver.users ADD COLUMN phone STRING
            """),
        ]

        for label, statement in checks:
            try:
                spark.sql(statement).collect()
            except Exception as exc:
                message = str(exc)
                denied = (
                    "forbidden" in message.lower()
                    or "403" in message
                )
                print(f"{label} denied by Polaris: {denied}", flush=True)
                print(message[:2500], flush=True)
                if not denied:
                    raise
            else:
                raise AssertionError(f"{label} unexpectedly succeeded")

        print("Analyst permission checks passed.", flush=True)


    # Exercise 1: drop country and inspect metadata
    if PRINCIPAL == "dev":
        snapshots_before = {
            row.snapshot_id
            for row in spark.sql(
                "SELECT snapshot_id FROM polaris.silver.users.snapshots"
            ).collect()
        }
        files_before = {
            row.file_path
            for row in spark.sql(
                "SELECT file_path FROM polaris.silver.users.files"
            ).collect()
        }

        spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")

        snapshots_after = {
            row.snapshot_id
            for row in spark.sql(
                "SELECT snapshot_id FROM polaris.silver.users.snapshots"
            ).collect()
        }
        files_after = {
            row.file_path
            for row in spark.sql(
                "SELECT file_path FROM polaris.silver.users.files"
            ).collect()
        }

        print("Country removed:",
              "country" not in spark.table("polaris.silver.users").columns,
              flush=True)
        print("Snapshots unchanged:",
              snapshots_before == snapshots_after, flush=True)
        print("Data file paths unchanged:",
              files_before == files_after, flush=True)

        assert "country" not in spark.table("polaris.silver.users").columns
        assert snapshots_before == snapshots_after
        assert files_before == files_after

        spark.sql("""
            SELECT timestamp, file, latest_schema_id
            FROM polaris.silver.users.metadata_log_entries
            ORDER BY timestamp
        """).show(truncate=False)

        print("Exercise 1 passed.", flush=True)


    # Exercise 2: prepare a gold table for access checks
    if PRINCIPAL == "dev":
        spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")

        if not spark.catalog.tableExists("polaris.gold.access_check"):
            spark.sql("""
                CREATE TABLE polaris.gold.access_check
                USING iceberg
                AS SELECT 1 AS id, 'gold test' AS description
            """)

        print("Gold table readable by dev:", flush=True)
        spark.sql("SELECT * FROM polaris.gold.access_check").show()
        print("Exercise 2 preparation complete.", flush=True)


    # Exercise 2: verify namespace isolation
    if PRINCIPAL == "silver_only":
        print("Tables visible in silver:", flush=True)
        spark.sql("SHOW TABLES IN polaris.silver").show()

        orders = spark.table("polaris.silver.orders").count()
        users = spark.table("polaris.silver.users").count()
        print("Silver orders readable:", orders, flush=True)
        print("Silver users readable:", users, flush=True)
        assert orders == 2830
        assert users == 50

        try:
            spark.sql("SELECT * FROM polaris.gold.access_check").collect()
        except Exception as exc:
            message = str(exc)
            if "ForbiddenException" not in message:
                raise
            print("Gold read denied: True", flush=True)
            for line in message.splitlines():
                if "Forbidden" in line:
                    print(line[:1500], flush=True)
                    break
        else:
            raise AssertionError("Gold was readable: namespace isolation failed")

        print("Exercise 2 passed.", flush=True)

    # NEXT_STEPS
finally:
    spark.stop()
