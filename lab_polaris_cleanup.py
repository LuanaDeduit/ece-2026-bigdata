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
    if PRINCIPAL != "dev":
        raise RuntimeError("Cleanup must run as dev")

    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
    spark.sql("DROP TABLE IF EXISTS polaris.gold.access_check")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")

    print("Remaining Silver tables:", flush=True)
    spark.sql("SHOW TABLES IN polaris.silver").show()

    count = spark.table("polaris.silver.orders").count()
    print("Orders preserved for Trino:", count, flush=True)
    assert count == 2830
    assert not spark.catalog.tableExists("polaris.silver.users")
    print("Cleanup passed.", flush=True)
finally:
    spark.stop()
