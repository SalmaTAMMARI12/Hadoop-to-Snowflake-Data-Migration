# -*- coding: utf-8 -*-
import json
import logging
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("olist.export")

SOURCE_DB = "olist_ready"
HDFS_OUTPUT_DIR = "/user/student/olist_migration"

TABLES = [
    "customers_ready",
    "geolocation_ready",
    "order_items_ready",
    "order_reviews_ready",
    "orders_ready",
    "payments_ready",
    "products_ready",
    "sellers_ready",
    "category_translation_ready",
]

def create_spark_session(app_name="HDP_Olist_Export_Parquet"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_ready_table(spark, table_name):
    try:
        return spark.table("{}.{}".format(SOURCE_DB, table_name))
    except Exception:
        pass
    try:
        return spark.table(table_name)
    except Exception:
        pass
    path = "/apps/spark/warehouse/{}.db/{}".format(SOURCE_DB, table_name)
    log.info("Lecture directe parquet : %s", path)
    return spark.read.parquet(path)

def compute_table_md5(df):
    cols = df.columns
    concat_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    row_concat = F.concat_ws("||", *concat_cols)

    checksum_df = (
        df.select(F.md5(row_concat).alias("row_md5"))
          .agg(F.md5(F.concat_ws(",", F.sort_array(F.collect_list("row_md5")))).alias("global_md5"))
    )
    return checksum_df.collect()[0]["global_md5"]

def export_table(df, table_name):
    # colonnes en minuscules pour faciliter le chargement Snowflake
    df_export = df.toDF(*[c.lower() for c in df.columns])

    output_path = "{}/{}".format(HDFS_OUTPUT_DIR, table_name)

    log.info("Export de %s vers %s", table_name, output_path)

    (
        df_export.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    return output_path

def main():
    spark = create_spark_session()

    checksums = {}

    for table_name in TABLES:
        log.info("Traitement de la table : %s", table_name)

        df = load_ready_table(spark, table_name)
        row_count = df.count()
        checksum = compute_table_md5(df)
        export_path = export_table(df, table_name)

        checksums[table_name] = {
            "row_count": row_count,
            "checksum_md5": checksum,
            "export_path": export_path,
        }

        log.info("Table %s | rows=%s | md5=%s", table_name, row_count, checksum)

    with open("source_checksums.json", "w") as f:
        json.dump(checksums, f, indent=2)

    log.info("Fichier source_checksums.json généré")
    spark.stop()

if __name__ == "__main__":
    main()