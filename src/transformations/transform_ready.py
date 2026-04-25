# -*- coding: utf-8 -*-
import logging
import os
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("olist.sprint3")

SOURCE_DB = "olist_legacy"
TARGET_DB = "olist_ready"
SOURCE_BASE_PATH = "/warehouse/tablespace/external/hive/olist_legacy.db"

def create_spark_session(app_name="HDP_Olist_Sprint3_Assess"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ensure_target_db(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(TARGET_DB))
    log.info("Base cible prête : %s", TARGET_DB)

def load_source_table(spark, table_name):
    path = os.path.join(SOURCE_BASE_PATH, table_name)
    log.info("Lecture source ORC directe : %s", path)
    return spark.read.format("orc").load(path)

def write_table(df, table_name):
    full_name = "{}.{}".format(TARGET_DB, table_name)
    log.info("Ecriture de la table cible : %s", full_name)
    df.write.mode("overwrite").format("orc").saveAsTable(full_name)
    log.info("Table écrite : %s", full_name)

def transform_customers(spark):
    df = load_source_table(spark, "customers_orc")
    return df.select(
        F.upper(F.trim(F.col("customer_id"))).alias("CUSTOMER_ID"),
        F.upper(F.trim(F.col("customer_unique_id"))).alias("CUSTOMER_UNIQUE_ID"),
        F.trim(F.col("customer_zip_code_prefix")).alias("CUSTOMER_ZIP_CODE_PREFIX"),
        F.upper(F.trim(F.col("customer_city"))).alias("CUSTOMER_CITY"),
        F.upper(F.trim(F.col("customer_state"))).alias("CUSTOMER_STATE")
    )

def transform_geolocation(spark):
    df = load_source_table(spark, "geolocation_orc")
    return df.select(
        F.trim(F.col("geolocation_zip_code_prefix")).alias("GEOLOCATION_ZIP_CODE_PREFIX"),
        F.col("geolocation_lat").cast("double").alias("GEOLOCATION_LAT"),
        F.col("geolocation_lng").cast("double").alias("GEOLOCATION_LNG"),
        F.upper(F.trim(F.col("geolocation_city"))).alias("GEOLOCATION_CITY"),
        F.upper(F.trim(F.col("geolocation_state"))).alias("GEOLOCATION_STATE")
    )

def transform_order_items(spark):
    df = load_source_table(spark, "order_items_orc")
    return df.select(
        F.upper(F.trim(F.col("order_id"))).alias("ORDER_ID"),
        F.col("order_item_id").cast("int").alias("ORDER_ITEM_ID"),
        F.upper(F.trim(F.col("product_id"))).alias("PRODUCT_ID"),
        F.upper(F.trim(F.col("seller_id"))).alias("SELLER_ID"),
        F.to_timestamp(F.col("shipping_limit_date")).alias("SHIPPING_LIMIT_TS"),
        F.round(F.col("price"), 2).alias("PRICE"),
        F.round(F.col("freight_value"), 2).alias("FREIGHT_VALUE")
    )

def transform_order_reviews(spark):
    df = load_source_table(spark, "order_reviews_orc")
    return df.select(
        F.upper(F.trim(F.col("review_id"))).alias("REVIEW_ID"),
        F.upper(F.trim(F.col("order_id"))).alias("ORDER_ID"),
        F.col("review_score").cast("int").alias("REVIEW_SCORE"),
        F.trim(F.col("review_comment_title")).alias("REVIEW_COMMENT_TITLE"),
        F.trim(F.col("review_comment_message")).alias("REVIEW_COMMENT_MESSAGE"),
        F.to_timestamp(F.col("review_creation_date")).alias("REVIEW_CREATION_TS"),
        F.to_timestamp(F.col("review_answer_timestamp")).alias("REVIEW_ANSWER_TS")
    )

def transform_orders(spark):
    df = load_source_table(spark, "orders_orc")
    return df.select(
        F.upper(F.trim(F.col("order_id"))).alias("ORDER_ID"),
        F.upper(F.trim(F.col("customer_id"))).alias("CUSTOMER_ID"),
        F.upper(F.trim(F.col("order_status"))).alias("ORDER_STATUS"),
        F.to_timestamp(F.col("order_purchase_timestamp")).alias("ORDER_PURCHASE_TS"),
        F.to_timestamp(F.col("order_approved_at")).alias("ORDER_APPROVED_TS"),
        F.to_timestamp(F.col("order_delivered_carrier_date")).alias("ORDER_DELIVERED_CARRIER_TS"),
        F.to_timestamp(F.col("order_delivered_customer_date")).alias("ORDER_DELIVERED_CUSTOMER_TS"),
        F.to_timestamp(F.col("order_estimated_delivery_date")).alias("ORDER_ESTIMATED_DELIVERY_TS")
    )

def transform_payments(spark):
    df = load_source_table(spark, "payments_orc")
    return df.select(
        F.upper(F.trim(F.col("order_id"))).alias("ORDER_ID"),
        F.col("payment_sequential").cast("int").alias("PAYMENT_SEQUENTIAL"),
        F.upper(F.trim(F.col("payment_type"))).alias("PAYMENT_TYPE"),
        F.col("payment_installments").cast("int").alias("PAYMENT_INSTALLMENTS"),
        F.round(F.col("payment_value"), 2).alias("PAYMENT_VALUE")
    )

def transform_products(spark):
    df = load_source_table(spark, "products_orc")
    return df.select(
        F.upper(F.trim(F.col("product_id"))).alias("PRODUCT_ID"),
        F.upper(F.trim(F.col("product_category_name"))).alias("PRODUCT_CATEGORY_NAME"),
        F.col("product_name_lenght").cast("int").alias("PRODUCT_NAME_LENGTH"),
        F.col("product_description_lenght").cast("int").alias("PRODUCT_DESCRIPTION_LENGTH"),
        F.col("product_photos_qty").cast("int").alias("PRODUCT_PHOTOS_QTY"),
        F.col("product_weight_g").cast("int").alias("PRODUCT_WEIGHT_G"),
        F.col("product_length_cm").cast("int").alias("PRODUCT_LENGTH_CM"),
        F.col("product_height_cm").cast("int").alias("PRODUCT_HEIGHT_CM"),
        F.col("product_width_cm").cast("int").alias("PRODUCT_WIDTH_CM")
    )

def transform_sellers(spark):
    df = load_source_table(spark, "sellers_orc")
    return df.select(
        F.upper(F.trim(F.col("seller_id"))).alias("SELLER_ID"),
        F.trim(F.col("seller_zip_code_prefix")).alias("SELLER_ZIP_CODE_PREFIX"),
        F.upper(F.trim(F.col("seller_city"))).alias("SELLER_CITY"),
        F.upper(F.trim(F.col("seller_state"))).alias("SELLER_STATE")
    )

def transform_category_translation(spark):
    df = load_source_table(spark, "category_translation_orc")
    return df.select(
        F.lower(F.trim(F.col("product_category_name"))).alias("PRODUCT_CATEGORY_NAME"),
        F.upper(F.trim(F.col("product_category_name_english"))).alias("PRODUCT_CATEGORY_NAME_ENGLISH")
    )

def verify_counts(spark):
    checks = [
        ("customers_orc", "customers_ready"),
        ("geolocation_orc", "geolocation_ready"),
        ("order_items_orc", "order_items_ready"),
        ("order_reviews_orc", "order_reviews_ready"),
        ("orders_orc", "orders_ready"),
        ("payments_orc", "payments_ready"),
        ("products_orc", "products_ready"),
        ("sellers_orc", "sellers_ready"),
        ("category_translation_orc", "category_translation_ready"),
    ]
    log.info("Verification des COUNT source vs cible")
    for src, tgt in checks:
        src_count = load_source_table(spark, src).count()
        tgt_count = spark.table("{}.{}".format(TARGET_DB, tgt)).count()
        status = "OK" if src_count == tgt_count else "FAIL"
        print("{:<30} {:>10} {:>10} {:>8}".format(src, src_count, tgt_count, status))

def main():
    spark = create_spark_session()
    ensure_target_db(spark)

    write_table(transform_customers(spark), "customers_ready")
    write_table(transform_geolocation(spark), "geolocation_ready")
    write_table(transform_order_items(spark), "order_items_ready")
    write_table(transform_order_reviews(spark), "order_reviews_ready")
    write_table(transform_orders(spark), "orders_ready")
    write_table(transform_payments(spark), "payments_ready")
    write_table(transform_products(spark), "products_ready")
    write_table(transform_sellers(spark), "sellers_ready")
    write_table(transform_category_translation(spark), "category_translation_ready")

    verify_counts(spark)
    spark.stop()

if __name__ == "__main__":
    main()
