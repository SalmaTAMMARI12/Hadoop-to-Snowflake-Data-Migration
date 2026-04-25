# -*- coding: utf-8 -*-
import json
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_PATH = os.getenv("OLIST_BASE_PATH", "/warehouse/tablespace/external/hive/olist_legacy.db")

ORC_TABLES = [
    "customers_orc",
    "geolocation_orc",
    "order_items_orc",
    "order_reviews_orc",
    "orders_orc",
    "payments_orc",
    "products_orc",
    "sellers_orc",
    "category_translation_orc",
]

DB_NAME = os.getenv("OLIST_DB_NAME", "olist_legacy")
SALTING_THRESHOLD_ROWS = 100000000  
OUTPUT_DIR = os.getenv("PROFILING_OUTPUT_DIR", "./profiling_reports")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("olist.profiling")


def create_spark_session(app_name="HDP_Olist_Profiling"):

    log.info("Initialisation de la SparkSession...")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.orc.impl", "native")
        .config("spark.sql.orc.enableVectorizedReader", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.network.timeout", "600s")
        .config("spark.sql.broadcastTimeout", "300")
        .enableHiveSupport()
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession créée | version Spark : {}".format(spark.version))
    return spark

def load_table(spark, table_name, base_path):
    """
    Charge une table ORC en essayant d'abord le chemin HDFS,
    puis le catalogue Hive.
    """
    table_path = os.path.join(base_path, table_name)
    log.info("Chargement de {} depuis {}".format(table_name, table_path))

    # Tentative 1 : lecture directe ORC depuis HDFS
    try:
        df = spark.read.format("orc").load(table_path)
        log.info("  Chargée via ORC direct | colonnes : {}".format(len(df.columns)))
        return df
    except Exception as e_orc:
        log.warning("  ORC direct échoué : {}".format(e_orc))

    # Tentative 2 : lecture via Hive
    try:
        query = "SELECT * FROM {}.{}".format(DB_NAME, table_name)
        df = spark.sql(query)
        log.info("  Chargée via Hive SQL | colonnes : {}".format(len(df.columns)))
        return df
    except Exception as e_hive:
        log.error("  Hive SQL échoué aussi : {}".format(e_hive))

    log.error("  Impossible de charger {}. Table ignorée.".format(table_name))
    return None

def _get_quality_flag(null_pct, distinct_count, total_rows):
    if null_pct > 50:
        return "CRITIQUE - >50% nulls"
    elif null_pct > 20:
        return "ATTENTION - 20-50% nulls"
    elif distinct_count == 1 and total_rows > 1:
        return "SUSPECT - colonne constante"
    elif distinct_count == total_rows and total_rows > 0:
        return "INFO - potentielle clé unique"
    else:
        return "OK"


def compute_column_stats(df, total_rows):
    """
    Calcule null_count, null_pct, distinct_count par colonne.
    Un seul passage agrégé sur le DataFrame.
    """
    log.info("  Calcul des statistiques colonnes ({} colonnes)...".format(len(df.columns)))
    agg_exprs = []
    for col_name in df.columns:
        col_ref = F.col("`{}`".format(col_name))
        agg_exprs.append(
            F.count(F.when(col_ref.isNull(), 1)).alias("{}__nulls".format(col_name))
        )
        agg_exprs.append(
            F.approx_count_distinct(col_ref).alias("{}__distinct".format(col_name))
        )

    agg_result = df.agg(*agg_exprs).collect()[0].asDict()

    stats = []
    for col_name in df.columns:
        null_count = agg_result.get("{}__nulls".format(col_name), 0) or 0
        distinct_count = agg_result.get("{}__distinct".format(col_name), 0) or 0
        if total_rows > 0:
            null_pct = round((float(null_count) / float(total_rows)) * 100.0, 4)
        else:
            null_pct = 0.0
        stats.append({
            "column": col_name,
            "dtype": str(df.schema[col_name].dataType),
            "null_count": int(null_count),
            "null_pct": null_pct,
            "distinct_count": int(distinct_count),
            "completeness_pct": round(100.0 - null_pct, 4),
            "quality_flag": _get_quality_flag(null_pct, distinct_count, total_rows),
        })

    return stats

def compute_md5_checksum(df):
    """
    Génère un checksum MD5 global de la table.
    """
    log.info("  Calcul du checksum MD5 (global table checksum)...")

    checksum_sample = int(os.getenv("CHECKSUM_SAMPLE_SIZE", "0"))
    df_for_checksum = df
    sampled = False

    if checksum_sample > 0:
        total = df.count()
        if total > checksum_sample:
            fraction = float(checksum_sample) / float(total)
            df_for_checksum = df.sample(False, fraction, 42)
            sampled = True
            log.info("  Echantillon utilisé pour MD5 : {:,} lignes".format(checksum_sample))

    concat_cols = [F.coalesce(F.col("`{}`".format(c)).cast("string"), F.lit("")) for c in df.columns]
    row_concat = F.concat_ws("||", *concat_cols)

    md5_df = (
        df_for_checksum
        .select(F.md5(row_concat).alias("row_md5"))
        .agg(F.md5(F.concat_ws(",", F.sort_array(F.collect_list("row_md5")))).alias("global_md5"))
    )

    checksum = md5_df.collect()[0]["global_md5"]
    if not checksum:
        checksum = "ERROR"
    if sampled:
        checksum = checksum + " (SAMPLED)"

    log.info("  MD5 : {}".format(checksum))
    return checksum

def analyze_salting_need(df, table_name, total_rows):
    """
    Identifie les tables candidates au salting.
    """
    salting_info = {
        "requires_salting": False,
        "reason": "Volume nominal",
        "recommended_n_salts": None,
        "recommended_partition_cols": [],
    }

    if total_rows >= SALTING_THRESHOLD_ROWS:
        import math
        salting_info["requires_salting"] = True
        salting_info["reason"] = "Volume critique : {:,} lignes >= {:,}".format(
            total_rows, SALTING_THRESHOLD_ROWS
        )
        n_salts = max(10, min(200, int(math.sqrt(float(total_rows) / 10000000.0))))
        salting_info["recommended_n_salts"] = n_salts
        log.warning("  SALTING REQUIS — {:,} lignes -> {} salts recommandés".format(total_rows, n_salts))

    key_cols = [c for c in df.columns if "id" in c.lower()][:3]
    if key_cols and total_rows > 1000000:
        for key_col in key_cols:
            try:
                top_val = (
                    df.groupBy(F.col("`{}`".format(key_col)))
                    .count()
                    .orderBy(F.col("count").desc())
                    .first()
                )
                if top_val:
                    top_freq_pct = (float(top_val["count"]) / float(total_rows)) * 100.0
                    if top_freq_pct > 30.0:
                        salting_info["requires_salting"] = True
                        salting_info["reason"] += " | Skew sur '{}' : {:.1f}% sur 1 valeur".format(
                            key_col, top_freq_pct
                        )
                        if key_col not in salting_info["recommended_partition_cols"]:
                            salting_info["recommended_partition_cols"].append(key_col)
                        log.warning("  Skew détecté sur {} : {:.1f}%".format(key_col, top_freq_pct))
            except Exception as e:
                log.warning("  Analyse skew ignorée pour {} : {}".format(key_col, e))

    return salting_info


def _build_quality_summary(col_stats):
    cols_with_nulls = [s for s in col_stats if s["null_count"] > 0]
    critical_cols = [s for s in col_stats if s["null_pct"] > 50]
    if col_stats:
        overall_completeness = round(
            sum([s["completeness_pct"] for s in col_stats]) / float(len(col_stats)), 2
        )
    else:
        overall_completeness = 0

    return {
        "total_columns": len(col_stats),
        "columns_with_nulls": len(cols_with_nulls),
        "critical_null_columns": len(critical_cols),
        "overall_completeness_pct": overall_completeness,
        "quality_gate": "PASS" if len(critical_cols) == 0 else "FAIL - colonnes critiques détectées",
    }


def profile_table(spark, table_name, base_path):
    start_time = datetime.now()
    log.info("\n{}".format("=" * 60))
    log.info("PROFILAGE : {}".format(table_name))
    log.info("{}".format("=" * 60))

    df = load_table(spark, table_name, base_path)
    if df is None:
        return {
            "table": table_name,
            "status": "ERROR",
            "error": "Table inaccessible - voir logs",
            "profiled_at": start_time.isoformat(),
        }

    total_rows = df.count()
    schema_info = dict([(col.name, str(col.dataType)) for col in df.schema.fields])
    log.info("  Lignes : {:,} | Colonnes : {}".format(total_rows, len(df.columns)))

    col_stats = compute_column_stats(df, total_rows)
    checksum_md5 = compute_md5_checksum(df)
    salting = analyze_salting_need(df, table_name, total_rows)

    duration_sec = round((datetime.now() - start_time).total_seconds(), 2)

    report = {
        "table": table_name,
        "status": "OK",
        "profiled_at": start_time.isoformat(),
        "duration_seconds": duration_sec,
        "metadata": {
            "total_rows": total_rows,
            "total_columns": len(df.columns),
            "schema": schema_info,
        },
        "integrity": {
            "checksum_md5": checksum_md5,
            "checksum_standard": " MD5 over full table rows",
        },
        "salting_analysis": salting,
        "column_statistics": col_stats,
        "quality_summary": _build_quality_summary(col_stats),
    }

    log.info("  Profilage terminé en {:.1f}s".format(duration_sec))
    return report


def export_report(reports, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = "phase1_profiling_report_{}.json".format(timestamp)
    filepath = os.path.join(output_dir, filename)

    full_report = {
        "report_metadata": {
            "project": "Migration HDP -> Snowflake | Olist Dataset",
            "phase": "Phase 1 - Lift & Inventaire Source",
            "sprint": "Sprint 2",
            "standard": "IBM LAMV-inspired",
            "author": "Salma - Junior Data Engineer",
            "generated_at": datetime.now().isoformat(),
            "total_tables_profiled": len([r for r in reports if r.get("status") == "OK"]),
            "total_tables_failed": len([r for r in reports if r.get("status") == "ERROR"]),
        },
        "salting_candidates": [
            r["table"] for r in reports
            if r.get("status") == "OK" and r.get("salting_analysis", {}).get("requires_salting")
        ],
        "tables": reports,
    }

    with open(filepath, "w") as f:
        json.dump(full_report, f, indent=2)

    log.info("\nRapport exporté -> {}".format(filepath))
    return filepath


def print_summary_console(reports):
    print("\n" + "=" * 78)
    print("  RAPPORT DE PROFILAGE - PHASE 1 LIFT | OLIST | HDP / PYSPARK")
    print("=" * 78)
    print("  {0:<30} {1:>12} {2:>6} {3:>10} {4:>8}".format("TABLE", "LIGNES", "COLS", "NULL_COLS", "SALT"))
    print("-" * 78)

    for r in reports:
        if r.get("status") == "ERROR":
            print("  {0:<30} {1}".format(r["table"], "ERREUR"))
            continue

        meta = r.get("metadata", {})
        qs = r.get("quality_summary", {})
        salt = "YES" if r.get("salting_analysis", {}).get("requires_salting") else "NO"

        print(
            "  {0:<30} {1:>12,} {2:>6} {3:>10} {4:>8}".format(
                r["table"],
                meta.get("total_rows", 0),
                meta.get("total_columns", 0),
                qs.get("columns_with_nulls", 0),
                salt,
            )
        )

    print("=" * 78)

def main():
    log.info("DÉMARRAGE - Phase 1 : Lift & Inventaire Source")
    log.info("  Base path     : {}".format(BASE_PATH))
    log.info("  Database      : {}".format(DB_NAME))
    log.info("  Tables cibles : {}".format(len(ORC_TABLES)))
    log.info("  Output dir    : {}".format(OUTPUT_DIR))

    spark = create_spark_session()
    reports = []

    for table_name in ORC_TABLES:
        try:
            reports.append(profile_table(spark, table_name, BASE_PATH))
        except Exception as e:
            log.error("Erreur inattendue sur {} : {}".format(table_name, e), exc_info=True)
            reports.append({
                "table": table_name,
                "status": "ERROR",
                "error": str(e),
                "profiled_at": datetime.now().isoformat(),
            })

    report_path = export_report(reports, OUTPUT_DIR)
    print_summary_console(reports)
    log.info("Phase 1 terminée | Rapport : {}".format(report_path))
    spark.stop()

if __name__ == "__main__":
    main()