-- =============================================================================
-- PROJET : Migration Olist HDP -> Snowflake
-- SPRINT : 1 - Data Ingestion & Structuration (LIFT)
-- OBJECTIF : Conversion Staging CSV vers ORC non-transactionnel pour Spark
-- =============================================================================

CREATE DATABASE IF NOT EXISTS olist_legacy;
USE olist_legacy;

-- Exemple pour la table CUSTOMERS
DROP TABLE IF EXISTS customers_orc;
-- 2. Création de la table EXTERNAL (Standard industriel pour Spark compatibility)
CREATE EXTERNAL TABLE customers_orc (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
)
STORED AS ORC
LOCATION '/warehouse/tablespace/external/hive/olist_legacy.db/customers_orc'
TBLPROPERTIES ('transactional'='false');

-- 3. Migration des données du Staging (CSV) vers le format cible (ORC)
INSERT OVERWRITE TABLE customers_orc 
SELECT * FROM stg_customers_raw;

-- 4. Vérification de l'intégrité
ANALYZE TABLE customers_orc COMPUTE STATISTICS;
SELECT COUNT(*) FROM customers_orc;