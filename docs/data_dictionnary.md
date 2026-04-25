# DATA_DICTIONARY.md

## Projet
Migration & Modernisation de Données — **HDP → Snowflake**  
Phase : **Sprint 3 — Assess (Schema Mapping & Transformation Rules)**

## Objectif
Ce document définit, pour chaque table source Hive, le mapping vers la cible Snowflake :
- nom source
- type source
- nom cible
- type cible
- nullabilité cible
- règle de transformation
- commentaire métier

## Règles globales de transformation

| Règle | Description |
|---|---|
| `trim` | supprimer les espaces inutiles au début et à la fin |
| `upper` | convertir en majuscules |
| `lower` | convertir en minuscules |
| `to_timestamp` | convertir une chaîne de caractères date/heure en `TIMESTAMP` |
| `round(2)` | arrondir les montants à 2 décimales |
| `keep_null` | conserver les valeurs nulles |
| `cast integer` | convertir explicitement en entier |
| `cast decimal` | convertir explicitement en numérique décimal |

## Convention de nommage cible
- Les noms de colonnes Snowflake sont écrits en **MAJUSCULES**
- Les fautes de frappe ou incohérences du schéma source sont **corrigées dans la cible**
- Les noms cibles doivent être **clairs, cohérents et stables**

---

## 1. Table `customers_orc`

**Qualité observée :** table propre, aucune colonne nulle.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| customers_orc | customer_id | StringType | CUSTOMER_ID | VARCHAR(50) | NO | trim, upper | identifiant client |
| customers_orc | customer_unique_id | StringType | CUSTOMER_UNIQUE_ID | VARCHAR(50) | NO | trim, upper | identifiant métier |
| customers_orc | customer_zip_code_prefix | StringType | CUSTOMER_ZIP_CODE_PREFIX | VARCHAR(20) | NO | trim | code postal |
| customers_orc | customer_city | StringType | CUSTOMER_CITY | VARCHAR(100) | NO | trim, upper | ville |
| customers_orc | customer_state | StringType | CUSTOMER_STATE | VARCHAR(10) | NO | trim, upper | état |

---

## 2. Table `geolocation_orc`

**Qualité observée :** table volumineuse, aucune colonne nulle.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| geolocation_orc | geolocation_zip_code_prefix | StringType | GEOLOCATION_ZIP_CODE_PREFIX | VARCHAR(20) | NO | trim | code postal |
| geolocation_orc | geolocation_lat | DoubleType | GEOLOCATION_LAT | NUMBER(12,8) | NO | cast decimal | latitude |
| geolocation_orc | geolocation_lng | DoubleType | GEOLOCATION_LNG | NUMBER(12,8) | NO | cast decimal | longitude |
| geolocation_orc | geolocation_city | StringType | GEOLOCATION_CITY | VARCHAR(100) | NO | trim, upper | ville |
| geolocation_orc | geolocation_state | StringType | GEOLOCATION_STATE | VARCHAR(10) | NO | trim, upper | état |

---

## 3. Table `order_items_orc`

**Qualité observée :** table propre, aucune colonne nulle.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| order_items_orc | order_id | StringType | ORDER_ID | VARCHAR(50) | NO | trim, upper | identifiant commande |
| order_items_orc | order_item_id | IntegerType | ORDER_ITEM_ID | INTEGER | NO | cast integer | numéro de ligne de commande |
| order_items_orc | product_id | StringType | PRODUCT_ID | VARCHAR(50) | NO | trim, upper | identifiant produit |
| order_items_orc | seller_id | StringType | SELLER_ID | VARCHAR(50) | NO | trim, upper | identifiant vendeur |
| order_items_orc | shipping_limit_date | StringType | SHIPPING_LIMIT_TS | TIMESTAMP | NO | to_timestamp | date limite d’expédition |
| order_items_orc | price | DoubleType | PRICE | NUMBER(12,2) | NO | round(2) | prix |
| order_items_orc | freight_value | DoubleType | FREIGHT_VALUE | NUMBER(12,2) | NO | round(2) | frais de livraison |

---

## 4. Table `order_reviews_orc`

**Qualité observée :** plusieurs colonnes avec nulls, table à surveiller.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| order_reviews_orc | review_id | StringType | REVIEW_ID | VARCHAR(50) | YES | trim, upper | identifiant review |
| order_reviews_orc | order_id | StringType | ORDER_ID | VARCHAR(50) | YES | trim, upper | identifiant commande |
| order_reviews_orc | review_score | IntegerType | REVIEW_SCORE | INTEGER | YES | cast integer | score de satisfaction |
| order_reviews_orc | review_comment_title | StringType | REVIEW_COMMENT_TITLE | VARCHAR(500) | YES | trim | titre du commentaire |
| order_reviews_orc | review_comment_message | StringType | REVIEW_COMMENT_MESSAGE | VARCHAR(5000) | YES | trim | texte du commentaire |
| order_reviews_orc | review_creation_date | StringType | REVIEW_CREATION_TS | TIMESTAMP | YES | to_timestamp, keep_null | date de création |
| order_reviews_orc | review_answer_timestamp | StringType | REVIEW_ANSWER_TS | TIMESTAMP | YES | to_timestamp, keep_null | date de réponse |

---

## 5. Table `orders_orc`

**Qualité observée :** table propre, aucune colonne nulle.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| orders_orc | order_id | StringType | ORDER_ID | VARCHAR(50) | NO | trim, upper | identifiant commande |
| orders_orc | customer_id | StringType | CUSTOMER_ID | VARCHAR(50) | NO | trim, upper | identifiant client |
| orders_orc | order_status | StringType | ORDER_STATUS | VARCHAR(30) | NO | trim, upper | statut commande |
| orders_orc | order_purchase_timestamp | StringType | ORDER_PURCHASE_TS | TIMESTAMP | NO | to_timestamp | date achat |
| orders_orc | order_approved_at | StringType | ORDER_APPROVED_TS | TIMESTAMP | NO | to_timestamp | date approbation |
| orders_orc | order_delivered_carrier_date | StringType | ORDER_DELIVERED_CARRIER_TS | TIMESTAMP | NO | to_timestamp | remise au transporteur |
| orders_orc | order_delivered_customer_date | StringType | ORDER_DELIVERED_CUSTOMER_TS | TIMESTAMP | NO | to_timestamp | livraison client |
| orders_orc | order_estimated_delivery_date | StringType | ORDER_ESTIMATED_DELIVERY_TS | TIMESTAMP | NO | to_timestamp | date estimée de livraison |

---

## 6. Table `payments_orc`

**Qualité observée :** table propre, aucune colonne nulle.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| payments_orc | order_id | StringType | ORDER_ID | VARCHAR(50) | NO | trim, upper | identifiant commande |
| payments_orc | payment_sequential | IntegerType | PAYMENT_SEQUENTIAL | INTEGER | NO | cast integer | ordre du paiement |
| payments_orc | payment_type | StringType | PAYMENT_TYPE | VARCHAR(30) | NO | trim, upper | type de paiement |
| payments_orc | payment_installments | IntegerType | PAYMENT_INSTALLMENTS | INTEGER | NO | cast integer | nombre d’échéances |
| payments_orc | payment_value | DoubleType | PAYMENT_VALUE | NUMBER(12,2) | NO | round(2) | montant du paiement |

---

## 7. Table `products_orc`

**Qualité observée :** quelques colonnes nulles, surtout sur les attributs descriptifs.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| products_orc | product_id | StringType | PRODUCT_ID | VARCHAR(50) | NO | trim, upper | identifiant produit |
| products_orc | product_category_name | StringType | PRODUCT_CATEGORY_NAME | VARCHAR(200) | NO | trim, upper | catégorie produit |
| products_orc | product_name_lenght | IntegerType | PRODUCT_NAME_LENGTH | INTEGER | YES | cast integer, keep_null | longueur du nom du produit |
| products_orc | product_description_lenght | IntegerType | PRODUCT_DESCRIPTION_LENGTH | INTEGER | YES | cast integer, keep_null | longueur de la description |
| products_orc | product_photos_qty | IntegerType | PRODUCT_PHOTOS_QTY | INTEGER | YES | cast integer, keep_null | nombre de photos |
| products_orc | product_weight_g | IntegerType | PRODUCT_WEIGHT_G | INTEGER | YES | cast integer, keep_null | poids en grammes |
| products_orc | product_length_cm | IntegerType | PRODUCT_LENGTH_CM | INTEGER | YES | cast integer, keep_null | longueur en cm |
| products_orc | product_height_cm | IntegerType | PRODUCT_HEIGHT_CM | INTEGER | YES | cast integer, keep_null | hauteur en cm |
| products_orc | product_width_cm | IntegerType | PRODUCT_WIDTH_CM | INTEGER | YES | cast integer, keep_null | largeur en cm |

---

## 8. Table `sellers_orc`

**Qualité observée :** table propre, aucune colonne nulle.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| sellers_orc | seller_id | StringType | SELLER_ID | VARCHAR(50) | NO | trim, upper | identifiant vendeur |
| sellers_orc | seller_zip_code_prefix | StringType | SELLER_ZIP_CODE_PREFIX | VARCHAR(20) | NO | trim | code postal vendeur |
| sellers_orc | seller_city | StringType | SELLER_CITY | VARCHAR(100) | NO | trim, upper | ville vendeur |
| sellers_orc | seller_state | StringType | SELLER_STATE | VARCHAR(10) | NO | trim, upper | état vendeur |

---

## 9. Table `category_translation_orc`

**Qualité observée :** petite table de référence, très propre.

| source_table | source_column | source_type | target_column | target_type | nullable | transformation_rule | commentaire |
|---|---|---|---|---|---|---|---|
| category_translation_orc | product_category_name | StringType | PRODUCT_CATEGORY_NAME | VARCHAR(200) | NO | trim, lower | catégorie source |
| category_translation_orc | product_category_name_english | StringType | PRODUCT_CATEGORY_NAME_ENGLISH | VARCHAR(200) | NO | trim, upper | libellé anglais |

---

## Décision de normalisation des noms

Certaines colonnes sources comportent des incohérences ou fautes de frappe, par exemple :
- `product_name_lenght`
- `product_description_lenght`

Dans la cible Snowflake, elles sont renommées en :
- `PRODUCT_NAME_LENGTH`
- `PRODUCT_DESCRIPTION_LENGTH`

### Pourquoi ?
Parce que :
1. le schéma cible doit être **propre et professionnel**
2. il doit être **facile à comprendre** pour les analystes et développeurs
3. on évite de propager une faute de frappe du système source vers le système cible

### Important
On **ne change pas la signification de la donnée**.  
On change seulement le **nom technique** pour le rendre cohérent.

---

## Ordre recommandé de migration par vagues

### Wave 1 — Tables simples
- `category_translation_orc`
- `sellers_orc`
- `customers_orc`

### Wave 2 — Tables intermédiaires
- `products_orc`
- `payments_orc`
- `geolocation_orc`

### Wave 3 — Tables plus complexes
- `orders_orc`
- `order_items_orc`
- `order_reviews_orc`

---

## Conclusion
Ce Data Dictionary constitue la base de la phase **Assess**.  
Il servira à :
- construire les scripts de transformation PySpark
- définir les schémas Snowflake cibles
- documenter les décisions de migration
- préparer la validation post-migration
