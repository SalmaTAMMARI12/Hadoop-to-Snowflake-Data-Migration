# Migration Waves Log — HDP to Snowflake

## Objectif
Migrer progressivement les tables Olist depuis Azure Blob vers Snowflake, en validant chaque vague avant de passer à la suivante.

## Règle GO / NO-GO
Une wave est considérée comme GO si :
- les tables sont chargées sans erreur ;
- les COUNT Snowflake correspondent aux COUNT source HDP ;
- aucun fichier en erreur dans COPY INTO.
## Wave 1 — Reference tables

### Tables
- CUSTOMERS
- SELLERS
- CATEGORY_TRANSLATION

### Validation Wave 1 côté Snowflake 

![alt text](doc/validation-wave1.png)|

### Decision
Wave 1 = GO.

## Wave 2 — Intermediate tables

### Tables
- PRODUCTS
- PAYMENTS
- GEOLOCATION

### Snowflake validation

![alt text](doc/validation-wave2.png)

### Decision
Wave 2 = GO.

## Wave 3 — Fact tables

### Tables
- ORDERS
- ORDER_ITEMS
- ORDER_REVIEWS

### Snowflake validation

![alt text](doc/validation-wave3.png)

### Decision
Wave 3 = GO.

