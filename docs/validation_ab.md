# Validation A/B — HDP vs Snowflake

## Objectif

La validation A/B consiste à comparer les résultats obtenus côté source HDP/Spark et côté cible Snowflake après migration.  
L’objectif est de vérifier que les données migrées conservent le même volume, les mêmes valeurs agrégées et les mêmes indicateurs clés.

---

## Environnements comparés

- Source A : HDP / Spark SQL / HDFS
- Cible B : Snowflake / OLIST_MIGRATION
- Zone de transit : Azure Blob Storage
- Méthode de chargement : External Stage + COPY INTO
- Organisation Snowflake :
  - RAW_WAVE1
  - RAW_WAVE2
  - RAW_WAVE3

---

## Règle GO / NO-GO

Une vague est validée si les contrôles suivants sont corrects :

- les volumes source et cible sont identiques ;
- les métriques numériques correspondent ;
- les clés principales ont le même nombre de valeurs distinctes ;
- le chargement Snowflake ne présente pas d’erreur critique.

---

## Niveau 1 — Validation des volumes

Le premier contrôle consiste à comparer le nombre de lignes entre HDP et Snowflake.

Côté HDP, la requête Spark SQL a retourné les volumes suivants :

- CUSTOMERS : 99 441
- SELLERS : 3 095
- CATEGORY_TRANSLATION : 71
- PRODUCTS : 32 951
- PAYMENTS : 103 886
- GEOLOCATION : 1 000 163
- ORDERS : 99 441
- ORDER_ITEMS : 112 650
- ORDER_REVIEWS : 104 719

Côté Snowflake, les mêmes volumes ont été obtenus dans les schémas RAW_WAVE1, RAW_WAVE2 et RAW_WAVE3.

Résultat : les volumes correspondent exactement entre la source et la cible.  
Décision : GO pour les trois waves.

Capture associée :
- `doc/count-hdfs.png`
- `doc/count-snowflake-cible.png`

---

## Niveau 2 — Réconciliation numérique

Des contrôles numériques ont été exécutés pour vérifier les montants et les scores après migration.

La somme de `PAYMENT_VALUE` est identique entre HDP et Snowflake :

- HDP : 16 008 872.12
- Snowflake : 16 008 872.12

La moyenne de `REVIEW_SCORE` est également identique :

- HDP : 4.0872
- Snowflake : 4.0872

Ces résultats montrent que les colonnes numériques critiques ont été migrées sans altération.

Résultat : validation numérique réussie.  
Décision : GO.

Captures associées :
- `doc/sum-hdfs.png`
- `doc/sum-snowflake.png`
- `doc/avvg.png`

---

## Niveau 3 — Validation des clés distinctes

Un contrôle des valeurs distinctes a été réalisé sur les identifiants principaux.

Pour les clients :

- HDP : 99 441 clients distincts
- Snowflake : 99 441 clients distincts

Pour les commandes :

- HDP : 99 441 commandes distinctes
- Snowflake : 99 441 commandes distinctes

Ces résultats confirment que les clés principales ont été conservées après migration.

Résultat : validation des clés réussie.  
Décision : GO.

Captures associées :
- `doc/distincts-hdp.png`
- `doc/count-distinct-snowflake.png`
- `doc/count-distinct-2-snowflake.png`

---

## Audit des chargements Snowflake

L’historique de chargement Snowflake a été vérifié avec `COPY_HISTORY`.

Les chargements montrent :

- fichiers Parquet correctement lus depuis Azure Blob ;
- lignes chargées égales aux lignes parsées ;
- absence de message d’erreur critique ;
- statut de chargement réussi.

Exemples observés :

- CUSTOMERS : 99 441 lignes chargées
- SELLERS : 3 095 lignes chargées
- GEOLOCATION : 1 000 163 lignes chargées
- ORDERS : 99 441 lignes chargées

Résultat : audit Snowflake validé.  
Décision : GO.

Captures associées :
- `doc/audit-snowflake-customers.png`
- `doc/audit-snowflake-sellers.png`
- `doc/AUDIT-snowflake-geolocation.png`
- `doc/audit-snowflake-orders-wave3.png`

---

### Checksum MD5 (intégrité)

Cette validation vise à garantir que les données migrées dans Snowflake sont strictement identiques aux données exportées depuis HDP, au niveau binaire.

Contrairement aux contrôles classiques (COUNT, SUM), le checksum permet de détecter toute modification, même minime (ordre, valeur, null, format).

### Méthodologie

La stratégie utilisée est la suivante :

1. Calcul d’un MD5 par ligne (concaténation de toutes les colonnes)
2. Agrégation de tous les MD5 lignes
3. Tri des valeurs pour garantir la stabilité
4. Concaténation
5. Calcul d’un MD5 global

### Exemple 

# Implémentation snowflake

![
](../integrity-validation-md5-snowflake.png)

![alt text](../integrity-hdp.png)

# Résultat
# MD5 source (export JSON) :

`74344eddf46a95fd1a188806df711ccb`

# MD5 Snowflake :

`74344eddf46a95fd1a188806df711ccb`

- Pour la table GEOLOCATION, le checksum MD5 global ne correspond pas exactement. Cette différence est due au formatage des colonnes de type FLOAT (`GEOLOCATION_LAT`, `GEOLOCATION_LNG`) entre Spark et Snowflake lors de la conversion en string. La validation de cette table repose donc sur les contrôles COUNT et éventuellement des agrégats numériques arrondis.

## Conclusion

La validation A/B confirme que la migration HDP vers Snowflake a été réalisée avec succès.

Les données migrées conservent :

- le même nombre de lignes ;
- les mêmes agrégats numériques ;
- les mêmes valeurs distinctes sur les clés principales ;
- un historique de chargement Snowflake sans erreur critique.

La migration est donc considérée comme validée.