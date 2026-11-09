# 📚 Data Dictionary — NYC Taxi Data Platform

> Catalogue exhaustif de chaque champ à travers les 4 couches du Lakehouse Médaillon et l'entrepôt de données dimensionnel.

---

## 🥉 Couche Bronze (Données Brutes Immuables)

### `s3a://bronze/yellow_tripdata/` (Parquet)
Stockage des fichiers bruts immuables mensuels publiés par la NYC Taxi and Limousine Commission (TLC). Les types ci-dessous correspondent au schéma strict appliqué (`YELLOW_TAXI_RAW_SCHEMA`), sans inférence de schéma.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `VendorID` | IntegerType | Oui | Code identifiant le fournisseur de technologie TPEP (1 = Creative Mobile Technologies, LLC ; 2 = VeriFone Inc.). |
| `tpep_pickup_datetime` | TimestampType | Oui | Horodatage exact (date et heure) de l'enclenchement du taximètre lors de la prise en charge du passager. |
| `tpep_dropoff_datetime` | TimestampType | Oui | Horodatage exact (date et heure) de l'arrêt du taximètre lors de la dépose du passager. |
| `passenger_count` | LongType | Oui | Nombre de passagers dans le véhicule (saisie manuelle du chauffeur). |
| `trip_distance` | DoubleType | Oui | Distance totale parcourue durant le trajet en miles enregistrée par le taximètre. |
| `RatecodeID` | LongType | Oui | Code tarifaire appliqué à la fin de la course (1 = Standard, 2 = JFK, 3 = Newark, 4 = Nassau/Westchester, 5 = Négocié, 6 = Course de groupe). |
| `store_and_fwd_flag` | StringType | Oui | Indicateur technique de stockage hors ligne ('Y' = stocké en mémoire locale avant transmission suite à une perte de connexion, 'N' = transmission temps réel). |
| `PULocationID` | IntegerType | Oui | Identifiant de la zone TLC de prise en charge (Pick-Up, 1 à 265). |
| `DOLocationID` | IntegerType | Oui | Identifiant de la zone TLC de dépose (Drop-Off, 1 à 265). |
| `payment_type` | LongType | Oui | Mode de règlement de la course (1 = Carte bancaire, 2 = Espèces, 3 = Sans frais, 4 = Litige, 5 = Inconnu, 6 = Trajet annulé). |
| `fare_amount` | DoubleType | Oui | Tarif de base calculé au temps passé et à la distance parcourue par le taximètre (\$ USD). |
| `extra` | DoubleType | Oui | Suppléments divers (\$ USD), incluant les heures de pointe (1.00 \$) et le tarif de nuit (0.50 \$). |
| `mta_tax` | DoubleType | Oui | Taxe d'État de la Metropolitan Transportation Authority de 0.50 \$ automatiquement déclenchée (\$ USD). |
| `tip_amount` | DoubleType | Oui | Montant du pourboire (\$ USD). Renseigné automatiquement pour les paiements par carte bancaire (ne comprend pas les pourboires en espèces). |
| `tolls_amount` | DoubleType | Oui | Montant cumulé des péages autoroutiers, ponts et tunnels franchis au cours du trajet (\$ USD). |
| `improvement_surcharge` | DoubleType | Oui | Taxe d'amélioration du service de taxi (0.30 \$ instaurée en 2015, portée à 1.00 \$ en 2023) (\$ USD). |
| `total_amount` | DoubleType | Oui | Montant total facturé aux passagers (\$ USD, hors pourboire en espèces). |
| `congestion_surcharge` | DoubleType | Oui | Surcharge de congestion de l'État de New York (2.50 \$ pour les Yellow Taxis au sud de la 96e rue de Manhattan) (\$ USD). |
| `Airport_fee` | DoubleType | Oui | Redevance aéroportuaire pour les prises en charge dans les aéroports LaGuardia (LGA) et JFK (1.25 \$) (\$ USD). |
| `cbd_congestion_fee` | DoubleType | Oui | Péage urbain de décongestion du Central Business District de Manhattan (\$ USD). |

---

## 🥈 Couche Silver (Delta Lake — Nettoyé & Dédupliqué)

### `s3a://silver/yellow/` (Delta Lake)
Données nettoyées, typées, validées selon les 5 règles qualité, enrichies de métriques dérivées et dédupliquées via hash cryptographique SHA-256. Partitionné physiquement par `year` et `month`.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `vendor_id` | IntegerType | Oui | Identifiant normalisé du fournisseur TPEP (cast depuis `VendorID`). |
| `tpep_pickup_datetime` | TimestampType | Oui | Horodatage exact de début de trajet validé dans les bornes du mois civil traité. |
| `tpep_dropoff_datetime` | TimestampType | Oui | Horodatage exact de fin de trajet. |
| `passenger_count` | IntegerType | Oui | Nombre de passagers (cast entier depuis `passenger_count`). |
| `trip_distance` | DoubleType | Oui | Distance parcourue en miles (validée strictement positive et ≤ 100 miles). |
| `ratecode_id` | IntegerType | Oui | Code tarifaire normalisé (cast entier depuis `RatecodeID`). |
| `store_and_fwd_flag` | StringType | Oui | Indicateur technique d'enregistrement hors ligne ('Y' ou 'N'). |
| `pu_location_id` | IntegerType | Oui | Identifiant de la zone TLC de prise en charge (validé entre 1 et 265). |
| `do_location_id` | IntegerType | Oui | Identifiant de la zone TLC de dépose (validé entre 1 et 265). |
| `payment_type` | IntegerType | Oui | Code numérique du mode de paiement (cast entier depuis `payment_type`). |
| `fare_amount` | DoubleType | Oui | Tarif de base de la course (\$ USD). |
| `extra` | DoubleType | Oui | Suppléments et extras divers (\$ USD). |
| `mta_tax` | DoubleType | Oui | Taxe MTA métropolitaine de 0.50 \$ (\$ USD). |
| `tip_amount` | DoubleType | Oui | Montant du pourboire par carte bancaire (\$ USD). |
| `tolls_amount` | DoubleType | Oui | Montant total des péages acquittés (\$ USD). |
| `improvement_surcharge` | DoubleType | Oui | Taxe d'amélioration du service (\$ USD). |
| `total_amount` | DoubleType | Oui | Montant total facturé (validé strictement positif > 0 \$ USD). |
| `congestion_surcharge` | DoubleType | Oui | Surcharge de circulation au sud de Manhattan (\$ USD). |
| `airport_fee` | DoubleType | Oui | Taxe d'accès aéroportuaire (\$ USD). |
| `duration_minutes` | DoubleType | Oui | Durée de la course calculée en minutes : `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60.0`. |
| `avg_speed_mph` | DoubleType | Oui | Vitesse moyenne calculée en miles par heure : `round(trip_distance / (duration_minutes / 60.0), 2)`. Null si la durée est nulle. |
| `pickup_hour` | IntegerType | Oui | Heure de la journée de la prise en charge (0 à 23). |
| `day_of_week` | StringType | Oui | Nom du jour de la semaine en anglais ('Monday' à 'Sunday') extrait de `tpep_pickup_datetime`. |
| `is_airport_trip` | BooleanType | Oui | Indicateur booléen (True si `pu_location_id` ou `do_location_id` est un aéroport : Newark [1], JFK [132] ou LaGuardia [138]). |
| `trip_hash` | StringType | Non | Empreinte cryptographique SHA-256 calculée sur `vendor_id\|pickup\|dropoff\|pu\|do\|distance\|fare` servant de clé unique de dédoublonnage et surrogate key `trip_id`. |
| `year` | IntegerType | Non | Année du batch mensuel traité (colonne de partition Delta Lake). |
| `month` | IntegerType | Non | Mois du batch mensuel traité (colonne de partition Delta Lake). |

---

## 🚫 Couche Quarantine (Rejets Qualité)

### `s3a://quarantine/yellow/` (Parquet)
Enregistrements non conformes aux règles de validation qualité rejetés lors de la transformation Bronze → Silver. Partitionné par `year` et `month`.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `reject_reason` | StringType | Non | Motif(s) d'échec aux règles qualité concaténés par `\|` (ex : `invalid_duration\|invalid_total_amount`). |
| `vendor_id` | IntegerType | Oui | Identifiant du fournisseur TPEP. |
| `tpep_pickup_datetime` | TimestampType | Oui | Horodatage de prise en charge d'origine (potentiellement hors limites mensuelles). |
| `tpep_dropoff_datetime` | TimestampType | Oui | Horodatage de dépose d'origine. |
| `passenger_count` | IntegerType | Oui | Nombre de passagers déclaré. |
| `trip_distance` | DoubleType | Oui | Distance enregistrée en miles (potentiellement ≤ 0 ou > 100 mi). |
| `ratecode_id` | IntegerType | Oui | Code tarifaire brut. |
| `store_and_fwd_flag` | StringType | Oui | Indicateur technique de stockage hors ligne. |
| `pu_location_id` | IntegerType | Oui | Zone de départ (potentiellement nulle ou hors référentiel 1-265). |
| `do_location_id` | IntegerType | Oui | Zone d'arrivée (potentiellement nulle ou hors référentiel 1-265). |
| `payment_type` | IntegerType | Oui | Code du mode de règlement. |
| `fare_amount` | DoubleType | Oui | Tarif de base (\$ USD). |
| `extra` | DoubleType | Oui | Suppléments (\$ USD). |
| `mta_tax` | DoubleType | Oui | Taxe MTA (\$ USD). |
| `tip_amount` | DoubleType | Oui | Pourboire (\$ USD). |
| `tolls_amount` | DoubleType | Oui | Péages (\$ USD). |
| `improvement_surcharge` | DoubleType | Oui | Surcharge d'amélioration (\$ USD). |
| `total_amount` | DoubleType | Oui | Montant total (potentiellement ≤ 0 \$ USD). |
| `congestion_surcharge` | DoubleType | Oui | Surcharge congestion (\$ USD). |
| `airport_fee` | DoubleType | Oui | Taxe aéroport (\$ USD). |
| `year` | IntegerType | Non | Année d'exécution du batch de contrôle (partition Parquet). |
| `month` | IntegerType | Non | Mois d'exécution du batch de contrôle (partition Parquet). |

---

## 🥇 Couche Gold (Delta Lake — Pré-agrégats)

### `s3a://gold/zone_hourly_metrics/` (Delta Lake)
Agrégats spatio-temporels précalculés par le job Spark `silver_to_gold.py` au grain 'zone de prise en charge × zone de dépose × date × heure' avec enrichissement dimensionnel des noms de zones et boroughs.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `pickup_date` | DateType | Oui | Date civile de la course (`to_date(tpep_pickup_datetime)`). |
| `pickup_hour` | IntegerType | Oui | Heure de prise en charge (0 à 23). |
| `day_of_week` | StringType | Oui | Nom du jour de la semaine ('Monday' à 'Sunday'). |
| `pu_borough` | StringType | Oui | Arrondissement (Borough) de prise en charge joint depuis la table de référence des zones. |
| `pu_zone` | StringType | Oui | Nom usuel de la zone TLC de prise en charge. |
| `do_borough` | StringType | Oui | Arrondissement (Borough) de dépose joint depuis la table de référence des zones. |
| `do_zone` | StringType | Oui | Nom usuel de la zone TLC de dépose. |
| `trip_count` | LongType | Non | Nombre total de courses enregistrées sur ce créneau et trajet. |
| `total_passengers` | LongType | Oui | Volume cumulé de passagers transportés. |
| `avg_trip_distance_miles` | DoubleType | Oui | Distance moyenne par course en miles (`round(avg(trip_distance), 2)`). |
| `avg_duration_minutes` | DoubleType | Oui | Durée moyenne par course en minutes (`round(avg(duration_minutes), 2)`). |
| `total_fare` | DoubleType | Oui | Somme totale des montants de base de facturation (\$ USD). |
| `total_tips` | DoubleType | Oui | Somme totale des pourboires collectés (\$ USD). |
| `total_revenue` | DoubleType | Oui | Chiffre d'affaires cumulé total généré (\$ USD). |
| `avg_speed_mph` | DoubleType | Oui | Vitesse moyenne estimée en miles par heure (`round(avg(avg_speed_mph), 2)`). |
| `airport_trip_count` | LongType | Non | Nombre de courses reliant un aéroport majeur. |
| `year` | IntegerType | Non | Année de la course (colonne de partition Delta Lake). |
| `month` | IntegerType | Non | Mois de la course (colonne de partition Delta Lake). |

---

## 🏛️ Entrepôt de Données (PostgreSQL / DuckDB)

### Staging Views

#### `stg_silver_trips`
Vue de staging standardisant les types SQL, normalisant les libellés et calculant les indicateurs d'intégrité à partir de la couche Silver Delta Lake.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `trip_id` | VARCHAR / TEXT | Non | Clé primaire unique de la course (empreinte `trip_hash` SHA-256 issue de Spark ou fallback MD5). |
| `vendor_id` | INTEGER | Oui | Identifiant du fournisseur TPEP (1 = Creative Mobile, 2 = VeriFone). |
| `pickup_datetime` | TIMESTAMP | Non | Horodatage de prise en charge du passager. |
| `dropoff_datetime` | TIMESTAMP | Non | Horodatage de dépose du passager. |
| `passenger_count` | INTEGER | Non | Nombre de passagers (valeur 1 par défaut si NULL). |
| `trip_distance_miles` | NUMERIC(10, 2) | Non | Distance parcourue en miles arrondie à 2 décimales. |
| `ratecode_id` | INTEGER | Oui | Code tarifaire TLC applicable. |
| `store_and_fwd_flag` | VARCHAR(1) | Non | Indicateur de stockage local ('Y' ou 'N', défaut 'N'). |
| `pu_location_id` | INTEGER | Non | Clé étrangère vers la zone de prise en charge (`dim_zone`). |
| `do_location_id` | INTEGER | Non | Clé étrangère vers la zone de dépose (`dim_zone`). |
| `payment_type_id` | INTEGER | Non | Clé étrangère vers le mode de règlement (valeur 5 = Inconnu si NULL). |
| `fare_amount` | NUMERIC(10, 2) | Non | Tarif de base (\$ USD). |
| `extra_amount` | NUMERIC(10, 2) | Non | Suppléments (\$ USD, défaut 0.00). |
| `mta_tax_amount` | NUMERIC(10, 2) | Non | Taxe métropolitaine MTA (\$ USD, défaut 0.00). |
| `tip_amount` | NUMERIC(10, 2) | Non | Montant du pourboire (\$ USD, défaut 0.00). |
| `tolls_amount` | NUMERIC(10, 2) | Non | Montant des péages (\$ USD, défaut 0.00). |
| `improvement_surcharge` | NUMERIC(10, 2) | Non | Surcharge d'amélioration du service (\$ USD, défaut 0.00). |
| `congestion_surcharge` | NUMERIC(10, 2) | Non | Taxe de congestion pour Manhattan (\$ USD, défaut 0.00). |
| `airport_fee` | NUMERIC(10, 2) | Non | Taxe aéroportuaire (\$ USD, défaut 0.00). |
| `total_amount` | NUMERIC(10, 2) | Non | Montant total facturé (\$ USD). |
| `duration_minutes` | NUMERIC(10, 2) | Non | Durée de la course en minutes. |
| `avg_speed_mph` | NUMERIC(10, 2) | Oui | Vitesse moyenne en miles par heure. |
| `is_airport_trip` | BOOLEAN | Non | Indicateur booléen signalant une origine ou destination aéroportuaire. |
| `partition_year` | INTEGER | Non | Année civile de partitionnement. |
| `partition_month_num` | INTEGER | Non | Numéro du mois de partitionnement (1 à 12). |
| `partition_month` | VARCHAR(7) | Non | Chaîne normalisée de la partition mensuelle ('YYYY-MM'). |

#### `stg_quarantine_trips`
Vue de staging standardisant les rejets de quarantaine et générant des drapeaux booléens analytiques par famille d'anomalie.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `reject_reason` | VARCHAR / TEXT | Non | Motif(s) d'échec aux règles qualité ('unspecified_error' si NULL). |
| `has_invalid_duration` | BOOLEAN | Non | Drapeau signalant une durée invalide (durée ≤ 0 ou > 6 h). |
| `has_invalid_distance` | BOOLEAN | Non | Drapeau signalant une distance invalide (distance ≤ 0 ou > 100 mi). |
| `has_invalid_amount` | BOOLEAN | Non | Drapeau signalant un montant total invalide (total_amount ≤ 0). |
| `has_timestamp_out_of_bounds` | BOOLEAN | Non | Drapeau signalant un horodatage pickup en dehors du mois du lot traité. |
| `has_invalid_zone` | BOOLEAN | Non | Drapeau signalant une zone de prise en charge ou dépose hors nomenclature 1-265. |
| `vendor_id` | INTEGER | Oui | Identifiant du fournisseur TPEP. |
| `pickup_datetime` | TIMESTAMP | Oui | Horodatage de début de course. |
| `dropoff_datetime` | TIMESTAMP | Oui | Horodatage de fin de course. |
| `passenger_count` | INTEGER | Oui | Nombre de passagers enregistré. |
| `trip_distance_miles` | NUMERIC(10, 2) | Oui | Distance enregistrée en miles. |
| `pu_location_id` | INTEGER | Oui | Identifiant de zone de départ brute. |
| `do_location_id` | INTEGER | Oui | Identifiant de zone d'arrivée brute. |
| `payment_type_id` | INTEGER | Oui | Identifiant brut du mode de paiement. |
| `fare_amount` | NUMERIC(10, 2) | Oui | Tarif de base (\$ USD). |
| `total_amount` | NUMERIC(10, 2) | Oui | Montant total (\$ USD). |
| `duration_minutes` | NUMERIC(10, 2) | Oui | Durée calculée à partir des timestamps bruts (NULL si horodatages manquants). |
| `partition_year` | INTEGER | Non | Année de la partition de rattachement. |
| `partition_month_num` | INTEGER | Non | Mois de la partition de rattachement (1 à 12). |
| `partition_month` | VARCHAR(7) | Non | Chaîne de partition standardisée ('YYYY-MM'). |

#### `stg_taxi_zones`
Vue de staging standardisant la nomenclature spatiale officielle des 265 zones de taxi de New York.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `zone_id` | INTEGER | Non | Clé primaire officielle de la zone TLC (`LocationID`, 1 à 265). |
| `borough` | VARCHAR | Non | Nom de l'arrondissement new-yorkais ('Unknown' si manquant). |
| `zone_name` | VARCHAR | Non | Nom officiel du quartier ou de la zone taxi ('Unknown Zone' si vide). |
| `service_zone` | VARCHAR | Non | Catégorie de zone TLC ('Yellow Zone', 'Boro Zone', 'Airports', 'EWR', 'N/A'). |
| `is_airport_zone` | BOOLEAN | Non | Indicateur identifiant les aéroports métropolitains majeurs (JFK, LGA, EWR). |

#### `stg_us_holidays`
Vue de staging des jours fériés légaux fédéraux aux États-Unis.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `holiday_date` | DATE | Non | Date civile du jour férié. |
| `holiday_name` | VARCHAR | Non | Intitulé officiel en anglais du jour férié fédéral (ex : 'Independence Day'). |
| `holiday_year` | INTEGER | Non | Année civile du jour férié. |
| `holiday_month` | INTEGER | Non | Numéro du mois (1 à 12). |
| `holiday_day` | INTEGER | Non | Numéro du jour dans le mois (1 à 31). |
| `day_of_week_name` | VARCHAR | Non | Nom du jour de la semaine en anglais ('Monday' à 'Sunday'). |
| `is_federal_holiday` | BOOLEAN | Non | Indicateur de jour férié fédéral officiel (toujours TRUE). |

#### `stg_nyc_weather`
Vue de staging standardisant les relevés météorologiques journaliers de New York (station Central Park / Open-Meteo).

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `date_day` | DATE | Non | Date civile de l'observation météorologique. |
| `date` | DATE | Non | Date civile de l'observation météorologique (alias miroir). |
| `temperature_max_c` | DOUBLE / NUMERIC | Oui | Température maximale journalière mesurée en degrés Celsius (°C). |
| `temperature_min_c` | DOUBLE / NUMERIC | Oui | Température minimale journalière mesurée en degrés Celsius (°C). |
| `precipitation_mm` | DOUBLE / NUMERIC | Oui | Hauteur de précipitations liquides cumulées sur la journée en millimètres (mm). |
| `snowfall_cm` | DOUBLE / NUMERIC | Oui | Hauteur de chutes de neige cumulées sur la journée en centimètres (cm). |

---

### Dimension Tables

#### `dim_date`
Dimension calendrier confluente au grain d'un jour civil, enrichie des jours fériés fédéraux américains et des observations météorologiques.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `date_id` | INTEGER | Non | Clé primaire numérique intelligente au format YYYYMMDD (ex : 20250115). |
| `date_day` | DATE | Non | Date civile standard (format YYYY-MM-DD). Unique. |
| `day_of_month` | INTEGER | Non | Numéro du jour dans le mois (1 à 31). |
| `month_number` | INTEGER | Non | Numéro du mois dans l'année (1 à 12). |
| `month_name` | VARCHAR | Non | Nom complet du mois en anglais ('January' à 'December'). |
| `year_number` | INTEGER | Non | Année civile (ex : 2025). |
| `quarter_number` | INTEGER | Non | Numéro du trimestre civil de l'année (1 à 4). |
| `day_of_week_number` | INTEGER | Non | Index du jour de la semaine (0 pour Dimanche, 6 pour Samedi). |
| `day_of_week_name` | VARCHAR | Non | Nom complet du jour de la semaine en anglais ('Sunday' à 'Saturday'). |
| `is_weekend` | BOOLEAN | Non | Indicateur de week-end (TRUE pour Samedi ou Dimanche, sinon FALSE). |
| `is_us_holiday` | BOOLEAN | Non | Indicateur désignant un jour férié officiel aux États-Unis. |
| `holiday_name` | VARCHAR | Oui | Intitulé officiel du jour férié américain (NULL si jour ouvré ordinaire). |
| `temperature_max_c` | DOUBLE / NUMERIC | Oui | Température maximale observée à Central Park (°C). |
| `temperature_min_c` | DOUBLE / NUMERIC | Oui | Température minimale observée à Central Park (°C). |
| `precipitation_mm` | DOUBLE / NUMERIC | Oui | Cumul des précipitations journalières à NYC (mm). |
| `snowfall_cm` | DOUBLE / NUMERIC | Oui | Cumul des chutes de neige journalières à NYC (cm). |

#### `dim_zone`
Dimension spatiale confluente modélisant les 265 zones géographiques officielles de la TLC avec typologie urbaine.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `zone_id` | INTEGER | Non | Clé primaire officielle TLC (`LocationID`, 1 à 265). Unique. |
| `zone_name` | VARCHAR | Non | Nom officiel ou usuel de la zone géographique / du quartier. |
| `borough` | VARCHAR | Non | Nom de l'arrondissement ('Manhattan', 'Queens', 'Brooklyn', 'Bronx', 'Staten Island', 'EWR', 'Unknown'). |
| `service_zone` | VARCHAR | Non | Classification administrative TLC ('Yellow Zone', 'Boro Zone', 'Airports', 'EWR', 'N/A'). |
| `is_airport_zone` | BOOLEAN | Non | Indicateur spécifiant si la zone est un aéroport commercial majeur. |
| `zone_category` | VARCHAR | Non | Macro-catégorie analytique : 'Core Urban' (Manhattan), 'Airport Hub', 'Outer Borough', 'Other'. |

#### `dim_payment_type`
Dimension confluente des modes de paiement avec codes techniques et libellés fonctionnels traduits en français.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `payment_type_id` | INTEGER | Non | Clé primaire correspondant au code numérique TLC (0 à 6). Unique. |
| `payment_code` | VARCHAR | Non | Code technique normalisé ('CREDIT_CARD', 'CASH', 'NO_CHARGE', 'DISPUTE', 'UNKNOWN', 'VOIDED_TRIP', 'UNKNOWN_ZERO'). |
| `payment_type_name` | VARCHAR | Non | Libellé métier explicite en français ('Carte bancaire', 'Espèces', 'Sans frais', 'Litige', 'Inconnu', 'Annulé'). |
| `description` | VARCHAR | Non | Description fonctionnelle détaillée de la modalité de paiement. |
| `is_electronic` | BOOLEAN | Non | Indicateur de paiement dématérialisé / électronique. |
| `is_cash` | BOOLEAN | Non | Indicateur de paiement direct en devises physiques (espèces). |

---

### Fact Tables

#### `fct_trips`
Table de faits transactionnelle centrale au grain élémentaire d'une course de taxi Yellow Taxi. Matérialisée de façon incrémentale avec stratégie `delete+insert` par partition mensuelle pour garantir une idempotence totale.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `trip_id` | VARCHAR / TEXT | Non | Clé primaire unique de la course (empreinte SHA-256 issue de la couche Silver). Unique. |
| `pickup_date_id` | INTEGER | Non | Clé étrangère vers `dim_date.date_id` pour la date de prise en charge (YYYYMMDD). |
| `dropoff_date_id` | INTEGER | Non | Clé étrangère vers `dim_date.date_id` pour la date de dépose (YYYYMMDD). |
| `pickup_hour` | INTEGER | Non | Heure d'enclenchement de la course (0 à 23). |
| `dropoff_hour` | INTEGER | Non | Heure de fin de course (0 à 23). |
| `pickup_zone_id` | INTEGER | Non | Clé étrangère vers `dim_zone.zone_id` indiquant la zone de départ. |
| `dropoff_zone_id` | INTEGER | Non | Clé étrangère vers `dim_zone.zone_id` indiquant la zone de dépose. |
| `payment_type_id` | INTEGER | Non | Clé étrangère vers `dim_payment_type.payment_type_id`. |
| `vendor_id` | INTEGER | Non | Identifiant du fournisseur de technologie TPEP (1: Creative Mobile, 2: VeriFone). |
| `ratecode_id` | INTEGER | Oui | Code tarifaire final appliqué au trajet. |
| `pickup_datetime` | TIMESTAMP | Non | Horodatage précis de début de course. |
| `dropoff_datetime` | TIMESTAMP | Non | Horodatage précis de fin de course. |
| `store_and_fwd_flag` | VARCHAR(1) | Oui | Indicateur technique de transmission différée ('Y' ou 'N'). |
| `is_airport_trip` | BOOLEAN | Non | Indicateur signalant une origine ou une destination aéroportuaire. |
| `passenger_count` | INTEGER | Non | Nombre de passagers enregistrés (au minimum 1). |
| `trip_distance_miles` | NUMERIC(10, 2) | Non | Distance parcourue en miles selon le taximètre (strictement positive, ≤ 100 mi). |
| `duration_minutes` | NUMERIC(10, 2) | Non | Durée totale de la course en minutes (≤ 360 min). |
| `avg_speed_mph` | NUMERIC(10, 2) | Oui | Vitesse moyenne estimée en miles par heure. |
| `fare_amount` | NUMERIC(10, 2) | Non | Montant du tarif de base au temps et à la distance (\$ USD). |
| `extra_amount` | NUMERIC(10, 2) | Non | Suppléments divers (heures de pointe, nuit) (\$ USD). |
| `mta_tax_amount` | NUMERIC(10, 2) | Non | Taxe métropolitaine MTA de 0.50 \$ (\$ USD). |
| `tip_amount` | NUMERIC(10, 2) | Non | Montant du pourboire (\$ USD). |
| `tolls_amount` | NUMERIC(10, 2) | Non | Frais de péages de ponts et tunnels (\$ USD). |
| `improvement_surcharge` | NUMERIC(10, 2) | Non | Taxe d'amélioration du service (\$ USD). |
| `congestion_surcharge` | NUMERIC(10, 2) | Non | Taxe de congestion pour circulation au sud de Manhattan (\$ USD). |
| `airport_fee` | NUMERIC(10, 2) | Non | Taxe d'accès aux aéroports JFK et LGA (\$ USD). |
| `total_amount` | NUMERIC(10, 2) | Non | Montant total débité au passager (\$ USD, strictement positif). |
| `tip_percentage` | NUMERIC(10, 2) | Non | Ratio du pourboire rapporté au tarif de base : `round(tip_amount * 100.0 / fare_amount, 2)` (en %). |
| `partition_year` | INTEGER | Non | Année de partitionnement. |
| `partition_month_num` | INTEGER | Non | Mois de partitionnement (1 à 12). |
| `partition_month` | VARCHAR(7) | Non | Partition mensuelle normalisée au format 'YYYY-MM'. |

---

### Analytical Marts

#### `agg_zone_hourly`
Table d'agrégat précalculée au grain 'zone de prise en charge × date civile × heure de la journée'. Optimisée pour l'accélération des tableaux de bord Metabase et des requêtes analytiques sur les flux de mobilité.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `pickup_zone_id` | INTEGER | Non | Clé étrangère vers `dim_zone.zone_id` indiquant la zone de départ. |
| `pickup_date` | DATE | Non | Date civile de la prise en charge. |
| `pickup_hour` | INTEGER | Non | Heure de la journée (0 à 23). |
| `trip_count` | BIGINT | Non | Nombre total de courses enregistrées sur ce créneau et cette zone. |
| `total_passengers` | BIGINT | Oui | Volume cumulé de passagers transportés. |
| `airport_trip_count` | BIGINT | Oui | Nombre de trajets desservant un aéroport. |
| `total_revenue` | NUMERIC(12, 2) | Non | Chiffre d'affaires cumulé total généré (\$ USD). |
| `total_fare` | NUMERIC(12, 2) | Oui | Somme cumulée des tarifs de base (\$ USD). |
| `total_tips` | NUMERIC(12, 2) | Oui | Somme cumulée des pourboires collectés (\$ USD). |
| `avg_tip_amount` | NUMERIC(10, 2) | Oui | Montant moyen du pourboire par course (\$ USD). |
| `avg_fare_amount` | NUMERIC(10, 2) | Oui | Tarif de base moyen par course (\$ USD). |
| `avg_trip_distance_miles` | NUMERIC(10, 2) | Oui | Distance moyenne parcourue par course (en miles). |
| `avg_duration_minutes` | NUMERIC(10, 2) | Oui | Durée moyenne de trajet (en minutes). |
| `avg_speed_mph` | NUMERIC(10, 2) | Oui | Vitesse moyenne estimée (en miles par heure). |

#### `quarantine_summary`
Vue d'observabilité et de monitoring de la qualité des données agrégeant les rejets en quarantaine au grain 'motif de rejet × mois de partition'.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `partition_month` | VARCHAR(7) | Non | Mois de partition concerné (format 'YYYY-MM'). |
| `reject_reason` | VARCHAR / TEXT | Non | Motif spécifique ou combinaison de règles qualité violées (séparées par `\|`). |
| `rejected_records_count` | BIGINT | Non | Nombre d'enregistrements rejetés pour ce motif précis. |
| `total_month_rejections` | BIGINT | Non | Nombre total d'enregistrements rejetés sur l'ensemble du mois. |
| `rejection_share_pct` | NUMERIC(6, 2) | Oui | Part en pourcentage représentée par ce motif sur l'ensemble des rejets du mois (en %). |
| `avg_invalid_duration_minutes` | NUMERIC(10, 2) | Oui | Durée moyenne des courses rejetées pour ce motif (en minutes). |
| `avg_invalid_distance_miles` | NUMERIC(10, 2) | Oui | Distance moyenne des courses rejetées pour ce motif (en miles). |
| `avg_invalid_total_amount` | NUMERIC(10, 2) | Oui | Montant total moyen des courses rejetées pour ce motif (\$ USD). |
| `min_pickup_datetime` | TIMESTAMP | Oui | Horodatage de prise en charge le plus ancien parmi les courses rejetées. |
| `max_pickup_datetime` | TIMESTAMP | Oui | Horodatage de prise en charge le plus récent parmi les courses rejetées. |

---

## 📡 Table d'Observabilité

### `pipeline_runs` (PostgreSQL DWH)
Table technique centrale de traçabilité DataOps hébergée dans PostgreSQL (`taxi-postgres`). Elle journalise chaque exécution de pipeline, les volumes traités, les taux de rejets en quarantaine et le respect des SLAs.

| Colonne | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `run_id` | VARCHAR(255) | Non | Clé primaire unique de l'exécution Airflow (DagRun ID ou Task Run ID). |
| `dag_id` | VARCHAR(100) | Non | Identifiant du Directed Acyclic Graph Airflow (ex : `taxi_lakehouse_pipeline`). |
| `task_id` | VARCHAR(100) | Oui | Identifiant de la tâche spécifique Airflow exécutée (ex : `spark_bronze_to_silver`). |
| `logical_date` | TIMESTAMP WITH TIME ZONE | Non | Date logique d'exécution planifiée Airflow (logical execution date). |
| `execution_month` | VARCHAR(7) | Non | Mois de partition ciblé par le traitement batch (format 'YYYY-MM'). |
| `status` | VARCHAR(50) | Non | Statut final ou en cours de l'exécution ('SUCCESS', 'FAILED', 'RUNNING', 'RETRY'). |
| `duration_seconds` | NUMERIC(10, 2) | Oui | Durée d'exécution du traitement en secondes (valeur par défaut : 0.0). |
| `records_processed` | BIGINT | Oui | Nombre total de lignes ingérées ou traitées par la tâche (valeur par défaut : 0). |
| `records_rejected` | BIGINT | Oui | Nombre d'enregistrements rejetés et routés vers la quarantaine (valeur par défaut : 0). |
| `rejection_rate` | NUMERIC(6, 4) | Oui | Fraction d'enregistrements rejetés (`records_rejected / records_processed`, ex : 0.0425 pour 4.25%). |
| `error_message` | TEXT | Oui | Message d'erreur textuel ou stack trace en cas d'incident ou d'échec. |
| `created_at` | TIMESTAMP WITH TIME ZONE | Oui | Horodatage de création de la ligne (défaut : `CURRENT_TIMESTAMP`). |
| `updated_at` | TIMESTAMP WITH TIME ZONE | Oui | Horodatage de dernière mise à jour de la ligne (défaut : `CURRENT_TIMESTAMP`). |

---

## 🔗 Règles de Validation Qualité (Bronze → Silver)

Lors du passage de la couche Bronze à la couche Silver dans le job Spark `bronze_to_silver.py`, la fonction pure `apply_quality_rules` évalue chaque ligne contre 5 règles d'intégrité métier strictes. Les lignes en échec sont immédiatement routées vers la couche **Quarantine** avec l'ensemble des motifs violés concaténés dans la colonne `reject_reason`.

| Règle | Code d'erreur | Condition de rejet | Seuil métier / Justification |
| :--- | :--- | :--- | :--- |
| **R1** | `invalid_duration` | `duration_s IS NULL OR duration_s <= 0 OR duration_s > 21600` | La durée doit être > 0 et ≤ 6 heures (21 600 s). Élimine les courses instantanées ou les compteurs non arrêtés. |
| **R2** | `invalid_distance` | `trip_distance IS NULL OR trip_distance <= 0 OR trip_distance > 100.0` | La distance doit être > 0 mi et ≤ 100 mi. Élimine les anomalies de capteurs odométriques. |
| **R3** | `invalid_total_amount` | `total_amount IS NULL OR total_amount <= 0` | Le montant total doit être > 0.00 \$. Élimine les soldes nuls ou négatifs. |
| **R4** | `pickup_outside_month` | `pickup < month_start OR pickup >= month_end` | L'horodatage de prise en charge doit être dans l'intervalle `[YYYY-MM-01, next_month-01[`. |
| **R5** | `invalid_zone_id` | `pu_location_id NOT IN (1..265) OR do_location_id NOT IN (1..265)` | Les zones doivent appartenir à la nomenclature officielle des 265 zones TLC. |

> **Note :** Lorsqu'une ligne viole simultanément plusieurs règles, les codes sont chaînés avec le délimiteur pipe (ex : `invalid_duration|invalid_total_amount`). Seules les lignes avec `reject_reason = ""` accèdent à la couche Silver.

---

## 🔑 Conventions de Nommage

1. **Casse et style :** Format `snake_case` strict en minuscules. Les noms CamelCase de la source TLC (`VendorID`, `PULocationID`) sont renommés dès la couche Silver.

2. **Préfixes Kimball (dbt) :**
   - `stg_` : Vues de staging (nettoyage syntaxique, casting strict)
   - `dim_` : Tables de dimensions confluentes (grain = 1 entité descriptive)
   - `fct_` : Tables de faits transactionnelles (grain = 1 événement/trajet)
   - `agg_` : Tables d'agrégats précalculés

3. **Suffixes sémantiques :**
   - `_id` : Clés primaires et étrangères
   - `_datetime` : Horodatages précis (date + heure)
   - `_date` / `_day` : Dates civiles (sans heure)
   - `_hour` : Heure entière (0–23)
   - `_amount` / `_fee` / `_tax` / `_surcharge` : Montants monétaires (\$ USD)
   - `_miles` / `_minutes` / `_mph` / `_mm` / `_cm` / `_c` : Métriques physiques avec unité
   - `is_` / `has_` : Indicateurs booléens (TRUE/FALSE)
   - `_count` / `_num` : Dénombrements
   - `_pct` / `_rate` : Ratios et pourcentages
