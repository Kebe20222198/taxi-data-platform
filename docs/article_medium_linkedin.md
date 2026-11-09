# Comment Traiter 40 Millions de Lignes sur un PC de 16 Go de RAM : Retour d'Expérience d'un Staff Data Engineer

*Paru dans "Engineering at Scale" — Temps de lecture : 7 minutes*

---

> *"Spark plante avec un Out-Of-Memory !"*  
> *"La machine gèle complètement à la jointure !"*  
> *"Une seule tâche tourne pendant 40 secondes pendant que les 3 autres cœurs se tournent les pouces !"*

Si vous avez déjà essayé de faire tourner un pipeline distribué sur un jeu de données volumineux en local, vous avez très certainement rencontré ces symptômes. 

Récemment, j'ai relevé le défi de bâtir une **plateforme Lakehouse de bout en bout** capable de traiter l'intégralité des courses annuelles de la flotte **NYC Yellow Taxi** — soit **35 à 40 millions d'enregistrements** (plusieurs dizaines de gigaoctets décompressés) — sur une simple machine de développement de **16 Go de RAM et 4 cœurs CPU**.

La stack technique ? **MinIO (S3)**, **Apache Spark 3.5**, **Delta Lake 3.1**, **Apache Airflow 3**, **dbt Core** et **Streamlit**.

Au premier essai, le pipeline naïf s'est exécuté en **420 secondes** (7 minutes), avec un débordement disque violent de 650 Mo et des alertes de saturation mémoire. En appliquant une méthode d'optimisation expérimentale rigoureuse, nous avons fait chuter ce temps d'exécution à **105 secondes** (**-75% de temps de cycle, soit un speedup de 4.0x**) tout en éliminant 100% du spill disque.

Voici le récit technique de cette optimisation, les 3 pièges redoutables dans lesquels nous sommes tombés, et comment nous les avons résolus.

---

## 🕳️ Piège #1 : Le cauchemar du Data Skew sur l'Aéroport JFK (La Tâche Traînarde)

Dans le monde réel, les données ne sont jamais réparties de manière uniforme. Les trajets de taxis new-yorkais suivent une loi de puissance (Zipfian distribution) impitoyable : **l'Aéroport International JFK (Zone 132)** et le quartier de **Midtown / Times Square (Zones 161 et 230)** concentrent à eux seuls plus de 25% du volume global de la mégapole.

### Ce qui se passe physiquement dans Spark :
Lorsqu'on effectue une agrégation par zone de départ (`groupBy("pu_location_id")`), Spark applique une fonction de hachage modulo le nombre de partitions de shuffle :

$$\text{partition\_id} = \text{hash}(\text{pu\_location\_id}) \pmod N$$

Conséquence ? Tous les millions de trajets ayant pour origine JFK atterrissent sur **le même réducteur**. 

Sur la Spark UI, le constat était flagrant : **3 de nos cœurs terminaient leur partition en 2 secondes chrono**, tandis que **le 4e cœur continuait de mouliner pendant 48 secondes** à lui seul. Pire encore : la mémoire vive allouée à cet exécuteur saturait, forçant Spark à vider les buffers sur le SSD (**650 Mo de Disk Spill**).

```
Stage Timeline (Avant) :
Core 0 : [== 2s ==] (Idle...)
Core 1 : [== 2s ==] (Idle...)
Core 2 : [== 2s ==] (Idle...)
Core 3 : [============================================== 48s Straggler !! (Spill)]
```

### Le Remède : La Technique du "Key Salting"
Puisque le partitionneur ne sait pas que JFK est surchargé, nous lui avons forcé la main en ajoutant un **sel aléatoire** (de 0 à 3) sur les clés critiques :

```python
# 1. On sale artificiellement la clé des zones asymétriques
SALT_FACTOR = 4
df_salted = df.withColumn(
    "salt",
    F.when(F.col("pu_location_id").isin([132, 138, 161, 230]), F.floor(F.rand() * SALT_FACTOR))
     .otherwise(F.lit(0))
).withColumn("salted_key", F.concat_ws("_", "pu_location_id", "salt"))
```

Le volume massif de JFK a ainsi été divisé en 4 flux indépendants et réparti équitablement sur nos 4 cœurs vCPU. En répliquant la table de dimension sur ces 4 clés, la durée de la tâche la plus lente est tombée de **48 secondes à 14 secondes**, et le Disk Spill a été réduit à **zéro**.

---

## 🕳️ Piège #2 : Le "Small Files Problem" dans Delta Lake

Par défaut, Spark initialise `spark.sql.shuffle.partitions = 200`. Si vous écrivez vos tables partitionnées par mois (`year=YYYY/month=MM/`) avec ce paramètre, chaque tâche va ouvrir et écrire un micro-fichier Parquet dans chaque répertoire de partition.

Résultat ? Après un an de données, nous nous sommes retrouvés avec **des milliers de fichiers de 200 Ko à 1 Mo**.

### Pourquoi c'est une catastrophe :
1. **Saturation des métadonnées MinIO** : Chaque lecture ultérieure (par dbt ou Spark) nécessite des milliers d'appels HTTP `HEAD` et `GET` pour lire les métadonnées de fichiers.
2. **Perte de l'avantage vectorisé de Parquet** : La compression Snappy et le saut de blocs (dictionary page pruning) ne fonctionnent efficacement que sur des fichiers denses d'au moins **128 Mo**.

### Le Remède :
Avant l'écriture finale dans MinIO, nous avons injecté un compactage ciblé en mémoire :

```python
# coalesce(4) regroupe les partitions sans déclencher de shuffle réseau
silver_writer = df_silver.coalesce(4)
silver_writer.write.format("delta").mode("overwrite").partitionBy("year", "month").save(path)
```

Nous sommes passés de **plus de 40 micro-fichiers par mois à exactement 4 fichiers Parquet optimisés de ~150 Mo**. La vitesse de lecture pour nos modèles dbt en a été multipliée par trois.

---

## 🕳️ Piège #3 : La Guerre Mémoire (Unified Memory Manager & Lineage)

Notre job de production doit alimenter trois destinations distinctes :
1. Écrire les rejets en **Quarantaine** (`s3a://quarantine/`).
2. Écrire les trajets assainis et dédupliqués en **Silver** (`s3a://silver/`).
3. Calculer les pré-agrégats financiers pour la couche **Gold** (`s3a://gold/`).

Dans la version naïve, sans mise en cache, que croyez-vous que Spark ait fait ?  
**Il a recalculé l'intégralité de la lignée de transformation 3 fois de suite depuis MinIO.**

Chaque action (`.write`) relisait les données brutes, ré-exécutait le cast de types, les 5 règles de qualité et l'empreinte cryptographique SHA-256 de déduplication.

### Le Remède :
Nous avons matérialisé en mémoire le DataFrame nettoyé juste avant la bifurcation du graphe :

```python
df_final_silver = df_final_silver.persist(StorageLevel.MEMORY_AND_DISK)
```

Grâce au format binaire natif de **Project Tungsten** (`UnsafeRow`), les 35 millions de lignes nettoyées sont restées logées dans le pool de mémoire de stockage de Spark. Les écritures Silver et le calcul Gold n'ont nécessité **zéro E/S disque supplémentaire**.

---

## 📊 La Méthode Expérimentale en 8 Paliers : Les Chiffres

Pour mesurer scientifiquement le gain de chaque décision d'architecture, nous avons conçu un banc de test automatisé qui exécute **3 fois consécutives** chaque configuration sur les 12 mois complets pour en extraire la médiane :

| Niveau | Optimisation Activée | Durée Médiane | Gain Relatif | Gain Cumulé | Shuffle Réseau |
| :---: | :--- | :---: | :---: | :---: | :---: |
| **Niv 0** | Baseline Naïve (`inferSchema`, SMJ, 200 parts) | **420s** | — | **0.0%** | 3 800 Mo |
| **Niv 1** | + Schéma Explicite (`StructType`) | **350s** | -16.7% | -16.7% | 3 800 Mo |
| **Niv 2** | + Partition Pruning direct | **310s** | -11.4% | -26.2% | 3 650 Mo |
| **Niv 3** | + BroadcastHashJoin (table zones 265 lignes) | **240s** | -22.5% | -42.8% | 1 900 Mo |
| **Niv 4** | + Tuning Shuffle (`partitions=8` pour 4 cœurs) | **195s** | -18.7% | -53.5% | 1 750 Mo |
| **Niv 5** | + Traitement du Skew (Key Salting JFK/Midtown) | **165s** | -15.4% | -60.7% | 1 620 Mo |
| **Niv 6** | + Caching Nettoyé (`persist MEMORY_AND_DISK`) | **135s** | -18.2% | -67.8% | 950 Mo |
| **Niv 7** | + Compactage Sortie (`coalesce(4)`) | **120s** | -11.1% | -71.4% | 950 Mo |
| **Niv 8** | + Adaptive Query Execution (AQE complet) | **105s** | -12.5% | **-75.0%** | **820 Mo** |

```
Résultat Final :
⚡ Vitesse multipliée par 4.0x (de 7 minutes à 1 min 45)
💾 Shuffle réseau divisé par 4.6x
🧹 Débordement disque : 0 Mo
```

---

## 💡 Ce qu'il Faut Retenir (Key Takeaways)

1. **Ne laissez jamais `inferSchema=True` en production** : spécifier un `StructType` strict a supprimé à lui seul 70 secondes de requêtes métadonnées stériles sur MinIO.
2. **Vérifiez systématiquement la taille des tables jointes** : diffuser une dimension de 265 lignes via `F.broadcast()` a éliminé 2 Go de shuffle réseau inutile.
3. **Surveillez l'asymétrie de vos clés (Data Skew)** : si une tâche traîne pendant que les autres dorment, appliquez le Key Salting ou activez l'Adaptive Query Execution (`spark.sql.adaptive.skewJoin.enabled = true`).
4. **Adoptez l'architecture Médaillon et l'idempotence stricte** : notre clause `replaceWhere` sur Delta Lake et la stratégie `delete+insert` sur dbt garantissent que n'importe quel mois peut être recalculé sans jamais corrompre le reste du Lakehouse.

Le code complet, les Dockerfiles et le pipeline dbt sont disponibles en open source sur le dépôt [GitHub du projet](https://github.com/Kebe20222198/taxi-data-platform).

---
*Vous optimisez des pipelines Spark distribués ? Quelles sont vos techniques favorites pour traquer les stragglers ? Discutons-en dans les commentaires !*
