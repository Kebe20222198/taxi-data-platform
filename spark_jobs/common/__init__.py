"""
spark_jobs.common
~~~~~~~~~~~~~~~~~
Modules partagés pour les jobs Spark du pipeline NYC Taxi.

- ``constants``       — Constantes métier et chemins S3A
- ``schemas``         — StructType explicites (Bronze + Silver)
- ``spark_factory``   — Factory SparkSession (Delta + S3A)
- ``transformations`` — Fonctions pures de nettoyage / enrichissement
"""

from spark_jobs.common.constants import *  # noqa: F401, F403
from spark_jobs.common.schemas import (  # noqa: F401
    YELLOW_TAXI_RAW_SCHEMA,
    YELLOW_TAXI_SILVER_SCHEMA,
)
from spark_jobs.common.spark_factory import create_spark_session  # noqa: F401
from spark_jobs.common.transformations import (  # noqa: F401
    add_derived_columns,
    add_partition_columns,
    apply_quality_rules,
    cast_types,
    deduplicate,
    rename_columns,
)
