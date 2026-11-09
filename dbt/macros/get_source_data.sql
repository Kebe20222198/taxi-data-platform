{# ==============================================================================
   NYC Taxi Data Platform — Dual-Engine Source Abstraction Macro
   Allows seamless execution on either DuckDB (scanning MinIO Delta Lake)
   or PostgreSQL (DWH tables) without changing model business logic.
============================================================================== #}

{% macro source_silver_yellow() %}
  {% if env_var('CI_DBT', 'false') == 'true' or target.type == 'postgres' %}
    {{ source('raw_silver', 'yellow_trips') }}
  {% elif target.type == 'duckdb' %}
    delta_scan('s3://silver/yellow/')
  {% else %}
    {{ source('raw_silver', 'yellow_trips') }}
  {% endif %}
{% endmacro %}

{% macro source_quarantine_yellow() %}
  {% if env_var('CI_DBT', 'false') == 'true' or target.type == 'postgres' %}
    {{ source('raw_quarantine', 'yellow_quarantine') }}
  {% elif target.type == 'duckdb' %}
    read_parquet('s3://quarantine/yellow/*/*/*.parquet', hive_partitioning=1)
  {% else %}
    {{ source('raw_quarantine', 'yellow_quarantine') }}
  {% endif %}
{% endmacro %}

{% macro source_ref_taxi_zones() %}
  {% if env_var('CI_DBT', 'false') == 'true' or target.type == 'postgres' %}
    {{ source('raw_reference', 'taxi_zones') }}
  {% elif target.type == 'duckdb' %}
    read_csv('s3://bronze/reference/taxi_zone_lookup.csv', header=true)
  {% else %}
    {{ source('raw_reference', 'taxi_zones') }}
  {% endif %}
{% endmacro %}

{% macro source_ref_us_holidays() %}
  {% if env_var('CI_DBT', 'false') == 'true' or target.type == 'postgres' %}
    {{ source('raw_reference', 'us_holidays') }}
  {% elif target.type == 'duckdb' %}
    read_csv('s3://bronze/reference/us_public_holidays.csv', header=true)
  {% else %}
    {{ source('raw_reference', 'us_holidays') }}
  {% endif %}
{% endmacro %}

{% macro source_ref_weather() %}
  {% if env_var('CI_DBT', 'false') == 'true' or target.type == 'postgres' %}
    {{ source('raw_reference', 'nyc_weather') }}
  {% elif target.type == 'duckdb' %}
    read_csv('s3://bronze/reference/nyc_weather_2025.csv', header=true)
  {% else %}
    {{ source('raw_reference', 'nyc_weather') }}
  {% endif %}
{% endmacro %}

