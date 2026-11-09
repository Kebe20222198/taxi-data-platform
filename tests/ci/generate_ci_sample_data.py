#!/usr/bin/env python3
"""
tests/ci/generate_ci_sample_data.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Generates an ephemeral lightweight DuckDB database containing sample
data for CI validation of the dbt analytics layer (models + quality tests).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import duckdb

CI_DIR = Path(__file__).resolve().parent
DB_PATH = CI_DIR / "ci_warehouse.duckdb"


def generate_ci_database(db_file: Path = DB_PATH) -> None:
    """Populate DuckDB database with synthetic valid and quarantined taxi records."""
    if db_file.exists():
        db_file.unlink()

    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_file))

    print(f"Creating CI database at {db_file}...")

    # 1. Taxi Zones Reference
    conn.execute("""
        CREATE TABLE taxi_zones (
            LocationID INTEGER,
            Borough VARCHAR,
            Zone VARCHAR,
            service_zone VARCHAR
        );
        INSERT INTO taxi_zones VALUES
            (1, 'EWR', 'Newark Airport', 'EWR'),
            (132, 'Queens', 'JFK Airport', 'Airports'),
            (138, 'Queens', 'LaGuardia Airport', 'Airports'),
            (161, 'Manhattan', 'Midtown Center', 'Yellow Zone'),
            (230, 'Manhattan', 'Times Sq/Theatre District', 'Yellow Zone'),
            (237, 'Manhattan', 'Upper East Side South', 'Yellow Zone'),
            (142, 'Manhattan', 'Lincoln Square East', 'Yellow Zone'),
            (61, 'Brooklyn', 'Crown Heights North', 'Boro Zone');
    """)

    # 2. US Holidays Reference (2025)
    conn.execute("""
        CREATE TABLE us_holidays (
            date DATE,
            holiday_name VARCHAR,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week VARCHAR
        );
        INSERT INTO us_holidays VALUES
            ('2025-01-01', 'New Year''s Day', 2025, 1, 1, 'Wednesday'),
            ('2025-01-20', 'Martin Luther King Jr. Day', 2025, 1, 20, 'Monday'),
            ('2025-02-17', 'Washington''s Birthday', 2025, 2, 17, 'Monday'),
            ('2025-05-26', 'Memorial Day', 2025, 5, 26, 'Monday'),
            ('2025-06-19', 'Juneteenth National Independence Day', 2025, 6, 19, 'Thursday'),
            ('2025-07-04', 'Independence Day', 2025, 7, 4, 'Friday'),
            ('2025-09-01', 'Labor Day', 2025, 9, 1, 'Monday'),
            ('2025-11-27', 'Thanksgiving Day', 2025, 11, 27, 'Thursday'),
            ('2025-12-25', 'Christmas Day', 2025, 12, 25, 'Thursday');
    """)

    # 3. NYC Weather Reference (2025)
    conn.execute("""
        CREATE TABLE nyc_weather (
            time DATE,
            temperature_2m_max DOUBLE,
            temperature_2m_min DOUBLE,
            precipitation_sum DOUBLE,
            snowfall_sum DOUBLE
        );
        INSERT INTO nyc_weather VALUES
            ('2025-01-01', 11.0, 3.4, 1.1, 0.0),
            ('2025-01-15', 3.5, -2.1, 0.0, 0.0),
            ('2025-01-16', 4.2, -1.0, 2.5, 0.5);
    """)

    # 4. Clean Silver Trips (2025 Sample matching YELLOW_TAXI_SILVER_SCHEMA)
    conn.execute("""
        CREATE TABLE yellow_trips (
            trip_hash VARCHAR,
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            ratecode_id INTEGER,
            store_and_fwd_flag VARCHAR,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            congestion_surcharge DOUBLE,
            airport_fee DOUBLE,
            total_amount DOUBLE,
            duration_minutes DOUBLE,
            avg_speed_mph DOUBLE,
            is_airport_trip BOOLEAN,
            year INTEGER,
            month INTEGER
        );
        INSERT INTO yellow_trips VALUES
            -- Valid trip JFK -> Midtown (Credit Card)
            ('hash_jfk_midtown_01', 2, '2025-01-15 10:00:00', '2025-01-15 10:45:00', 1, 18.5, 2, 'N', 132, 161, 1, 52.0, 0.0, 0.5, 10.0, 6.55, 1.0, 2.5, 1.25, 73.8, 45.0, 24.67, true, 2025, 1),
            -- Valid trip Times Square -> Lincoln Square (Cash)
            ('hash_times_lincoln_02', 1, '2025-01-15 14:00:00', '2025-01-15 14:15:00', 2, 1.8, 1, 'N', 230, 142, 2, 9.5, 0.0, 0.5, 0.0, 0.0, 1.0, 2.5, 0.0, 13.5, 15.0, 7.20, false, 2025, 1),
            -- Valid trip Midtown -> LGA (Credit Card)
            ('hash_midtown_lga_03', 2, '2025-01-15 18:00:00', '2025-01-15 18:40:00', 1, 10.2, 1, 'N', 161, 138, 1, 38.0, 2.5, 0.5, 8.5, 6.55, 1.0, 2.5, 1.25, 60.8, 40.0, 15.30, true, 2025, 1),
            -- Valid trip Upper East Side -> Midtown (Credit Card)
            ('hash_ues_midtown_04', 1, '2025-01-16 08:30:00', '2025-01-16 08:48:00', 1, 2.4, 1, 'N', 237, 161, 1, 12.0, 0.0, 0.5, 2.5, 0.0, 1.0, 2.5, 0.0, 18.5, 18.0, 8.00, false, 2025, 1),
            -- Valid trip Brooklyn -> Midtown (Credit Card)
            ('hash_bk_midtown_05', 2, '2025-01-16 09:00:00', '2025-01-16 09:35:00', 3, 7.8, 1, 'N', 61, 161, 1, 28.0, 0.0, 0.5, 5.0, 0.0, 1.0, 2.5, 0.0, 37.0, 35.0, 13.37, false, 2025, 1);
    """)

    # 5. Quarantine Trips (2025 Sample invalid trips for quality monitoring tests)
    conn.execute("""
        CREATE TABLE yellow_quarantine (
            reject_reason VARCHAR,
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            total_amount DOUBLE,
            year INTEGER,
            month INTEGER
        );
        INSERT INTO yellow_quarantine VALUES
            ('invalid_duration', 1, '2025-01-15 11:00:00', '2025-01-15 10:59:00', 1, 2.5, 161, 230, 1, 10.0, 12.0, 2025, 1),
            ('invalid_distance', 2, '2025-01-15 12:00:00', '2025-01-15 12:20:00', 1, 0.0, 161, 161, 2, 8.0, 9.5, 2025, 1);
    """)

    conn.close()
    print("✅ CI DuckDB database created with clean 2025 sample data.")


if __name__ == "__main__":
    generate_ci_database()
