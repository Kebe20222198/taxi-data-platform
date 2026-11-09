"""
tests/unit/test_dag_integrity.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests validating DAG structure, dependencies, parameters,
and quality check gate logic for taxi_monthly.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest


def test_dag_loaded():
    """Verify that the taxi_monthly DAG is successfully loaded with correct configurations."""
    try:
        from airflow.dags.taxi_monthly import dag
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    assert dag is not None
    assert dag.dag_id == "taxi_monthly"
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    # Schedule check (Airflow 3 schedule parameter)
    assert str(dag.schedule) == "0 0 5 * *"


def test_dag_default_args():
    """Verify default_args on the DAG (retries=2, retry_delay=5min, failure callback)."""
    try:
        from airflow.dags.taxi_monthly import dag, pipeline_failure_callback
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    assert dag.default_args.get("retries") == 2
    assert dag.default_args.get("retry_delay") == timedelta(minutes=5)
    assert dag.default_args.get("on_failure_callback") is pipeline_failure_callback


def test_dag_task_sequence():
    """Verify the strict linear task sequence:
    ingest -> bronze_to_silver -> quality_checks_silver -> silver_to_gold -> dbt_run -> dbt_test -> notify
    """
    try:
        from airflow.dags.taxi_monthly import dag
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    expected_tasks = {
        "ingest",
        "bronze_to_silver",
        "quality_checks_silver",
        "silver_to_gold",
        "dbt_run",
        "dbt_test",
        "notify",
    }
    assert set(dag.task_dict.keys()) == expected_tasks

    assert dag.get_task("ingest").downstream_task_ids == {"bronze_to_silver"}
    assert dag.get_task("bronze_to_silver").downstream_task_ids == {"quality_checks_silver"}
    assert dag.get_task("quality_checks_silver").downstream_task_ids == {"silver_to_gold"}
    assert dag.get_task("silver_to_gold").downstream_task_ids == {"dbt_run"}
    assert dag.get_task("dbt_run").downstream_task_ids == {"dbt_test"}
    assert dag.get_task("dbt_test").downstream_task_ids == {"notify"}
    assert dag.get_task("notify").downstream_task_ids == set()


def test_dag_idempotent_templating():
    """Verify that tasks receive the dynamic execution date {{ logical_date.strftime('%Y-%m') }}."""
    try:
        from airflow.dags.taxi_monthly import dag
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    date_template = "{{ logical_date.strftime('%Y-%m') }}"
    assert date_template in dag.get_task("ingest").bash_command
    assert date_template in dag.get_task("bronze_to_silver").bash_command
    assert date_template in dag.get_task("silver_to_gold").bash_command
    assert dag.get_task("quality_checks_silver").op_kwargs["month"] == date_template


def test_quality_checks_silver_passes_nominal():
    """Verify that quality check passes when partition is non-empty and rejection rate < 20%."""
    try:
        from airflow.dags.taxi_monthly import check_silver_quality
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    mock_ti = MagicMock()
    # 1,000,000 valid rows, 1,000 quarantined (~0.1% rejection rate)
    mock_ti.xcom_pull.return_value = {
        "silver_count": 1000000,
        "rejected_count": 1000,
    }

    result = check_silver_quality(
        month="2025-01",
        task_instance=mock_ti,
        logical_date=datetime(2025, 1, 5),
    )

    assert result["passed"] is True
    assert result["rejection_rate"] < 0.20
    assert result["silver_records"] == 1000000


def test_quality_checks_silver_fails_on_empty_partition():
    """Verify that quality check raises AirflowException if silver partition is empty."""
    try:
        from airflow.dags.taxi_monthly import check_silver_quality
        from airflow.exceptions import AirflowException
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = {
        "silver_count": 0,
        "rejected_count": 50,
    }

    with pytest.raises(AirflowException, match="Silver partition 2025-01 is EMPTY"):
        check_silver_quality(
            month="2025-01",
            task_instance=mock_ti,
            logical_date=datetime(2025, 1, 5),
        )


def test_quality_checks_silver_fails_on_high_quarantine_rate():
    """Verify that quality check raises AirflowException if rejection rate > 20%."""
    try:
        from airflow.dags.taxi_monthly import check_silver_quality
        from airflow.exceptions import AirflowException
    except ImportError:
        pytest.skip("Airflow package not installed in the local host test environment.")

    mock_ti = MagicMock()
    # 700 valid, 300 rejected -> 30% rejection rate (> 20%)
    mock_ti.xcom_pull.return_value = {
        "silver_count": 700,
        "rejected_count": 300,
    }

    with pytest.raises(AirflowException, match="exceeding the critical safety threshold"):
        check_silver_quality(
            month="2025-01",
            task_instance=mock_ti,
            logical_date=datetime(2025, 1, 5),
        )
