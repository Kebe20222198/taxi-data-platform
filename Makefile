# ==============================================================================
# NYC Taxi Data Platform — Makefile
# ==============================================================================
.PHONY: up down ingest run test logs status clean help

COMPOSE := docker compose
MONTH   ?= 2025-01

# ── Help ─────────────────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ──────────────────────────────────────────────────────────
up: ## Start all services (MinIO, Spark, Airflow, Postgres, Metabase)
	$(COMPOSE) up -d --build
	@echo "⏳ Waiting for MinIO to be ready..."
	@until curl -sf http://localhost:$${MINIO_PORT:-9000}/minio/health/live > /dev/null 2>&1; do sleep 2; done
	@echo "✅ All services are up!"
	@echo "   MinIO Console : http://localhost:$${MINIO_CONSOLE_PORT:-9001}"
	@echo "   Spark Master  : http://localhost:$${SPARK_MASTER_WEBUI_PORT:-8085}"
	@echo "   Airflow       : http://localhost:8080"
	@echo "   Metabase      : http://localhost:$${METABASE_PORT:-3000}"

down: ## Stop and remove all containers, networks
	$(COMPOSE) down --remove-orphans

# ── Ingestion ────────────────────────────────────────────────────────────────
ingest: ## Ingest a month of taxi data (usage: make ingest MONTH=2025-01 SOURCE=local)
	@echo "🚕 Ingesting Yellow Taxi data for $(MONTH) from $(or $(SOURCE),local)..."
	@if $(COMPOSE) ps airflow-scheduler 2>/dev/null | grep -qE "Up|running"; then \
		$(COMPOSE) exec airflow-scheduler python3 /opt/airflow/ingestion/ingest.py --month $(MONTH) --source $(or $(SOURCE),local); \
	else \
		python3 ingestion/ingest.py --month $(MONTH) --source $(or $(SOURCE),local); \
	fi

ingest-range: ## Ingest a range of months (usage: make ingest-range START=2025-01 END=2025-12 SOURCE=local)
	@for m in $$(python3 -c "import pandas as pd; [print(d.strftime('%Y-%m')) for d in pd.date_range('$(or $(START),2025-01)-01','$(or $(END),2025-12)-01',freq='MS')]"); do \
		echo "🚕 Ingesting $$m from $(or $(SOURCE),local)..."; \
		if $(COMPOSE) ps airflow-scheduler 2>/dev/null | grep -qE "Up|running"; then \
			$(COMPOSE) exec airflow-scheduler python3 /opt/airflow/ingestion/ingest.py --month $$m --source $(or $(SOURCE),local); \
		else \
			python3 ingestion/ingest.py --month $$m --source $(or $(SOURCE),local); \
		fi; \
	done

# ── Spark Jobs ──────────────────────────────────────────────────────────────
SPARK_JARS ?= /opt/spark/jars_extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars_extra/delta-storage-3.1.0.jar,/opt/spark/jars_extra/hadoop-aws-3.3.4.jar,/opt/spark/jars_extra/aws-java-sdk-bundle-1.12.262.jar

run: ## Submit Spark jobs (bronze → silver → gold)
	@echo "⚡ Submitting Spark Bronze → Silver job for $(MONTH)..."
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--jars $(SPARK_JARS) \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		/opt/spark_jobs/bronze_to_silver.py --month $(MONTH)
	@echo "⚡ Submitting Spark Silver → Gold job for $(MONTH)..."
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--jars $(SPARK_JARS) \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		/opt/spark_jobs/silver_to_gold.py --month $(MONTH)

# ── Airflow 3 Orchestration ──────────────────────────────────────────────────
airflow-trigger: ## Trigger monthly DAG run (usage: make airflow-trigger MONTH=2025-01)
	@echo "🚀 Triggering taxi_monthly DAG for $(MONTH)..."
	$(COMPOSE) exec airflow-scheduler airflow dags trigger taxi_monthly --logical-date $(MONTH)-05

airflow-backfill: ## Run Airflow 3 backfill for 3 months (Jan-Mar 2025)
	@echo "⏳ Creating Airflow 3 backfill (3 months, max_active_runs=1)..."
	$(COMPOSE) exec airflow-scheduler airflow backfill create \
		--dag-id taxi_monthly \
		--from-date 2025-01-01 \
		--to-date 2025-03-31 \
		--max-active-runs 1 \
		--reprocess-behavior completed

airflow-runs: ## Display pipeline_runs table records from DWH PostgreSQL
	@echo "📊 Querying pipeline_runs table from DWH..."
	$(COMPOSE) exec postgres psql -U airflow -d airflow -c \
		"SELECT run_id, dag_id, execution_month, status, records_processed, records_rejected, rejection_rate, duration_seconds FROM pipeline_runs ORDER BY created_at DESC LIMIT 10;"

# ── dbt Data Warehouse ───────────────────────────────────────────────────────
dbt-run: ## Execute dbt models (usage: make dbt-run MONTH=2025-01)
	@echo "🏗️ Running dbt models for $(MONTH)..."
	$(COMPOSE) exec airflow-scheduler dbt run \
		--project-dir /opt/airflow/dbt \
		--profiles-dir /opt/airflow/dbt \
		--vars '{"month": "$(MONTH)"}'

dbt-test: ## Execute dbt data quality tests
	@echo "🧪 Running dbt tests..."
	$(COMPOSE) exec airflow-scheduler dbt test \
		--project-dir /opt/airflow/dbt \
		--profiles-dir /opt/airflow/dbt

dbt-docs: ## Generate dbt documentation catalog
	@echo "📚 Generating dbt documentation..."
	$(COMPOSE) exec airflow-scheduler dbt docs generate \
		--project-dir /opt/airflow/dbt \
		--profiles-dir /opt/airflow/dbt

# ── Spark Benchmarking (Phase 5) ─────────────────────────────────────────────
benchmark: ## Run full 8-level automated Spark benchmark (3 runs per level)
	@echo "🧪 Running full Spark benchmark across all levels..."
	python3 benchmarks/run_benchmarks.py --levels 0,1,2,3,4,5,6,7,8 --runs 3

benchmark-level: ## Run benchmark for a single level (usage: make benchmark-level LEVEL=8)
	@echo "⚡ Running Spark benchmark for Level $(LEVEL)..."
	python3 spark_jobs/benchmark_pipeline.py --opt-level $(LEVEL) --year 2025

# ── Dashboard & BI ────────────────────────────────────────────────────────────
dashboard: ## Launch Streamlit analytical dashboard locally
	@echo "📊 Starting Streamlit Dashboard on http://localhost:8501..."
	streamlit run dashboard/app.py --server.port 8501

# ── CI / CD Local Emulation ──────────────────────────────────────────────────
ci-lint: ## Run ruff linting and formatting check
	@echo "🧹 Running ruff checks..."
	ruff check . && ruff format --check .

ci-test: ## Run unit tests with strict 70% coverage requirement
	@echo "🧪 Running unit tests with coverage gate..."
	python3 -m pytest --cov=spark_jobs.common.transformations --cov-fail-under=70 tests/unit/

ci-dbt: ## Generate micro-dataset and execute dbt build locally on DuckDB
	@echo "🏗️ Generating CI dataset and building dbt models on DuckDB..."
	python3 tests/ci/generate_ci_sample_data.py
	DBT_TARGET=duckdb CI_DBT=true DUCKDB_PATH=tests/ci/ci_warehouse.duckdb dbt build --project-dir dbt --profiles-dir dbt --vars '{"month": "2025-01"}'

# ── Testing ──────────────────────────────────────────────────────────────────
test: ## Run all tests locally (unit + integration)
	@echo "🧪 Running tests locally..."
	python3 -m pytest tests/ -v --tb=short

test-unit: ## Run unit tests locally
	@echo "🧪 Running unit tests..."
	python3 -m pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests locally
	@echo "🧪 Running integration tests..."
	python3 -m pytest tests/integration/ -v --tb=short

test-docker: ## Run tests inside the spark-master Docker container
	@echo "🧪 Running tests inside spark-master container..."
	$(COMPOSE) exec spark-master python3 -m pytest /opt/tests/ -v --tb=short

# ── Streaming Extension (Section 9) ──────────────────────────────────────────
stream-producer: ## Run real-time stream producer (usage: make stream-producer MONTH=2025-01 SPEED=60)
	@echo "📡 Starting Kafka stream producer for $(MONTH) at $(SPEED:-60)x speed..."
	python3 ingestion/stream_producer.py --month $(MONTH) --speed $${SPEED:-60}

stream-spark: ## Submit Spark Structured Streaming job
	@echo "⚡ Submitting Spark Structured Streaming job (Kafka -> Delta)..."
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		/opt/spark_jobs/stream_trips.py

kafka-topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list

kafka-tail: ## Tail recent messages from taxi_trips topic
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic taxi_trips --from-beginning --max-messages 10

# ── Observability ────────────────────────────────────────────────────────────
logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=50

status: ## Show container status
	$(COMPOSE) ps

# ── Cleanup ──────────────────────────────────────────────────────────────────
clean: ## Remove all containers, volumes, and data
	$(COMPOSE) down -v --remove-orphans
	@echo "🧹 All volumes and containers removed."
