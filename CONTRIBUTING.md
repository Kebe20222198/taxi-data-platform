# Contributing to NYC Taxi Data Platform

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## 🚀 Getting Started

### Prerequisites

- **Docker** ≥ 24.0 and **Docker Compose** ≥ 2.27
- **Python** ≥ 3.11
- **Java** 17 (for local PySpark testing)
- **Make** (GNU Make)
- **16 GB RAM** minimum on the host machine

### Setting Up the Development Environment

```bash
# 1. Clone the repository
git clone https://github.com/Kebe20222198/taxi-data-platform.git
cd taxi-data-platform

# 2. Create your environment file
cp .env.example .env

# 3. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Start the infrastructure
make up

# 5. Run the tests
make test
```

## 📋 Development Workflow

### Code Standards

- **Linter**: [Ruff](https://docs.astral.sh/ruff/) — Configuration in `pyproject.toml`
- **Python**: Follow PEP 8, type hints encouraged
- **SQL (dbt)**: Uppercase keywords, CTEs over subqueries, descriptive aliases
- **Commits**: Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`)

### Running Quality Checks

```bash
# Lint Python code
make ci-lint

# Run unit tests with coverage
make test

# Validate dbt models
make ci-dbt

# Full CI pipeline locally
make ci-lint && make test && make ci-dbt
```

### Branch Naming Convention

- `feat/<description>` — New features
- `fix/<description>` — Bug fixes
- `docs/<description>` — Documentation updates
- `test/<description>` — Test additions or improvements
- `refactor/<description>` — Code refactoring

## 🏗️ Architecture Overview

The project follows the **Medallion Lakehouse Architecture**:

```
Sources → Ingestion → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → DWH (dbt) → Dashboard
```

Key directories:
- `ingestion/` — Data ingestion scripts (Python + MinIO S3)
- `spark_jobs/` — PySpark transformation jobs (Bronze→Silver→Gold)
- `airflow/dags/` — Airflow 3 orchestration DAGs
- `dbt/` — dbt models, tests, and macros
- `dashboard/` — Streamlit analytics dashboard
- `tests/` — Pytest test suite (unit + integration + e2e)
- `infra/` — Infrastructure scripts (MinIO init, PostgreSQL DDL)
- `docs/` — Technical documentation

## 🧪 Testing Guidelines

- **Unit tests** go in `tests/unit/` — Pure function tests, no I/O
- **Integration tests** go in `tests/integration/` — Local Spark + file system
- **dbt tests** go in `dbt/tests/` — SQL-based data quality assertions
- Minimum coverage threshold: **70%**

## 📝 License

By contributing, you agree that your contributions will be licensed under the MIT License.

