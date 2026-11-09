-- ==============================================================================
-- NYC Taxi Data Platform — DWH & Observability Database Initialization
-- Target: PostgreSQL 15 (taxi-postgres)
-- ==============================================================================

-- Observability Table: Tracks all pipeline execution runs, data metrics, and SLA status
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(255) PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100),
    logical_date TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_month VARCHAR(7) NOT NULL,         -- Format YYYY-MM
    status VARCHAR(50) NOT NULL,                 -- SUCCESS, FAILED, RUNNING, RETRY
    duration_seconds NUMERIC(10, 2) DEFAULT 0.0,
    records_processed BIGINT DEFAULT 0,
    records_rejected BIGINT DEFAULT 0,
    rejection_rate NUMERIC(6, 4) DEFAULT 0.0,    -- e.g. 0.0425 for 4.25%
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_month ON pipeline_runs(execution_month);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_id ON pipeline_runs(dag_id);

COMMENT ON TABLE pipeline_runs IS 'DataOps pipeline execution metadata, data quality metrics and SLA observability';
COMMENT ON COLUMN pipeline_runs.run_id IS 'Unique Airflow DagRun or task run identifier';
COMMENT ON COLUMN pipeline_runs.logical_date IS 'Airflow logical execution date';
COMMENT ON COLUMN pipeline_runs.execution_month IS 'Target partitioned batch month (YYYY-MM)';
COMMENT ON COLUMN pipeline_runs.rejection_rate IS 'Fraction of records routed to quarantine during quality checks';
