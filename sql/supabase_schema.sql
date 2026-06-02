ALTER TABLE IF EXISTS pipeline_runs DROP COLUMN IF EXISTS churn_rate_pct;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT UNIQUE NOT NULL,
    started_at TEXT NOT NULL,
    raw_rows INTEGER NOT NULL,
    clean_rows INTEGER NOT NULL,
    dropped_rows INTEGER NOT NULL,
    issue_count INTEGER NOT NULL,
    completeness_pct DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_issues (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    severity TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    row_number INTEGER,
    customer_id TEXT,
    detected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telco_customers (
    customer_id TEXT PRIMARY KEY,
    gender TEXT NOT NULL,
    senior_citizen INTEGER NOT NULL,
    partner TEXT NOT NULL,
    dependents TEXT NOT NULL,
    tenure INTEGER NOT NULL,
    phone_service TEXT NOT NULL,
    multiple_lines TEXT NOT NULL,
    internet_service TEXT NOT NULL,
    online_security TEXT NOT NULL,
    online_backup TEXT NOT NULL,
    device_protection TEXT NOT NULL,
    tech_support TEXT NOT NULL,
    streaming_tv TEXT NOT NULL,
    streaming_movies TEXT NOT NULL,
    contract TEXT NOT NULL,
    paperless_billing TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    monthly_charges DOUBLE PRECISION NOT NULL,
    total_charges DOUBLE PRECISION NOT NULL,
    churn TEXT NOT NULL,
    loaded_run_id TEXT NOT NULL
);
