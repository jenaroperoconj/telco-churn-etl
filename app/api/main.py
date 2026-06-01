import os

import httpx
from fastapi import FastAPI, HTTPException

from app.shared.db import connect, placeholder


app = FastAPI(title="Telco Churn DataOps API", version="1.0.0")
ETL_INTERNAL_URL = os.getenv("ETL_INTERNAL_URL", "http://pipeline:8001")


def fetch_one(sql: str, params: tuple = ()):
    with connect() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else {}


def fetch_all(sql: str, params: tuple = ()):
    with connect() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


@app.get("/")
def root():
    return {
        "service": "telco-churn-api",
        "status": "ok",
        "endpoints": [
            "/health",
            "/api/v1/pipeline/start",
            "/api/v1/pipeline/status",
            "/api/v1/pipeline/latest",
            "/api/v1/pipeline/issues",
            "/api/v1/data/customers/summary",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/v1/pipeline/start")
def start_pipeline_v1():
    try:
        response = httpx.post(f"{ETL_INTERNAL_URL}/internal/pipeline/start", timeout=10)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"ETL service unavailable: {exc}") from exc


@app.get("/api/v1/pipeline/status")
def pipeline_status_v1():
    try:
        response = httpx.get(f"{ETL_INTERNAL_URL}/internal/pipeline/status", timeout=10)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"ETL service unavailable: {exc}") from exc


@app.get("/pipeline/latest")
def latest_pipeline_run():
    return fetch_one("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1")


@app.get("/api/v1/pipeline/latest")
def latest_pipeline_run_v1():
    return latest_pipeline_run()


@app.get("/pipeline/issues")
def pipeline_issues(limit: int = 20):
    mark = placeholder()
    return fetch_all(
        f"SELECT stage, severity, code, message, row_number, customer_id, detected_at FROM data_quality_issues ORDER BY detected_at DESC LIMIT {mark}",
        (limit,),
    )


@app.get("/api/v1/pipeline/issues")
def pipeline_issues_v1(limit: int = 20):
    return pipeline_issues(limit)


@app.get("/customers/summary")
def customer_summary():
    total = fetch_one("SELECT COUNT(*) AS total_customers FROM telco_customers")
    churn = fetch_one("SELECT COUNT(*) AS churn_customers FROM telco_customers WHERE churn = 'Yes'")
    contract = fetch_all("SELECT contract, COUNT(*) AS customers FROM telco_customers GROUP BY contract ORDER BY customers DESC")
    return {
        "total_customers": total.get("total_customers", 0),
        "churn_customers": churn.get("churn_customers", 0),
        "contract_distribution": contract,
    }


@app.get("/api/v1/data/customers/summary")
def customer_summary_v1():
    return customer_summary()


@app.get("/customers/high-risk-sample")
def high_risk_sample(limit: int = 10):
    mark = placeholder()
    return fetch_all(
        f"""
        SELECT customer_id, tenure, contract, internet_service, payment_method, monthly_charges, total_charges, churn
        FROM telco_customers
        WHERE contract = 'Month-to-month' AND payment_method = 'Electronic check'
        ORDER BY monthly_charges DESC
        LIMIT {mark}
        """,
        (limit,),
    )


@app.get("/api/v1/data/customers")
def customers_v1(limit: int = 50):
    mark = placeholder()
    return fetch_all(
        f"""
        SELECT customer_id, gender, senior_citizen, tenure, contract, internet_service,
               payment_method, monthly_charges, total_charges, churn
        FROM telco_customers
        ORDER BY customer_id
        LIMIT {mark}
        """,
        (limit,),
    )


@app.get("/api/v1/data/customers/high-risk")
def high_risk_sample_v1(limit: int = 10):
    return high_risk_sample(limit)
