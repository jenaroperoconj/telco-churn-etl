import os

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile

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


def get_etl_status():
    try:
        response = httpx.get(f"{ETL_INTERNAL_URL}/internal/pipeline/status", timeout=10)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        return {"status": "unavailable", "message": f"ETL service unavailable: {exc}"}


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "telco-churn-api",
        "status": "ok",
        "endpoints": [
            "/health",
            "/upload",
            "/status",
            "/result",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="El archivo debe ser CSV")

    file_content = await file.read()
    if not file_content:
        raise HTTPException(status_code=400, detail="El archivo esta vacio")

    try:
        response = httpx.post(
            f"{ETL_INTERNAL_URL}/internal/upload",
            files={"file": (file.filename, file_content, file.content_type or "text/csv")},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"ETL service unavailable: {exc}") from exc


@app.get("/status")
def pipeline_status():
    return get_etl_status()


@app.get("/pipeline/latest", include_in_schema=False)
def latest_pipeline_run():
    return fetch_one("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1")


@app.get("/pipeline/issues", include_in_schema=False)
def pipeline_issues(limit: int = 20):
    mark = placeholder()
    return fetch_all(
        f"SELECT stage, severity, code, message, row_number, customer_id, detected_at FROM data_quality_issues ORDER BY detected_at DESC LIMIT {mark}",
        (limit,),
    )


@app.get("/customers/summary", include_in_schema=False)
def customer_summary():
    total = fetch_one("SELECT COUNT(*) AS total_customers FROM telco_customers")
    churn = fetch_one("SELECT COUNT(*) AS churn_customers FROM telco_customers WHERE churn = 'Yes'")
    contract = fetch_all("SELECT contract, COUNT(*) AS customers FROM telco_customers GROUP BY contract ORDER BY customers DESC")
    return {
        "total_customers": total.get("total_customers", 0),
        "churn_customers": churn.get("churn_customers", 0),
        "contract_distribution": contract,
    }


@app.get("/customers/high-risk-sample", include_in_schema=False)
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


@app.get("/result")
def result():
    etl_status = get_etl_status()
    try:
        return {
            "status": "ok",
            "etl_status": etl_status,
            "latest_run": latest_pipeline_run(),
            "summary": customer_summary(),
            "issues": pipeline_issues(20),
        }
    except Exception as exc:
        return {
            "status": "error",
            "message": "No se pudo consultar el resultado en la base de datos. Revisa DATABASE_URL en Render/Supabase.",
            "database_error": str(exc),
            "etl_status": etl_status,
        }
