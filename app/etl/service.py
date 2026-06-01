import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile


app = FastAPI(title="Telco Churn ETL Service", version="1.0.0")
RAW_CSV_PATH = Path("data/raw/telco_customer_churn.csv")

state = {
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "return_code": None,
    "message": "Pipeline not started",
    "logs": [],
}
state_lock = threading.Lock()


def append_log(line: str) -> None:
    text = line.strip()
    if not text:
        return
    with state_lock:
        state["logs"].append(text)
        state["logs"] = state["logs"][-80:]


def run_pipeline() -> None:
    with state_lock:
        state.update(
            {
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "return_code": None,
                "message": "Pipeline running",
                "logs": [],
            }
        )

    return_code = 1
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "app.etl.pipeline"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if process.stdout:
            for line in process.stdout:
                append_log(line)
        return_code = process.wait()
    except Exception as exc:
        append_log(f"Pipeline execution error: {exc}")

    final_status = "success" if return_code == 0 else "failed"
    with state_lock:
        state.update(
            {
                "status": final_status,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "return_code": return_code,
                "message": "Pipeline finished successfully" if final_status == "success" else "Pipeline failed; check logs",
            }
        )


@app.get("/internal/health")
def health():
    return {"status": "ok", "service": "etl"}


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "telco-churn-etl",
        "status": "ok",
        "endpoints": [
            "/internal/health",
            "/internal/pipeline/start",
            "/internal/pipeline/status",
        ],
    }


@app.post("/internal/pipeline/start")
def start_pipeline():
    with state_lock:
        if state["status"] == "running":
            raise HTTPException(status_code=409, detail="Pipeline already running")

    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()
    return {"status": "accepted", "message": "Pipeline execution started"}


@app.post("/internal/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="El archivo debe ser CSV")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="El archivo esta vacio")

    RAW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_CSV_PATH.write_bytes(content)
    start_response = start_pipeline()
    return {
        "status": "accepted",
        "filename": file.filename,
        "saved_as": str(RAW_CSV_PATH),
        "message": "Archivo recibido; consulta GET /status para ver logs y estado",
        "etl": start_response,
    }


@app.get("/internal/pipeline/status")
def pipeline_status():
    with state_lock:
        return dict(state)
