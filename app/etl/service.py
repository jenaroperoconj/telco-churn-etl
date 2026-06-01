import subprocess
import sys
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException


app = FastAPI(title="Telco Churn ETL Service", version="1.0.0")

state = {
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "return_code": None,
    "last_stdout": "",
    "last_stderr": "",
}
state_lock = threading.Lock()


def run_pipeline() -> None:
    with state_lock:
        state.update(
            {
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "return_code": None,
                "last_stdout": "",
                "last_stderr": "",
            }
        )

    result = subprocess.run(
        [sys.executable, "-m", "app.etl.pipeline"],
        text=True,
        capture_output=True,
        check=False,
    )

    with state_lock:
        state.update(
            {
                "status": "success" if result.returncode == 0 else "failed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "return_code": result.returncode,
                "last_stdout": result.stdout[-4000:],
                "last_stderr": result.stderr[-4000:],
            }
        )


@app.get("/internal/health")
def health():
    return {"status": "ok", "service": "etl"}


@app.post("/internal/pipeline/start")
def start_pipeline():
    with state_lock:
        if state["status"] == "running":
            raise HTTPException(status_code=409, detail="Pipeline already running")

    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()
    return {"status": "accepted", "message": "Pipeline execution started"}


@app.get("/internal/pipeline/status")
def pipeline_status():
    with state_lock:
        return dict(state)
