import csv
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.shared.db import connect, is_postgres, placeholder, serial_type


RAW_PATH = Path(os.getenv("RAW_CSV_PATH", "data/raw/telco_customer_churn.csv"))
PROCESSED_PATH = Path(os.getenv("PROCESSED_CSV_PATH", "data/processed/telco_customer_churn_clean.csv"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

EXPECTED_COLUMNS = [
    "customerID",
    "gender",
    "SeniorCitizen",
    "Partner",
    "Dependents",
    "tenure",
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "Contract",
    "PaperlessBilling",
    "PaymentMethod",
    "MonthlyCharges",
    "TotalCharges",
    "Churn",
]

ENUMS = {
    "gender": {"Male", "Female"},
    "SeniorCitizen": {"0", "1"},
    "Partner": {"Yes", "No"},
    "Dependents": {"Yes", "No"},
    "PhoneService": {"Yes", "No"},
    "MultipleLines": {"Yes", "No", "No phone service"},
    "InternetService": {"DSL", "Fiber optic", "No"},
    "OnlineSecurity": {"Yes", "No", "No internet service"},
    "OnlineBackup": {"Yes", "No", "No internet service"},
    "DeviceProtection": {"Yes", "No", "No internet service"},
    "TechSupport": {"Yes", "No", "No internet service"},
    "StreamingTV": {"Yes", "No", "No internet service"},
    "StreamingMovies": {"Yes", "No", "No internet service"},
    "Contract": {"Month-to-month", "One year", "Two year"},
    "PaperlessBilling": {"Yes", "No"},
    "PaymentMethod": {
        "Electronic check",
        "Mailed check",
        "Bank transfer (automatic)",
        "Credit card (automatic)",
    },
    "Churn": {"Yes", "No"},
}

NUMERIC_COLUMNS = {"tenure", "MonthlyCharges", "TotalCharges"}
INTERNET_DEPENDENT_COLUMNS = [
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
]


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"pipeline_{RUN_ID}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
    )
    return log_path


def issue(stage: str, severity: str, code: str, message: str, row: int | None = None, customer_id: str | None = None):
    return {
        "run_id": RUN_ID,
        "stage": stage,
        "severity": severity,
        "code": code,
        "message": message,
        "row_number": row,
        "customer_id": customer_id,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


def read_raw() -> tuple[list[dict], list[dict]]:
    issues = []
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo de ingesta: {RAW_PATH}")

    with RAW_PATH.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames != EXPECTED_COLUMNS:
            issues.append(
                issue(
                    "ingesta",
                    "critical",
                    "schema_columns_mismatch",
                    f"Columnas recibidas: {reader.fieldnames}; columnas esperadas: {EXPECTED_COLUMNS}",
                )
            )
            raise ValueError("El esquema de columnas no coincide con el metadata del caso 1")
        rows = list(reader)

    logging.info("Ingesta completada: %s filas leidas desde %s", len(rows), RAW_PATH)
    return rows, issues


def to_float(value: str) -> float | None:
    text = str(value).strip()
    if text == "":
        return None
    return float(text)


def clean_and_validate(rows: list[dict]) -> tuple[list[dict], list[dict], dict]:
    issues = []
    clean_rows = []
    seen_ids = set()
    dropped_rows = 0

    for index, raw in enumerate(rows, start=2):
        row = {column: str(raw.get(column, "")).strip() for column in EXPECTED_COLUMNS}
        customer_id = row["customerID"]

        if not customer_id:
            issues.append(issue("limpieza", "error", "missing_customer_id", "Fila sin customerID; se descarta", index))
            dropped_rows += 1
            continue

        if customer_id in seen_ids:
            issues.append(issue("limpieza", "warning", "duplicate_customer_id", "customerID duplicado; se conserva primera aparicion", index, customer_id))
            dropped_rows += 1
            continue
        seen_ids.add(customer_id)

        for column, allowed_values in ENUMS.items():
            if row[column] not in allowed_values:
                issues.append(issue("validacion_semantica", "error", "invalid_enum", f"{column}='{row[column]}' fuera de catalogo", index, customer_id))

        try:
            row["tenure"] = int(row["tenure"])
            row["MonthlyCharges"] = round(to_float(row["MonthlyCharges"]), 2)
            total_charges = to_float(row["TotalCharges"])
        except (TypeError, ValueError):
            issues.append(issue("validacion_estructural", "error", "invalid_numeric", "Campos numericos no convertibles; se descarta fila", index, customer_id))
            dropped_rows += 1
            continue

        if total_charges is None and row["tenure"] == 0:
            total_charges = 0.0
            issues.append(issue("limpieza", "info", "blank_total_charges_imputed", "TotalCharges venia vacio para cliente nuevo; se imputa 0.0", index, customer_id))
        elif total_charges is None:
            issues.append(issue("limpieza", "error", "blank_total_charges_unresolved", "TotalCharges vacio con tenure > 0; se descarta fila", index, customer_id))
            dropped_rows += 1
            continue
        row["TotalCharges"] = round(total_charges, 2)

        if row["tenure"] < 0 or row["MonthlyCharges"] < 0 or row["TotalCharges"] < 0:
            issues.append(issue("validacion_semantica", "error", "negative_amount", "Valores negativos en permanencia o cargos; se descarta fila", index, customer_id))
            dropped_rows += 1
            continue

        if row["InternetService"] == "No":
            for column in INTERNET_DEPENDENT_COLUMNS:
                if row[column] != "No internet service":
                    issues.append(issue("validacion_semantica", "warning", "internet_service_inconsistency", f"{column} deberia ser 'No internet service'", index, customer_id))

        clean_rows.append(row)

    metrics = {
        "run_id": RUN_ID,
        "raw_rows": len(rows),
        "clean_rows": len(clean_rows),
        "dropped_rows": dropped_rows,
        "issue_count": len(issues),
        "completeness_pct": round(len(clean_rows) / len(rows) * 100, 2) if rows else 0,
        "churn_rate_pct": round(sum(1 for row in clean_rows if row["Churn"] == "Yes") / len(clean_rows) * 100, 2) if clean_rows else 0,
    }
    logging.info("Limpieza/validacion completada: %s", metrics)
    return clean_rows, issues, metrics


def write_processed(rows: list[dict]) -> None:
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    with PROCESSED_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info("CSV limpio escrito en %s", PROCESSED_PATH)


def create_tables(conn) -> None:
    cursor = conn.cursor()
    id_type = serial_type()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id {id_type},
            run_id TEXT UNIQUE NOT NULL,
            started_at TEXT NOT NULL,
            raw_rows INTEGER NOT NULL,
            clean_rows INTEGER NOT NULL,
            dropped_rows INTEGER NOT NULL,
            issue_count INTEGER NOT NULL,
            completeness_pct REAL NOT NULL,
            churn_rate_pct REAL NOT NULL
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS data_quality_issues (
            id {id_type},
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            severity TEXT NOT NULL,
            code TEXT NOT NULL,
            message TEXT NOT NULL,
            row_number INTEGER,
            customer_id TEXT,
            detected_at TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
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
            monthly_charges REAL NOT NULL,
            total_charges REAL NOT NULL,
            churn TEXT NOT NULL,
            loaded_run_id TEXT NOT NULL
        )
        """
    )


def load(rows: list[dict], issues: list[dict], metrics: dict) -> None:
    mark = placeholder()
    with connect() as conn:
        create_tables(conn)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM telco_customers")

        customer_sql = f"""
            INSERT INTO telco_customers VALUES ({",".join([mark] * 22)})
        """
        customer_values = [
            (
                row["customerID"],
                row["gender"],
                int(row["SeniorCitizen"]),
                row["Partner"],
                row["Dependents"],
                row["tenure"],
                row["PhoneService"],
                row["MultipleLines"],
                row["InternetService"],
                row["OnlineSecurity"],
                row["OnlineBackup"],
                row["DeviceProtection"],
                row["TechSupport"],
                row["StreamingTV"],
                row["StreamingMovies"],
                row["Contract"],
                row["PaperlessBilling"],
                row["PaymentMethod"],
                row["MonthlyCharges"],
                row["TotalCharges"],
                row["Churn"],
                RUN_ID,
            )
            for row in rows
        ]
        cursor.executemany(customer_sql, customer_values)

        run_sql = f"""
            INSERT INTO pipeline_runs
            (run_id, started_at, raw_rows, clean_rows, dropped_rows, issue_count, completeness_pct, churn_rate_pct)
            VALUES ({",".join([mark] * 8)})
        """
        cursor.execute(
            run_sql,
            (
                RUN_ID,
                datetime.now(timezone.utc).isoformat(),
                metrics["raw_rows"],
                metrics["clean_rows"],
                metrics["dropped_rows"],
                metrics["issue_count"],
                metrics["completeness_pct"],
                metrics["churn_rate_pct"],
            ),
        )

        issue_sql = f"""
            INSERT INTO data_quality_issues
            (run_id, stage, severity, code, message, row_number, customer_id, detected_at)
            VALUES ({",".join([mark] * 8)})
        """
        cursor.executemany(
            issue_sql,
            [
                (
                    item["run_id"],
                    item["stage"],
                    item["severity"],
                    item["code"],
                    item["message"],
                    item["row_number"],
                    item["customer_id"],
                    item["detected_at"],
                )
                for item in issues
            ],
        )

        if is_postgres():
            conn.commit()
    logging.info("Carga completada en base de datos. Filas cargadas: %s", len(rows))


def write_evidence(issues: list[dict], metrics: dict, log_path: Path) -> None:
    evidence_path = LOG_DIR / f"pipeline_{RUN_ID}_evidence.json"
    with evidence_path.open("w", encoding="utf-8") as file:
        json.dump({"metrics": metrics, "issues": issues[:100]}, file, indent=2, ensure_ascii=False)
    logging.info("Evidencia JSON escrita en %s", evidence_path)
    logging.info("Log de ejecucion disponible en %s", log_path)


def main() -> None:
    log_path = setup_logging()
    logging.info("Inicio pipeline Telco Churn run_id=%s", RUN_ID)
    rows, ingest_issues = read_raw()
    clean_rows, validation_issues, metrics = clean_and_validate(rows)
    issues = ingest_issues + validation_issues
    metrics["issue_count"] = len(issues)
    write_processed(clean_rows)
    load(clean_rows, issues, metrics)
    write_evidence(issues, metrics, log_path)
    logging.info("Pipeline finalizado correctamente")


if __name__ == "__main__":
    main()
