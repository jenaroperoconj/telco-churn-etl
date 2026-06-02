import csv
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from app.shared.db import connect, is_postgres, placeholder, serial_type


RAW_PATH = Path(os.getenv("RAW_CSV_PATH", "data/raw/telco_customer_churn.csv"))
PROCESSING_DIR = Path(os.getenv("PROCESSING_DIR", "data/processing"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", "data/processed"))
FAILED_DIR = Path(os.getenv("FAILED_DIR", "data/failed"))
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
    "gender": {"male", "female"},
    "SeniorCitizen": {"0", "1"},
    "Partner": {"yes", "no"},
    "Dependents": {"yes", "no"},
    "PhoneService": {"yes", "no"},
    "MultipleLines": {"yes", "no", "no phone service"},
    "InternetService": {"dsl", "fiber optic", "no"},
    "OnlineSecurity": {"yes", "no", "no internet service"},
    "OnlineBackup": {"yes", "no", "no internet service"},
    "DeviceProtection": {"yes", "no", "no internet service"},
    "TechSupport": {"yes", "no", "no internet service"},
    "StreamingTV": {"yes", "no", "no internet service"},
    "StreamingMovies": {"yes", "no", "no internet service"},
    "Contract": {"month-to-month", "one year", "two year"},
    "PaperlessBilling": {"yes", "no"},
    "PaymentMethod": {
        "electronic check",
        "mailed check",
        "bank transfer (automatic)",
        "credit card (automatic)",
    },
    "Churn": {"yes", "no"},
}

NUMERIC_COLUMNS = {"tenure", "MonthlyCharges", "TotalCharges"}
TEXT_COLUMNS = set(EXPECTED_COLUMNS) - NUMERIC_COLUMNS - {"SeniorCitizen"}
INTERNET_DEPENDENT_COLUMNS = [
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
]


def ensure_data_dirs() -> None:
    for path in [RAW_PATH.parent, PROCESSING_DIR, PROCESSED_DIR, FAILED_DIR, PROCESSED_PATH.parent]:
        path.mkdir(parents=True, exist_ok=True)


def move_to_processing() -> Path:
    ensure_data_dirs()
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo de ingesta: {RAW_PATH}")

    processing_path = PROCESSING_DIR / f"{RUN_ID}_{RAW_PATH.name}"
    if processing_path.exists():
        processing_path.unlink()
    shutil.move(str(RAW_PATH), str(processing_path))
    logging.info("Archivo movido a processing: %s", processing_path)
    return processing_path


def finish_processing(processing_path: Path, success: bool) -> None:
    if not processing_path.exists():
        return
    target_dir = PROCESSED_DIR if success else FAILED_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / processing_path.name
    if target_path.exists():
        target_path.unlink()
    shutil.move(str(processing_path), str(target_path))
    logging.info("Archivo movido a %s: %s", target_dir.name, target_path)


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


def read_raw(source_path: Path) -> tuple[list[dict], list[dict]]:
    issues = []
    if not source_path.exists():
        raise FileNotFoundError(f"No existe el archivo de ingesta: {source_path}")

    with source_path.open(newline="", encoding="utf-8-sig") as file:
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

    logging.info("Ingesta completada: %s filas leidas desde %s", len(rows), source_path)
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
    normalization_stats: dict[str, dict] = {}

    for index, raw in enumerate(rows, start=2):
        row = {}
        for column in EXPECTED_COLUMNS:
            value = str(raw.get(column, "")).strip()
            if column in TEXT_COLUMNS:
                normalized = value.lower()
                if value != normalized:
                    column_stats = normalization_stats.setdefault(column, {"count": 0, "examples": []})
                    column_stats["count"] += 1
                    if len(column_stats["examples"]) < 3:
                        column_stats["examples"].append(f"{value} -> {normalized}")
                row[column] = normalized
            else:
                row[column] = value

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

        if row["InternetService"] == "no":
            for column in INTERNET_DEPENDENT_COLUMNS:
                if row[column] != "no internet service":
                    issues.append(issue("validacion_semantica", "warning", "internet_service_inconsistency", f"{column} deberia ser 'no internet service'", index, customer_id))

        clean_rows.append(row)

    for column, stats in normalization_stats.items():
        examples = "; ".join(stats["examples"])
        issues.append(
            issue(
                "transformacion",
                "info",
                "lowercase_normalization",
                f"Columna {column} normalizada a minusculas: {stats['count']} valores modificados. Ejemplos: {examples}",
            )
        )

    metrics = {
        "run_id": RUN_ID,
        "raw_rows": len(rows),
        "clean_rows": len(clean_rows),
        "dropped_rows": dropped_rows,
        "issue_count": len(issues),
        "completeness_pct": round(len(clean_rows) / len(rows) * 100, 2) if rows else 0,
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
    cursor.execute("DROP TABLE IF EXISTS pipeline_runs")
    cursor.execute("DROP TABLE IF EXISTS data_quality_issues")
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
            completeness_pct REAL NOT NULL
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
        cursor.execute("DELETE FROM pipeline_runs")
        cursor.execute("DELETE FROM data_quality_issues")

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
            (run_id, started_at, raw_rows, clean_rows, dropped_rows, issue_count, completeness_pct)
            VALUES ({",".join([mark] * 7)})
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
    processing_path = None
    try:
        processing_path = move_to_processing()
        rows, ingest_issues = read_raw(processing_path)
        clean_rows, validation_issues, metrics = clean_and_validate(rows)
        issues = ingest_issues + validation_issues
        metrics["issue_count"] = len(issues)
        write_processed(clean_rows)
        load(clean_rows, issues, metrics)
        write_evidence(issues, metrics, log_path)
        finish_processing(processing_path, success=True)
        logging.info("Pipeline finalizado correctamente")
    except Exception:
        if processing_path is not None:
            finish_processing(processing_path, success=False)
        logging.exception("Pipeline finalizado con error")
        raise


if __name__ == "__main__":
    main()
