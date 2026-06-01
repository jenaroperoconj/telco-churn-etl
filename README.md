# Telco Customer Churn - Pipeline DataOps

Proyecto para la entrega 2 del caso 1: prediccion y analisis de abandono de clientes en telecomunicaciones.

## Arquitectura

- `pipeline`: contenedor ETL que expone endpoints para iniciar y revisar el pipeline.
- `api`: contenedor FastAPI publico para demo funcional, monitoreo y disparo del ETL.
- `Supabase`: PostgreSQL administrado para la carga final.
- `Render`: despliegue del API y worker del pipeline usando Docker.

## Ejecucion local

```bash
cp .env.example .env
docker compose build
docker compose up
```

Endpoints de demo:

- `GET /health`
- `POST /upload`
- `GET /status`
- `GET /result`

## Supabase

1. Crear proyecto en Supabase.
2. Ejecutar `sql/supabase_schema.sql` en SQL Editor.
3. Copiar el connection string PostgreSQL.
4. Configurar `DATABASE_URL` en Render.

## Render

El archivo `render.yaml` define:

- `telco-churn-api`: servicio web Docker publico.
- `telco-churn-pipeline`: servicio web Docker que ejecuta el pipeline al recibir el trigger desde la API.

En Render se debe agregar `DATABASE_URL` con el string de Supabase y `ETL_INTERNAL_URL` en el API apuntando a la URL del servicio ETL.

## Evidencias

Cada ejecucion del ETL genera:

- Logs en `logs/pipeline_<run_id>.log`.
- Evidencia JSON en `logs/pipeline_<run_id>_evidence.json`.
- CSV limpio en `data/processed/telco_customer_churn_clean.csv`.

Estos archivos sirven como muestra para el informe final junto con capturas de Docker, Render, Supabase y endpoints del API.
