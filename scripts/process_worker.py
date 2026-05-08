import json
import os
from io import BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from threading import Thread

from flask import Flask, jsonify, request
from minio import Minio


# =========================
# MinIO config
# =========================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

CLEAN_BUCKET = os.getenv("CLEAN_BUCKET", "clean-zone")
PROCESS_BUCKET = os.getenv("PROCESS_BUCKET", "process-zone")


# =========================
# Flask app
# =========================

app = Flask(__name__)


# =========================
# Helpers (sin cambios)
# =========================

def get_target_date() -> str:
    madrid_now = datetime.now(ZoneInfo("Europe/Madrid"))
    yesterday = madrid_now - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(bucket_name: str) -> None:
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"[MINIO] Bucket creado: {bucket_name}")


def read_json(bucket_name: str, object_name: str) -> dict:
    client = get_minio_client()
    response = client.get_object(bucket_name, object_name)
    try:
        data = response.read().decode("utf-8")
        return json.loads(data)
    finally:
        response.close()
        response.release_conn()


def upload_json(bucket_name: str, object_name: str, data: dict) -> None:
    ensure_bucket(bucket_name)
    client = get_minio_client()
    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type="application/json",
    )
    print(f"[MINIO] Subido: s3://{bucket_name}/{object_name}")


def build_hour_path(dataset: str, date: str, hour: int) -> str:
    year, month, day = date.split("-")
    hour_str = f"{hour:02d}"
    return (
        f"openmeteo/{dataset}/"
        f"year={year}/month={month}/day={day}/hour={hour_str}/"
        f"{dataset}_{date}_{hour_str}.json"
    )


def add_basic_process_fields(record: dict, dataset: str) -> dict:
    datetime_value = record["datetime"]
    dt = datetime.fromisoformat(datetime_value)
    processed = {
        "datetime": datetime_value,
        "date": dt.strftime("%Y-%m-%d"),
        "hour": dt.hour,
        "dataset": dataset,
    }
    for key, value in record.items():
        if key == "datetime":
            continue
        processed[key] = value
    return processed


def percentage(value, total):
    if value is None or total is None or total == 0:
        return None
    return round((value / total) * 100, 2)


def sum_field(records: list, field: str):
    values = [r.get(field) for r in records if r.get(field) is not None]
    return sum(values) if values else 0


def is_toxic_air(record: dict) -> bool:
    pm10 = record.get("pm10")
    pm2_5 = record.get("pm2_5")
    ozone = record.get("ozone")
    if pm10 is not None and pm10 > 10:
        return True
    if pm2_5 is not None and pm2_5 > 10:
        return True
    if ozone is not None and ozone > 50:
        return True
    return False


def read_clean_day(dataset: str, date: str) -> list:
    records = []
    for hour in range(24):
        path = build_hour_path(dataset, date, hour)
        try:
            record = read_json(CLEAN_BUCKET, path)
            records.append(record)
        except Exception as error:
            print(f"[PROCESS ERROR] No se pudo leer {dataset} hour={hour:02d}: {error}")
    return records


def process_air_quality_day(date: str) -> None:
    print("=" * 60)
    print(f"[PROCESS] Procesando air_quality para {date}")
    clean_records = read_clean_day("air_quality", date)
    totals = {
        "pm10": sum_field(clean_records, "pm10"),
        "pm2_5": sum_field(clean_records, "pm2_5"),
        "carbon_dioxide": sum_field(clean_records, "carbon_dioxide"),
        "uv_index": sum_field(clean_records, "uv_index"),
        "olive_pollen": sum_field(clean_records, "olive_pollen"),
        "ozone": sum_field(clean_records, "ozone"),
        "methane": sum_field(clean_records, "methane"),
    }
    for record in clean_records:
        processed = add_basic_process_fields(record, "air_quality")
        processed["pm10_percentage_of_day"] = percentage(record.get("pm10"), totals["pm10"])
        processed["pm2_5_percentage_of_day"] = percentage(record.get("pm2_5"), totals["pm2_5"])
        processed["carbon_dioxide_percentage_of_day"] = percentage(record.get("carbon_dioxide"), totals["carbon_dioxide"])
        processed["uv_index_percentage_of_day"] = percentage(record.get("uv_index"), totals["uv_index"])
        processed["olive_pollen_percentage_of_day"] = percentage(record.get("olive_pollen"), totals["olive_pollen"])
        processed["ozone_percentage_of_day"] = percentage(record.get("ozone"), totals["ozone"])
        processed["methane_percentage_of_day"] = percentage(record.get("methane"), totals["methane"])
        processed["is_toxic"] = is_toxic_air(record)
        output_path = build_hour_path("air_quality", processed["date"], processed["hour"])
        upload_json(PROCESS_BUCKET, output_path, processed)
    print(f"[PROCESS] air_quality terminado para {date} ({len(clean_records)} registros)")


def process_weather_day(date: str) -> None:
    print("=" * 60)
    print(f"[PROCESS] Procesando weather para {date}")
    clean_records = read_clean_day("weather", date)
    totals = {
        "temperature_2m": sum_field(clean_records, "temperature_2m"),
        "precipitation": sum_field(clean_records, "precipitation"),
    }
    for record in clean_records:
        processed = add_basic_process_fields(record, "weather")
        processed["temperature_percentage_of_day"] = percentage(record.get("temperature_2m"), totals["temperature_2m"])
        processed["precipitation_percentage_of_day"] = percentage(record.get("precipitation"), totals["precipitation"])
        output_path = build_hour_path("weather", processed["date"], processed["hour"])
        upload_json(PROCESS_BUCKET, output_path, processed)
    print(f"[PROCESS] weather terminado para {date} ({len(clean_records)} registros)")


# =========================
# HTTP endpoints
# =========================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "worker": "process"}), 200


@app.route("/run", methods=["POST"])
def run():
    """
    n8n llama a POST /run cuando quiere lanzar el process.
    Acepta opcionalmente { "date": "YYYY-MM-DD" } en el body
    para reprocesar una fecha concreta. Si no viene, usa ayer.
    """
    body = request.get_json(silent=True) or {}
    date = body.get("date") or get_target_date()

    print(f"[PROCESS] Solicitud recibida para fecha: {date}")

    try:
        process_air_quality_day(date)
        process_weather_day(date)
        return jsonify({
            "status": "ok",
            "date": date,
            "message": f"process completado para {date}"
        }), 200

    except Exception as error:
        print(f"[PROCESS ERROR] {error}")
        return jsonify({
            "status": "error",
            "date": date,
            "error": str(error)
        }), 500


# =========================
# Entrypoint
# =========================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8001"))
    print(f"[PROCESS] Iniciando servidor en puerto {port}")
    app.run(host="0.0.0.0", port=port)
