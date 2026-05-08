import csv
import json
import os
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request
from minio import Minio


# =========================
# MinIO config
# =========================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

PROCESS_BUCKET = os.getenv("PROCESS_BUCKET", "process-zone")
ACCESS_BUCKET = os.getenv("ACCESS_BUCKET", "access-zone")


# =========================
# Flask app
# =========================

app = Flask(__name__)


# =========================
# Helpers
# =========================

def get_target_date() -> str:
    """
    Devuelve la fecha a procesar.

    Si existe TARGET_DATE, usa esa fecha.
    Si no, calcula ayer según Europe/Madrid.
    """

    target_date = os.getenv("TARGET_DATE")

    if target_date:
        return target_date

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


def upload_csv(bucket_name: str, object_name: str, records: list[dict]) -> None:
    """
    Sube una lista de diccionarios como CSV a MinIO.
    Cada diccionario será una fila.
    """

    ensure_bucket(bucket_name)
    client = get_minio_client()

    if not records:
        raise ValueError("No hay registros para generar el CSV.")

    csv_buffer = StringIO()

    # Cogemos todas las columnas posibles por si algún registro tiene campos extra
    fieldnames = []
    for record in records:
        for key in record.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    writer = csv.DictWriter(
        csv_buffer,
        fieldnames=fieldnames,
        extrasaction="ignore",
    )

    writer.writeheader()
    writer.writerows(records)

    content = csv_buffer.getvalue().encode("utf-8")

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type="text/csv",
    )

    print(f"[MINIO] CSV subido: s3://{bucket_name}/{object_name}")


def build_process_path(dataset: str, date: str, hour: int) -> str:
    year, month, day = date.split("-")
    hour_str = f"{hour:02d}"

    return (
        f"openmeteo/{dataset}/"
        f"year={year}/month={month}/day={day}/hour={hour_str}/"
        f"{dataset}_{date}_{hour_str}.json"
    )


def build_access_path(date: str) -> str:
    year, month, day = date.split("-")

    return (
        f"openmeteo/environmental_daily/"
        f"year={year}/month={month}/day={day}/"
        f"environmental_daily_{date}.csv"
    )


def average(records: list[dict], field: str):
    values = [
        record.get(field)
        for record in records
        if record.get(field) is not None
    ]

    if not values:
        return None

    return round(sum(values) / len(values), 2)


def total(records: list[dict], field: str):
    values = [
        record.get(field)
        for record in records
        if record.get(field) is not None
    ]

    if not values:
        return None

    return round(sum(values), 2)


def maximum(records: list[dict], field: str):
    values = [
        record.get(field)
        for record in records
        if record.get(field) is not None
    ]

    if not values:
        return None

    return max(values)


def join_air_and_weather(air: dict, weather: dict) -> dict:
    """
    Une un registro horario de air_quality con uno de weather.
    Ambos ya vienen procesados desde process-zone.
    """

    joined = {
        "datetime": air.get("datetime") or weather.get("datetime"),
        "date": air.get("date") or weather.get("date"),
        "hour": air.get("hour") if air.get("hour") is not None else weather.get("hour"),
        "city": "Madrid",
    }

    # Weather
    joined["temperature_2m"] = weather.get("temperature_2m")
    joined["precipitation"] = weather.get("precipitation")
    joined["temperature_percentage_of_day"] = weather.get("temperature_percentage_of_day")
    joined["precipitation_percentage_of_day"] = weather.get("precipitation_percentage_of_day")

    # Air quality
    joined["pm10"] = air.get("pm10")
    joined["pm2_5"] = air.get("pm2_5")
    joined["carbon_dioxide"] = air.get("carbon_dioxide")
    joined["uv_index"] = air.get("uv_index")
    joined["olive_pollen"] = air.get("olive_pollen")
    joined["ozone"] = air.get("ozone")
    joined["methane"] = air.get("methane")

    # Percentages from process-zone
    joined["pm10_percentage_of_day"] = air.get("pm10_percentage_of_day")
    joined["pm2_5_percentage_of_day"] = air.get("pm2_5_percentage_of_day")
    joined["carbon_dioxide_percentage_of_day"] = air.get("carbon_dioxide_percentage_of_day")
    joined["uv_index_percentage_of_day"] = air.get("uv_index_percentage_of_day")
    joined["olive_pollen_percentage_of_day"] = air.get("olive_pollen_percentage_of_day")
    joined["ozone_percentage_of_day"] = air.get("ozone_percentage_of_day")
    joined["methane_percentage_of_day"] = air.get("methane_percentage_of_day")

    # Business rule from process-zone
    joined["is_toxic"] = air.get("is_toxic")

    return joined


def build_access_dataset(date: str) -> dict:
    """
    Construye el dataset final diario.

    Lee:
    - process-zone/openmeteo/air_quality/...
    - process-zone/openmeteo/weather/...

    Escribe:
    - access-zone/openmeteo/environmental_daily/...csv
    """

    print("=" * 60)
    print(f"[ACCESS] Construyendo dataset diario para {date}")

    records = []

    for hour in range(24):
        try:
            air_path = build_process_path("air_quality", date, hour)
            weather_path = build_process_path("weather", date, hour)

            air_record = read_json(PROCESS_BUCKET, air_path)
            weather_record = read_json(PROCESS_BUCKET, weather_path)

            joined = join_air_and_weather(air_record, weather_record)

            records.append(joined)

            print(f"[ACCESS] Hora {hour:02d} OK")

        except Exception as error:
            print(f"[ACCESS ERROR] hour={hour:02d}: {error}")

    toxic_hours = sum(
        1 for record in records
        if record.get("is_toxic") is True
    )

    precipitation_hours = sum(
        1 for record in records
        if record.get("precipitation", 0) > 0
    )

    total_records = len(records)

    if total_records > 0:
        toxic_hours_percentage = round((toxic_hours / total_records) * 100, 2)
        precipitation_hours_percentage = round((precipitation_hours / total_records) * 100, 2)
    else:
        toxic_hours_percentage = None
        precipitation_hours_percentage = None

    summary = {
        "total_records": total_records,
        "toxic_hours": toxic_hours,
        "toxic_hours_percentage": toxic_hours_percentage,
        "precipitation_hours": precipitation_hours,
        "precipitation_hours_percentage": precipitation_hours_percentage,
        "average_temperature_2m": average(records, "temperature_2m"),
        "total_precipitation": total(records, "precipitation"),
        "average_pm10": average(records, "pm10"),
        "average_pm2_5": average(records, "pm2_5"),
        "average_ozone": average(records, "ozone"),
        "max_uv_index": maximum(records, "uv_index"),
    }

    access_path = build_access_path(date)

    upload_csv(
        bucket_name=ACCESS_BUCKET,
        object_name=access_path,
        records=records,
    )

    print(f"[ACCESS] Dataset CSV creado: s3://{ACCESS_BUCKET}/{access_path}")
    print("=" * 60)

    return summary


# =========================
# HTTP endpoints
# =========================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "worker": "access"
    }), 200


@app.route("/run", methods=["POST"])
def run():
    """
    n8n llama a POST /run cuando quiere construir el dataset de access.

    Acepta opcionalmente:
    {
      "date": "YYYY-MM-DD"
    }

    Si no viene date, usa ayer automáticamente.
    """

    body = request.get_json(silent=True) or {}
    date = body.get("date") or get_target_date()

    print(f"[ACCESS] Solicitud recibida para fecha: {date}")

    try:
        summary = build_access_dataset(date)

        return jsonify({
            "status": "ok",
            "date": date,
            "message": f"access completado para {date}",
            "output_format": "csv",
            "output_file": build_access_path(date),
            "summary": summary,
        }), 200

    except Exception as error:
        print(f"[ACCESS ERROR] {error}")

        return jsonify({
            "status": "error",
            "date": date,
            "error": str(error),
        }), 500


# =========================
# Entrypoint
# =========================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8002"))

    print(f"[ACCESS] Iniciando servidor en puerto {port}")

    app.run(
        host="0.0.0.0",
        port=port,
    )
