import json
import os
import time
import requests
from io import BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pika
from minio import Minio
from minio.error import S3Error


# =========================
# RabbitMQ config
# =========================

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

AIR_QUEUE = os.getenv("AIR_QUEUE", "air_queue")
WEATHER_QUEUE = os.getenv("WEATHER_QUEUE", "weather_queue")


# =========================
# MinIO config
# =========================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

CLEAN_BUCKET = os.getenv("CLEAN_BUCKET", "clean-zone")


# =========================
# n8n Workflow 2 webhook
# =========================

N8N_PIPELINE_WEBHOOK_URL = os.getenv(
    "N8N_PIPELINE_WEBHOOK_URL",
    "http://n8n:5678/webhook/run-pipeline"
)


# =========================
# MinIO helpers
# =========================

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


def object_exists(bucket_name: str, object_name: str) -> bool:
    client = get_minio_client()

    try:
        client.stat_object(bucket_name, object_name)
        return True

    except S3Error as error:
        if error.code in ["NoSuchKey", "NoSuchBucket"]:
            return False
        raise


# =========================
# Date helpers
# =========================

def get_target_date() -> str:
    """
    Devuelve la fecha que queremos conservar.

    En entrega final:
    - calcula automáticamente ayer según Europe/Madrid.

    Para pruebas:
    - permite forzar una fecha con TARGET_DATE.
      Ejemplo: TARGET_DATE=2026-05-06
    """

    target_date = os.getenv("TARGET_DATE")

    if target_date:
        return target_date

    madrid_now = datetime.now(ZoneInfo("Europe/Madrid"))
    yesterday = madrid_now - timedelta(days=1)

    return yesterday.strftime("%Y-%m-%d")


def parse_datetime(time_value: str) -> str:
    """
    Valida y normaliza un string tipo:
    2026-05-06T00:00

    Devuelve:
    YYYY-MM-DDTHH:MM
    """

    if time_value is None:
        raise ValueError("El campo time viene nulo")

    try:
        dt = datetime.fromisoformat(str(time_value))
        return dt.strftime("%Y-%m-%dT%H:%M")

    except Exception:
        raise ValueError(f"Formato de time inválido: {time_value}")


# =========================
# Cleaning helpers
# =========================

def unwrap_n8n_message(message: dict) -> dict:
    """
    Soporta dos formatos posibles desde n8n:

    1) Mensaje directo:
       {
         "time": "2026-05-06T00:00",
         "pm10": 5.8
       }

    2) Mensaje anidado:
       {
         "json": {
           "time": "2026-05-06T00:00",
           "pm10": 5.8
         }
       }
    """

    if "json" in message and isinstance(message["json"], dict):
        return message["json"]

    return message


def to_number(value, field_name: str):
    """
    Convierte valores numéricos a int o float.

    Si viene None o texto no numérico, lanza error.
    """

    if value is None:
        raise ValueError(f"Valor nulo en campo obligatorio: {field_name}")

    if isinstance(value, bool):
        raise ValueError(f"Valor booleano no válido en campo numérico: {field_name}")

    if isinstance(value, int) or isinstance(value, float):
        return value

    try:
        numeric_value = float(value)

        if numeric_value.is_integer():
            return int(numeric_value)

        return numeric_value

    except Exception:
        raise ValueError(f"Valor no numérico en campo {field_name}: {value}")


def clean_air_record(record: dict) -> dict:
    """
    Limpia un registro horario de calidad del aire.

    Entrada esperada desde n8n:
    {
      "time": "2026-05-06T00:00",
      "pm10": 5.8,
      "pm2_5": 3.3,
      "carbon_dioxide": 458,
      "uv_index": 0,
      "olive_pollen": 31.3,
      "ozone": 55,
      "methane": 1405
    }
    """

    required_fields = [
        "time",
        "pm10",
        "pm2_5",
        "carbon_dioxide",
        "uv_index",
        "olive_pollen",
        "ozone",
        "methane",
    ]

    for field in required_fields:
        if field not in record:
            raise ValueError(f"Falta el campo {field} en air_quality")

    return {
        "datetime": parse_datetime(record["time"]),
        "pm10": to_number(record["pm10"], "pm10"),
        "pm2_5": to_number(record["pm2_5"], "pm2_5"),
        "carbon_dioxide": to_number(record["carbon_dioxide"], "carbon_dioxide"),
        "uv_index": to_number(record["uv_index"], "uv_index"),
        "olive_pollen": to_number(record["olive_pollen"], "olive_pollen"),
        "ozone": to_number(record["ozone"], "ozone"),
        "methane": to_number(record["methane"], "methane"),
    }


def clean_weather_record(record: dict) -> dict:
    """
    Limpia un registro horario meteorológico.

    Entrada esperada desde n8n:
    {
      "time": "2026-05-06T00:00",
      "temperature_2m": 10.4,
      "precipitation": 0
    }
    """

    required_fields = [
        "time",
        "temperature_2m",
        "precipitation",
    ]

    for field in required_fields:
        if field not in record:
            raise ValueError(f"Falta el campo {field} en weather")

    return {
        "datetime": parse_datetime(record["time"]),
        "temperature_2m": to_number(record["temperature_2m"], "temperature_2m"),
        "precipitation": to_number(record["precipitation"], "precipitation"),
    }


# =========================
# Path builders
# =========================

def build_clean_path(dataset: str, datetime_value: str) -> str:
    """
    Construye la ruta del objeto en clean-zone.

    Ejemplo:
    openmeteo/air_quality/year=2026/month=05/day=06/hour=00/air_quality_2026-05-06_00.json
    """

    date_part = datetime_value.split("T")[0]
    hour_part = datetime_value.split("T")[1].split(":")[0]

    year, month, day = date_part.split("-")

    return (
        f"openmeteo/{dataset}/"
        f"year={year}/month={month}/day={day}/hour={hour_part}/"
        f"{dataset}_{date_part}_{hour_part}.json"
    )


def build_clean_path_from_date_hour(dataset: str, date: str, hour: int) -> str:
    """
    Construye la ruta esperada de una hora concreta en clean-zone.
    """

    year, month, day = date.split("-")
    hour_str = f"{hour:02d}"

    return (
        f"openmeteo/{dataset}/"
        f"year={year}/month={month}/day={day}/hour={hour_str}/"
        f"{dataset}_{date}_{hour_str}.json"
    )


def build_trigger_marker_path(date: str) -> str:
    """
    Marcador para no disparar Workflow 2 varias veces para el mismo día.
    """

    return f"_triggers/process_requested/date={date}.json"


# =========================
# Pipeline trigger helpers
# =========================

def is_clean_day_complete(date: str) -> bool:
    """
    Comprueba si ya existen las 24 horas de weather
    y las 24 horas de air_quality en clean-zone.
    """

    for hour in range(24):
        weather_path = build_clean_path_from_date_hour("weather", date, hour)
        air_path = build_clean_path_from_date_hour("air_quality", date, hour)

        if not object_exists(CLEAN_BUCKET, weather_path):
            return False

        if not object_exists(CLEAN_BUCKET, air_path):
            return False

    return True


def trigger_already_sent(date: str) -> bool:
    marker_path = build_trigger_marker_path(date)
    return object_exists(CLEAN_BUCKET, marker_path)


def save_trigger_marker(date: str) -> None:
    marker_path = build_trigger_marker_path(date)

    marker = {
        "date": date,
        "status": "process_requested",
        "webhook": N8N_PIPELINE_WEBHOOK_URL,
        "created_at": datetime.now(ZoneInfo("Europe/Madrid")).isoformat(),
    }

    upload_json(CLEAN_BUCKET, marker_path, marker)


def trigger_workflow_2(date: str) -> None:
    """
    Llama al Webhook del Workflow 2 en n8n.

    El Workflow 2 debe tener:
    POST /webhook/run-pipeline
    """

    print(f"[CLEAN] Disparando Workflow 2 para fecha {date}")
    print(f"[CLEAN] Webhook URL: {N8N_PIPELINE_WEBHOOK_URL}")

    response = requests.post(
        N8N_PIPELINE_WEBHOOK_URL,
        json={"date": date},
        timeout=300,
    )

    response.raise_for_status()

    print(f"[CLEAN] Workflow 2 disparado correctamente: {response.status_code}")


def check_and_trigger_pipeline(date: str) -> None:
    """
    Si clean-zone está completa para la fecha dada,
    dispara el Workflow 2 una sola vez.
    """

    if trigger_already_sent(date):
        print(f"[CLEAN] Workflow 2 ya fue disparado para {date}")
        return

    if not is_clean_day_complete(date):
        print(f"[CLEAN] Clean-zone todavía no está completa para {date}")
        return

    trigger_workflow_2(date)
    save_trigger_marker(date)


# =========================
# Main processing
# =========================

def process_message(message: dict, dataset: str) -> None:
    """
    Procesa un mensaje individual recibido desde RabbitMQ.

    1. Desanida el mensaje si viene con campo json.
    2. Limpia y valida el registro.
    3. Filtra para quedarse solo con la fecha objetivo.
    4. Sube el registro limpio a clean-zone.
    5. Si clean-zone ya está completa, dispara Workflow 2.
    """

    record = unwrap_n8n_message(message)

    if dataset == "air_quality":
        cleaned = clean_air_record(record)

    elif dataset == "weather":
        cleaned = clean_weather_record(record)

    else:
        raise ValueError(f"Dataset no reconocido: {dataset}")

    target_date = get_target_date()
    record_date = cleaned["datetime"].split("T")[0]

    if record_date != target_date:
        print(
            f"[CLEAN] Saltando registro {cleaned['datetime']} "
            f"porque no pertenece a {target_date}"
        )
        return

    clean_path = build_clean_path(dataset, cleaned["datetime"])

    print("=" * 80)
    print(f"[CLEAN] Dataset: {dataset}")
    print(f"[CLEAN] Datetime: {cleaned['datetime']}")
    print(f"[CLEAN] Output: s3://{CLEAN_BUCKET}/{clean_path}")

    upload_json(CLEAN_BUCKET, clean_path, cleaned)

    print("[CLEAN] OK")

    check_and_trigger_pipeline(record_date)

    print("=" * 80)


def make_callback(dataset: str):
    """
    Crea un callback distinto para cada cola.

    Así sabemos si el mensaje viene de air_queue o weather_queue
    y podemos procesarlo como air_quality o weather.
    """

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))

            process_message(message, dataset)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as error:
            print(f"[CLEAN ERROR] {dataset}: {error}")

            # No reencolamos para evitar bucles infinitos si el mensaje viene mal.
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    return callback


def create_rabbitmq_connection():
    """
    Crea conexión con RabbitMQ con reintentos.
    """

    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD,
    )

    connection = None

    for attempt in range(1, 16):
        try:
            print(f"[CLEAN] Intentando conectar a RabbitMQ... intento {attempt}/15")

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
            )

            print("[CLEAN] Conexión a RabbitMQ establecida")
            break

        except pika.exceptions.AMQPConnectionError as error:
            print(f"[CLEAN] RabbitMQ todavía no está listo: {error}")
            time.sleep(5)

    if connection is None:
        raise RuntimeError("No se pudo conectar a RabbitMQ después de 15 intentos")

    return connection


def main():
    print("[CLEAN] Iniciando clean worker")
    print(f"[CLEAN] RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    print(f"[CLEAN] Air queue: {AIR_QUEUE}")
    print(f"[CLEAN] Weather queue: {WEATHER_QUEUE}")
    print(f"[CLEAN] MinIO: {MINIO_ENDPOINT}")
    print(f"[CLEAN] Clean bucket: {CLEAN_BUCKET}")
    print(f"[CLEAN] Target date: {get_target_date()}")
    print(f"[CLEAN] Workflow 2 webhook: {N8N_PIPELINE_WEBHOOK_URL}")

    connection = create_rabbitmq_connection()

    channel = connection.channel()

    # n8n normalmente crea colas no durables.
    # Si aquí pusieras durable=True y la cola existe como no durable,
    # RabbitMQ puede lanzar PRECONDITION_FAILED.
    channel.queue_declare(queue=AIR_QUEUE, durable=False)
    channel.queue_declare(queue=WEATHER_QUEUE, durable=False)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=AIR_QUEUE,
        on_message_callback=make_callback("air_quality"),
    )

    channel.basic_consume(
        queue=WEATHER_QUEUE,
        on_message_callback=make_callback("weather"),
    )

    print("[CLEAN] Esperando mensajes de RabbitMQ...")

    channel.start_consuming()


if __name__ == "__main__":
    main()