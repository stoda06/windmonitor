#!/usr/bin/env python3
"""Subscribe to MQTT wind data and store readings in MariaDB once per minute."""

import json
import logging
import threading
import time

import mysql.connector
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "weather/rooftop_wind"

DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "windmonitor"
DB_PASSWORD = "windmonitor"
DB_NAME = "windmonitor"

STORE_INTERVAL_SECONDS = 60

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mqtt_to_db")

# ---------------------------------------------------------------------------
# Shared state – holds the most recent reading received from MQTT
# ---------------------------------------------------------------------------
latest_reading = None
reading_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db_connection():
    """Return a new MariaDB connection."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )


def store_reading(wind_speed: float, wind_dir: float):
    """Insert a single wind reading into the database."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO wind_readings (wind_speed, wind_dir) VALUES (%s, %s)",
            (wind_speed, wind_dir),
        )
        conn.commit()
        log.info("Stored reading: speed=%.2f dir=%.1f", wind_speed, wind_dir)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Periodic writer – runs in its own thread
# ---------------------------------------------------------------------------
def periodic_writer():
    """Every STORE_INTERVAL_SECONDS, write the latest reading to the DB."""
    global latest_reading

    while True:
        time.sleep(STORE_INTERVAL_SECONDS)

        with reading_lock:
            reading = latest_reading
            latest_reading = None  # consume it

        if reading is None:
            log.debug("No new reading to store this interval.")
            continue

        try:
            store_reading(reading["wind"], reading["dir"])
        except Exception:
            log.exception("Failed to store reading in database")


# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("Connected to MQTT broker at %s:%d", MQTT_BROKER, MQTT_PORT)
        client.subscribe(MQTT_TOPIC)
        log.info("Subscribed to topic: %s", MQTT_TOPIC)
    else:
        log.error("MQTT connection failed with code %d", rc)


def on_message(client, userdata, msg):
    global latest_reading

    try:
        payload = json.loads(msg.payload.decode())
        wind = float(payload["wind"])
        direction = float(payload["dir"])
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        log.warning("Ignoring malformed message on %s: %s", msg.topic, exc)
        return

    with reading_lock:
        latest_reading = {"wind": wind, "dir": direction}

    log.debug("Received: wind=%.2f dir=%.1f", wind, direction)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Start the background writer thread
    writer = threading.Thread(target=periodic_writer, daemon=True)
    writer.start()

    # Set up and connect the MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    log.info("Connecting to MQTT broker %s:%d ...", MQTT_BROKER, MQTT_PORT)
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

    # Blocking loop – handles reconnects automatically
    client.loop_forever()


if __name__ == "__main__":
    main()
