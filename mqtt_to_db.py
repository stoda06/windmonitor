#!/usr/bin/env python3
"""Subscribe to MQTT wind data and store readings in MariaDB once per minute."""

import argparse
import json
import logging
import sys
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
log = logging.getLogger("mqtt_to_db")

# ---------------------------------------------------------------------------
# Shared state – holds the most recent reading received from MQTT
# ---------------------------------------------------------------------------
latest_reading = None
reading_lock = threading.Lock()
message_count = 0
store_count = 0


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db_connection():
    """Return a new MariaDB connection."""
    log.debug("Opening DB connection to %s:%d db=%s user=%s",
              DB_HOST, DB_PORT, DB_NAME, DB_USER)
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    log.debug("DB connection established (id=%s)", conn.connection_id)
    return conn


def store_reading(wind_speed: float, wind_dir: float):
    """Insert a single wind reading into the database."""
    global store_count
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO wind_readings (wind_speed, wind_dir) VALUES (%s, %s)"
        params = (wind_speed, wind_dir)
        log.debug("Executing SQL: %s with params %s", sql, params)
        cursor.execute(sql, params)
        conn.commit()
        store_count += 1
        log.info("Stored reading #%d: speed=%.2f dir=%.1f",
                 store_count, wind_speed, wind_dir)
    finally:
        conn.close()
        log.debug("DB connection closed")


# ---------------------------------------------------------------------------
# Periodic writer – runs in its own thread
# ---------------------------------------------------------------------------
def periodic_writer():
    """Every STORE_INTERVAL_SECONDS, write the latest reading to the DB."""
    global latest_reading

    log.debug("Writer thread started (interval=%ds)", STORE_INTERVAL_SECONDS)

    while True:
        time.sleep(STORE_INTERVAL_SECONDS)

        with reading_lock:
            reading = latest_reading
            latest_reading = None  # consume it

        if reading is None:
            log.debug("No new reading to store this interval.")
            continue

        log.debug("Writer woke up, storing reading: %s", reading)
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


def on_disconnect(client, userdata, flags, rc, properties=None):
    log.warning("Disconnected from MQTT broker (rc=%d)", rc)


def on_message(client, userdata, msg):
    global latest_reading, message_count

    log.debug("Raw MQTT message on %s: %s", msg.topic, msg.payload)

    try:
        payload = json.loads(msg.payload.decode())
        wind = float(payload["wind"])
        direction = float(payload["dir"])
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        log.warning("Ignoring malformed message on %s: %s (payload=%s)",
                     msg.topic, exc, msg.payload)
        return

    message_count += 1

    with reading_lock:
        prev = latest_reading
        latest_reading = {"wind": wind, "dir": direction}

    log.debug("Message #%d received: wind=%.2f dir=%.1f (previous=%s)",
              message_count, wind, direction, prev)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Subscribe to MQTT wind data and store in MariaDB"
    )
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug output (show MQTT messages, DB queries, thread activity)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.debug:
        log.debug("Debug mode enabled")
        log.debug("Config: MQTT=%s:%d topic=%s", MQTT_BROKER, MQTT_PORT, MQTT_TOPIC)
        log.debug("Config: DB=%s:%d db=%s user=%s",
                   DB_HOST, DB_PORT, DB_NAME, DB_USER)
        log.debug("Config: store_interval=%ds", STORE_INTERVAL_SECONDS)

    # Verify DB connectivity at startup
    try:
        conn = get_db_connection()
        conn.close()
        log.info("Database connection verified")
    except mysql.connector.Error as exc:
        log.error("Cannot connect to database: %s", exc)
        sys.exit(1)

    # Start the background writer thread
    writer = threading.Thread(target=periodic_writer, daemon=True)
    writer.start()

    # Set up and connect the MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    if args.debug:
        client.enable_logger(log)

    log.info("Connecting to MQTT broker %s:%d ...", MQTT_BROKER, MQTT_PORT)
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

    # Blocking loop – handles reconnects automatically
    client.loop_forever()


if __name__ == "__main__":
    main()
