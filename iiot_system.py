#!/usr/bin/env python3  # https://docs.python.org/3/using/unix.html#python-interface
# -*- coding: utf-8 -*-  # https://docs.python.org/3/library/functions.html#open

# Import standard libraries with official docs referenced on each line

import os  # https://docs.python.org/3/library/os.html
import sys  # https://docs.python.org/3/library/sys.html
import json  # https://docs.python.org/3/library/json.html
import time  # https://docs.python.org/3/library/time.html
import random  # https://docs.python.org/3/library/random.html
import argparse  # https://docs.python.org/3/library/argparse.html
import logging  # https://docs.python.org/3/library/logging.html
import sqlite3  # https://docs.python.org/3/library/sqlite3.html
from datetime import datetime  # https://docs.python.org/3/library/datetime.html

# Third-party libraries with official docs linked

# paho-mqtt client for MQTT publish/subscribe
# Docs: https://www.eclipse.org/paho/index.php?page=clients/python/docs/index.php
import paho.mqtt.client as mqtt  # https://www.eclipse.org/paho/index.php?page=clients/python/docs/index.php

# opcua library for building an OPC UA server
# Docs: https://freeopcua.github.io/python-opcua/
from opcua import ua, Server  # https://freeopcua.github.io/python-opcua/

# Streamlit for a lightweight dashboard
# Docs: https://docs.streamlit.io/
import streamlit as st  # https://docs.streamlit.io/

# ---------- Configuration Helpers ----------

def env(key: str, default: str) -> str:
    """Read environment variable with a default."""  # https://docs.python.org/3/library/os.html#os.getenv
    return os.getenv(key, default)  # https://docs.python.org/3/library/os.html#os.getenv

def mk_logger(name: str = __name__) -> logging.Logger:
    """Create a configured logger."""  # https://docs.python.org/3/library/logging.html#logging.getLogger
    logger = logging.getLogger(name)  # https://docs.python.org/3/library/logging.html#logging.getLogger
    if not logger.handlers:  # https://docs.python.org/3/library/logging.html#logging.Logger.handlers
        logger.setLevel(logging.INFO)  # https://docs.python.org/3/library/logging.html#logging.Logger.setLevel
        handler = logging.StreamHandler(sys.stdout)  # https://docs.python.org/3/library/logging.handlers.html#streamhandler
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')  # https://docs.python.org/3/library/logging.html#logging.Formatter
        handler.setFormatter(fmt)  # https://docs.python.org/3/library/logging.html#logging.Handler.setFormatter
        logger.addHandler(handler)  # https://docs.python.org/3/library/logging.html#logging.Logger.addHandler
    return logger  # https://docs.python.org/3/library/functions.html#return

log = mk_logger("iiot")  # https://docs.python.org/3/library/logging.html

# ---------- SQLite Utilities ----------

DB_PATH = env("IIOT_DB", "telemetry.db")  # https://docs.python.org/3/library/os.html#os.getenv

def db_init(db_path: str = DB_PATH) -> None:
    """Initialize SQLite schema for telemetry."""  # https://docs.python.org/3/library/sqlite3.html
    conn = sqlite3.connect(db_path)  # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    try:  # https://docs.python.org/3/tutorial/errors.html#handling-exceptions
        cur = conn.cursor()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.cursor
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              sensor TEXT NOT NULL,
              temperature REAL NOT NULL,
              humidity REAL NOT NULL
            )
            """
        )  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.execute
        conn.commit()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.commit
    finally:
        conn.close()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.close

def db_insert(ts: str, sensor: str, temperature: float, humidity: float, db_path: str = DB_PATH) -> None:
    """Insert a telemetry row."""  # https://docs.python.org/3/library/sqlite3.html
    conn = sqlite3.connect(db_path)  # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    try:
        cur = conn.cursor()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.cursor
        cur.execute(
            "INSERT INTO telemetry(ts, sensor, temperature, humidity) VALUES (?, ?, ?, ?)",
            (ts, sensor, temperature, humidity),
        )  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.execute
        conn.commit()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.commit
    finally:
        conn.close()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.close

def db_latest(n: int = 200, db_path: str = DB_PATH):
    """Return latest N rows for dashboard."""  # https://docs.python.org/3/library/sqlite3.html
    conn = sqlite3.connect(db_path)  # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    try:
        cur = conn.cursor()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.cursor
        cur.execute("SELECT ts, sensor, temperature, humidity FROM telemetry ORDER BY id DESC LIMIT ?", (n,))  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.execute
        return cur.fetchall()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.fetchall
    finally:
        conn.close()  # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.close

# ---------- MQTT Publisher (Simulated Sensors) ----------

MQTT_HOST = env("IIOT_MQTT_HOST", "127.0.0.1")  # https://docs.python.org/3/library/os.html#os.getenv
MQTT_PORT = int(env("IIOT_MQTT_PORT", "1883"))  # https://docs.python.org/3/library/functions.html#int
MQTT_USER = env("IIOT_MQTT_USER", "")  # https://docs.python.org/3/library/os.html#os.getenv
MQTT_PASS = env("IIOT_MQTT_PASS", "")  # https://docs.python.org/3/library/os.html#os.getenv
MQTT_TOPIC = env("IIOT_MQTT_TOPIC", "factory/line1/sensor/telemetry")  # https://docs.python.org/3/library/os.html#os.getenv
PUBLISH_INTERVAL = float(env("IIOT_PUBLISH_INTERVAL", "2.0"))  # https://docs.python.org/3/library/functions.html#float

def make_mqtt_client(client_id: str) -> mqtt.Client:
    """Create and configure a Paho MQTT client."""  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#mqtt-client
    client = mqtt.Client(client_id=client_id, clean_session=True)  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#client
    if MQTT_USER:  # https://docs.python.org/3/tutorial/controlflow.html#if-statements
        client.username_pw_set(MQTT_USER, MQTT_PASS)  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#username-pw-set
    client.on_connect = lambda c, u, f, rc: log.info(f"MQTT connected rc={rc}")  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#on-connect
    client.on_publish = lambda c, u, mid: log.debug(f"published mid={mid}")  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#on-publish
    return client  # https://docs.python.org/3/library/functions.html#return

def run_publisher(sensor_name: str = "sensor-001") -> None:
    """Run a loop that publishes simulated telemetry."""  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#publishing
    db_init()  # https://docs.python.org/3/library/sqlite3.html
    client = make_mqtt_client(client_id=f"pub-{sensor_name}")  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#client
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#connect
    client.loop_start()  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#loop-start
    log.info("Publisher started")  # https://docs.python.org/3/library/logging.html#logging.Logger.info
    try:
        while True:  # https://docs.python.org/3/reference/compound_stmts.html#the-while-statement
            now = datetime.utcnow().isoformat()  # https://docs.python.org/3/library/datetime.html#datetime.datetime.isoformat
            temp = round(random.uniform(20.0, 30.0), 2)  # https://docs.python.org/3/library/random.html#random.uniform
            hum = round(random.uniform(30.0, 70.0), 2)  # https://docs.python.org/3/library/random.html#random.uniform
            payload = {"ts": now, "sensor": sensor_name, "temperature": temp, "humidity": hum}  # https://docs.python.org/3/library/stdtypes.html#dict
            j = json.dumps(payload)  # https://docs.python.org/3/library/json.html#json.dumps
            result = client.publish(MQTT_TOPIC, j, qos=1, retain=False)  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#publish
            status = result[0] if isinstance(result, tuple) else result.rc  # https://docs.python.org/3/library/functions.html#isinstance
            if status == mqtt.MQTT_ERR_SUCCESS:  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#constants
                log.info(f"Published {j} to {MQTT_TOPIC}")  # https://docs.python.org/3/library/logging.html#logging.Logger.info
            else:
                log.error(f"Publish failed rc={status}")  # https://docs.python.org/3/library/logging.html#logging.Logger.error
            time.sleep(PUBLISH_INTERVAL)  # https://docs.python.org/3/library/time.html#time.sleep
    except KeyboardInterrupt:  # https://docs.python.org/3/tutorial/errors.html#handling-exceptions
        log.info("Publisher stopping")  # https://docs.python.org/3/library/logging.html#logging.Logger.info
    finally:
        client.loop_stop()  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#loop-stop
        client.disconnect()  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#disconnect

# ---------- MQTT Subscriber (ingest → SQLite) ----------

def run_subscriber() -> None:
    """Subscribe to MQTT and write telemetry into SQLite."""  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#subscriptions
    db_init()  # https://docs.python.org/3/library/sqlite3.html
    client = make_mqtt_client(client_id="sub-telemetry")  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#client

    def on_message(client, userdata, msg):  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#on-message
        try:
            payload = json.loads(msg.payload.decode("utf-8"))  # https://docs.python.org/3/library/json.html#json.loads
            ts = payload["ts"]  # https://docs.python.org/3/library/stdtypes.html#dict
            sensor = payload["sensor"]
            temperature = float(payload["temperature"])  # https://docs.python.org/3/library/functions.html#float
            humidity = float(payload["humidity"])  # https://docs.python.org/3/library/functions.html#float
            db_insert(ts, sensor, temperature, humidity)  # https://docs.python.org/3/library/sqlite3.html
            log.info(f"Ingested row from {sensor}")  # https://docs.python.org/3/library/logging.html#logging.Logger.info
        except Exception as e:  # https://docs.python.org/3/tutorial/errors.html#handling-exceptions
            log.exception(f"on_message error: {e}")  # https://docs.python.org/3/library/logging.html#logging.Logger.exception

    client.on_message = on_message  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#on-message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#connect
    client.subscribe(MQTT_TOPIC, qos=1)  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#subscribe
    client.loop_forever()  # https://www.eclipse.org/paho/files/pypi/paho-mqtt/html/index.html#loop-forever

# ---------- OPC UA Server ----------

OPCUA_ENDPOINT = env("IIOT_OPCUA_ENDPOINT", "opc.tcp://0.0.0.0:4840")  # https://freeopcua.github.io/python-opcua/

def run_opcua_server() -> None:
    """Run a minimal OPC UA server that exposes telemetry variables."""  # https://freeopcua.github.io/python-opcua/
    server = Server()  # https://freeopcua.github.io/python-opcua/
    server.set_endpoint(OPCUA_ENDPOINT)  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.server.server.Server.set_endpoint
    uri = "http://example.org/iiot"  # https://freeopcua.github.io/python-opcua/
    idx = server.register_namespace(uri)  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.server.server.Server.register_namespace
    objects = server.get_objects_node()  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.server.server.Server.get_objects_node
    iiot = objects.add_object(idx, "IIoT")  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.add_object
    v_temp = iiot.add_variable(idx, "Temperature", 0.0)  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.add_variable
    v_hum = iiot.add_variable(idx, "Humidity", 0.0)  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.add_variable
    v_sensor = iiot.add_variable(idx, "Sensor", "sensor-001")  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.add_variable
    v_temp.set_writable()  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.set_writable
    v_hum.set_writable()  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.set_writable
    v_sensor.set_writable()  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.set_writable

    db_init()  # https://docs.python.org/3/library/sqlite3.html
    server.start()  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.server.server.Server.start
    log.info(f"OPC UA server started at {OPCUA_ENDPOINT}")  # https://docs.python.org/3/library/logging.html#logging.Logger.info
    try:
        while True:  # https://docs.python.org/3/reference/compound_stmts.html#the-while-statement
            rows = db_latest(1)  # https://docs.python.org/3/library/sqlite3.html
            if rows:
                ts, sensor, temperature, humidity = rows[0]  # https://docs.python.org/3/library/stdtypes.html#typesseq
                v_temp.set_value(float(temperature))  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.set_value
                v_hum.set_value(float(humidity))  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.set_value
                v_sensor.set_value(str(sensor))  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.common.node.Node.set_value
            time.sleep(1.0)  # https://docs.python.org/3/library/time.html#time.sleep
    except KeyboardInterrupt:  # https://docs.python.org/3/tutorial/errors.html#handling-exceptions
        log.info("OPC UA server stopping")  # https://docs.python.org/3/library/logging.html#logging.Logger.info
    finally:
        server.stop()  # https://freeopcua.github.io/python-opcua/0.98.13/server.html#opcua.server.server.Server.stop

# ---------- Streamlit Dashboard ----------

def run_dashboard() -> None:
    """Run a Streamlit dashboard that reads from SQLite and charts values."""  # https://docs.streamlit.io/
    st.set_page_config(page_title="IIoT Dashboard", layout="wide")  # https://docs.streamlit.io/develop/api-reference/utilities/st.set_page_config
    st.title("IIoT Machine Monitoring System")  # https://docs.streamlit.io/develop/api-reference/text/st.title
    st.caption("Live telemetry from MQTT via SQLite")  # https://docs.streamlit.io/develop/api-reference/text/st.caption

    db_init()  # https://docs.python.org/3/library/sqlite3.html
    n = st.sidebar.slider("Rows", min_value=50, max_value=1000, value=200, step=50)  # https://docs.streamlit.io/develop/api-reference/widgets/st.slider
    data = db_latest(n)  # https://docs.python.org/3/library/sqlite3.html

    # Transform rows into lists for Streamlit charts
    ts = [r[0] for r in data][::-1]  # https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions
    temps = [r[2] for r in data][::-1]  # https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions
    hums = [r[3] for r in data][::-1]  # https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions

    st.subheader("Temperature (°C)")  # https://docs.streamlit.io/develop/api-reference/text/st.subheader
    st.line_chart({"Temperature": temps})  # https://docs.streamlit.io/develop/api-reference/charts/st.line_chart

    st.subheader("Humidity (%)")  # https://docs.streamlit.io/develop/api-reference/text/st.subheader
    st.line_chart({"Humidity": hums})  # https://docs.streamlit.io/develop/api-reference/charts/st.line_chart

    with st.expander("Raw rows"):  # https://docs.streamlit.io/develop/api-reference/layout/st.expander
        st.write(data)  # https://docs.streamlit.io/develop/api-reference/write-magic/st.write

    st.sidebar.markdown("**Hint:** Keep publisher & subscriber running for live data.")  # https://docs.streamlit.io/develop/api-reference/text/st.markdown

# ---------- CLI ----------

def parse_args(argv=None):
    """Parse command line arguments."""  # https://docs.python.org/3/library/argparse.html
    p = argparse.ArgumentParser(description="IIoT Machine Monitoring System")  # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser
    p.add_argument("--mode", choices=["publisher", "subscriber", "opcua", "dashboard"], required=True,
                   help="Which component to run")  # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.add_argument
    p.add_argument("--sensor", default="sensor-001", help="Sensor name for publisher mode")  # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.add_argument
    return p.parse_args(argv)  # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args

def main(argv=None) -> int:
    """Main entrypoint that dispatches to the selected mode."""  # https://docs.python.org/3/library/functions.html#callable
    args = parse_args(argv)  # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args
    if args.mode == "publisher":  # https://docs.python.org/3/tutorial/controlflow.html#if-statements
        run_publisher(sensor_name=args.sensor)  # see above
    elif args.mode == "subscriber":  # https://docs.python.org/3/tutorial/controlflow.html#if-statements
        run_subscriber()  # see above
    elif args.mode == "opcua":  # https://docs.python.org/3/tutorial/controlflow.html#if-statements
        run_opcua_server()  # see above
    elif args.mode == "dashboard":  # https://docs.python.org/3/tutorial/controlflow.html#if-statements
        run_dashboard()  # see above
    return 0  # https://docs.python.org/3/library/constants.html#True

if __name__ == "__main__":  # https://docs.python.org/3/library/__main__.html
    raise SystemExit(main())  # https://docs.python.org/3/library/constants.html#SystemExit
