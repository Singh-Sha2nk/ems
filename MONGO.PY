import paho.mqtt.client as mqtt
from datetime import datetime
import json
import requests
import time
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Check for internet connectivity:

url = "http://google.com"
timeout = 10

def check_connection():
    try:
        # requesting URL
        request = requests.get(url, timeout=timeout)
        print("Internet is on")
        return "ON"
    except (requests.ConnectionError, requests.Timeout) as exception:
        print("Internet is off")
        return "OFF"

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if check_connection() == "ON":
        print("Internet Connection ok")

        if rc == 0:
            print("Connected to MQTT broker")
            client.subscribe("JBMGroup/em3phase/#")
        else:
            raise Exception("Connection failed with return code: {}".format(rc))
    elif check_connection() == "OFF":
        print("No internet connection..")
    

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    rawdata = str(msg.payload.decode("utf-8"))

    try:
        data = json.loads(rawdata)
        print("Received JSON data:", data)

        if not isinstance(data, dict):
            print("Received data is not a valid JSON object.")
            return

        # Check if "tags" data exists as a separate field
        tag_data = data.get("tags")
        if tag_data is None:
            # If "tags" data doesn't exist, assume the whole message is the tags data
            tag_data = data

        print("Tags data:", tag_data)

        if not isinstance(tag_data, dict):
            print("Tags data not found or is not a valid JSON object.")
            return

        # Rest of your code to process and write data to InfluxDB

        current_time = str(datetime.now())
        dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S.%f')
        formatted_datetime = str(dt.strftime('%Y-%m-%d %H:%M:%S'))
        tag_data["currTime"] = formatted_datetime

        print(tag_data)
        print(type(tag_data))

        # Write data to InfluxDB
        with InfluxDBClient(url="http://localhost:8086", token="yq-ltfF4RCiLbSBpTF0E48b_6rNwsN8mqEway_5Q5nctEdRAXFDaFiJr_WUL9kpAVGBuEZVI8jbNfzFlWX7vSg==", org="JBM") as _client:
            with _client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000, jitter_interval=2_000,
                                                             retry_interval=5_000, max_retries=5, max_retry_delay=30_000,
                                                             exponential_base=2)) as _write_client:
                try:
                    _write_client.write("EMS", "JBM", {"measurement": "Test47", "fields": tag_data})
                except Exception as e:
                    print("Error writing data to InfluxDB: ", e)

    except Exception as e:
        print("Error occurred: ", e)

# MQTT client setup
timestamp = datetime.timestamp(datetime.now())
client_id = str(timestamp)
client = mqtt.Client(client_id)
client.on_connect = on_connect
client.on_message = on_message

def run():
    try:
        client.connect("3.7.85.13", 1883, 60)
        client.loop_forever()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    while True:
        if check_connection() == "ON":
            print("Connection ok")
            try:
                run()
            except Exception as e:
                print(e)
        elif check_connection() == "OFF":
            print("Connection OFF")
            time.sleep(5)
