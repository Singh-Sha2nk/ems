import paho.mqtt.client as mqtt
from datetime import datetime
import json
import pymongo
import requests
import urllib.request
import time
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Check for internet connectivity:

url = "http://google.com"
timeout = 10
def check_connection():
    try:
    # requesting URL
        request = requests.get(url,
                            timeout=timeout)
        print("Internet is on")
        return "ON"
 
# catching exception
    except (requests.ConnectionError,
            requests.Timeout) as exception:
        print("Internet is off")
        return "OFF"



# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

    ''' Check for internet connectivity'''
    if check_connection() == "ON":
        print("Internet Connection ok")

        if rc == 0:
            print("Connected to MQTT broker")
            client.subscribe("JBMGroup/MachineData/#")
        else:
            raise Exception("Connection failed with return code: {}".format(rc))
    elif check_connection() == "":
        print("No internet connection..")
    

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    
 
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic + " "+str(msg.payload))
    rawdata = str(msg.payload.decode("utf-8"))
    data = json.loads(rawdata)
    tag_data = data.get("tags")
    try:
        current_time = str(datetime.now())
        dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S.%f')
        formatted_datetime = str(dt.strftime('%Y-%m-%d %H:%M:%S'))
        tag_data["currTime"] = formatted_datetime
       
    except Exception as e:
        print("Error occured: ",e)

    print(tag_data)
    print(type(tag_data))

    # insert this data to mongodb:

    with InfluxDBClient(url="http://localhost:8086", token="7gapN5GqkuxfDiOkkS9r-kH94GQ8p89i9A7SP87YNSkYGpx9D5NNRBKOleFHfBe6UEDitZx84FvzB_jfsEGenQ==", org="JBM") as _client:

            with _client.write_api(write_options=WriteOptions(batch_size=500,
                                                          flush_interval=10_000,
                                                          jitter_interval=2_000,
                                                          retry_interval=5_000,
                                                          max_retries=5,
                                                          max_retry_delay=30_000,
                                                          exponential_base=2)) as _write_client:


                try:
                    _write_client.write("EMS", "JBM", {"measurement": "Test46",  "fields": tag_data})
                except Exception as e:
                    print("Error Occured... ",e)
    #     myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    #     print("connected to database")
    #     mydb = myclient["EMS"]
    #     mycol = mydb["EMSJ2"]
    #     EMSData = tag_data
    #     x = mycol.insert_one(EMSData)
    #     myclient.close()
    #     print("Connection closed..")
    # except Exception as e:
    #     print(e)

    # return tag_data
     
        

# function to push data to mongo and get data from above function:


timestamp = datetime.timestamp(datetime.now())
client_id = str(timestamp)

client = mqtt.Client(client_id)
client.on_connect = on_connect
client.on_message = on_message


def run():
    try:
        client.connect("3.7.85.13", 1883, 60)
        client.loop_start()
        while True:
            pass
                          
    except Exception as e:
        print(e)


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

    



