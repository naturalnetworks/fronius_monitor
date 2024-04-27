"""
fronius_monitor.py

Poll Fronius Data Manager REST API for telemetry data and publish it to MQTT broker.

Dependencies:
    - Paho MQTT client library (https://pypi.org/project/paho-mqtt/)
    - requests library (https://pypi.org/project/requests/)

Author: Ben Johns (bjohns@naturalnetworks.net)
"""

import time
import json
import requests
import paho.mqtt.client as mqtt


# Fronius IP address
froniusIp = "fronius.home.arpa"

# MQTT Broker details
broker_address = "nas.home.arpa"
broker_port = 1883
mqtt_topic = "test/fronius"

def get_froniusData(ip, endpoint):
    url = f"http://{ip}{endpoint}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data from Fronius API: {response.status_code}")
        return None

def main():
    try:
        # Retrieve data from Fronius API
        froniusData = get_froniusData(froniusIp, "/solar_api/v1/GetPowerFlowRealtimeData.fcgi") 
        pvLoad = froniusData['Body']['Data']['Site']['P_Load']
        pvGeneration = froniusData['Body']['Data']['Site']['P_PV']
        pvGrid = froniusData['Body']['Data']['Site']['P_Grid']

        froniusData_common = get_froniusData(froniusIp, "/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=CommonInverterData")
        pvGridVoltage = froniusData_common['Body']['Data']['UAC']['Value']
        pvGridFrequency = froniusData_common['Body']['Data']['FAC']['Value']

        froniusData_meter = get_froniusData(froniusIp, "/solar_api/v1/GetMeterRealtimeData.cgi?Scope=Device&DeviceId=0")
        pvMeterGridVoltage = froniusData_meter['Body']['Data']['Voltage_AC_Phase_1']
        pvMeterGridFrequency = froniusData_meter['Body']['Data']['Frequency_Phase_Average']
        pvMeterGridPf = froniusData_meter['Body']['Data']['PowerFactor_Phase_1']

        # Generate MQTT payload
        device_id = 1  # Assuming device_id is 1
        mqtt_payload = {
            "sensorID": device_id,
            "timecollected": int(time.time()),
            "pvImport": pvGeneration - pvLoad,
            "pvExport": pvLoad - pvGeneration,
            "pvGeneration": pvGeneration,
            "pvLoad": pvLoad,
            "gridVoltage": pvGridVoltage,
            "gridFrequency": pvGridFrequency,
            "gridVoltage1": pvMeterGridVoltage,
            "gridFrequency1": pvMeterGridFrequency,
            "gridPf": pvMeterGridPf
        }

        # Publish data to MQTT broker
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.connect(broker_address, broker_port, 60)
        client.loop_start()
        client.publish(mqtt_topic, json.dumps(mqtt_payload))
        client.loop_stop()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
