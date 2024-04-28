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
import traceback
import requests
import paho.mqtt.client as mqtt

# Fronius IP address
froniusIp = "fronius.home.arpa"

# MQTT Broker details
broker_address = "nas.home.arpa"
broker_port = 1883
mqtt_topic = "home/fronius"

def get_froniusData(ip, endpoint):
    url = f"http://{ip}{endpoint}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data from Fronius API: {response.status_code}")
        return None

def main(interval_seconds):
    while True:
        try:
            # Retrieve data from Fronius API
            froniusData = get_froniusData(froniusIp, "/solar_api/v1/GetPowerFlowRealtimeData.fcgi")
            pvLoad = froniusData['Body']['Data']['Site'].get('P_Load')
            pvGeneration = froniusData['Body']['Data']['Site'].get('P_PV')
            pvGrid = froniusData['Body']['Data']['Site'].get('P_Grid')

            # Check for None values and assign default if necessary
            pvLoad = pvLoad if pvLoad is not None else 0
            pvGeneration = pvGeneration if pvGeneration is not None else 0
            pvGrid = pvGrid if pvGrid is not None else 0

            froniusData_common = get_froniusData(froniusIp, "/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=CommonInverterData")
            pvGridVoltage = froniusData_common['Body']['Data'].get('UAC', {}).get('Value')
            pvGridFrequency = froniusData_common['Body']['Data'].get('FAC', {}).get('Value')

            # Check for None values and assign default if necessary
            pvGridVoltage = pvGridVoltage if pvGridVoltage is not None else 0
            pvGridFrequency = pvGridFrequency if pvGridFrequency is not None else 0

            froniusData_meter = get_froniusData(froniusIp, "/solar_api/v1/GetMeterRealtimeData.cgi?Scope=Device&DeviceId=0")
            pvMeterGridVoltage = froniusData_meter['Body']['Data'].get('Voltage_AC_Phase_1')
            pvMeterGridFrequency = froniusData_meter['Body']['Data'].get('Frequency_Phase_Average')
            pvMeterGridPf = froniusData_meter['Body']['Data'].get('PowerFactor_Phase_1')

            # Check for None values and assign default if necessary
            pvMeterGridVoltage = pvMeterGridVoltage if pvMeterGridVoltage is not None else 0
            pvMeterGridFrequency = pvMeterGridFrequency if pvMeterGridFrequency is not None else 0
            pvMeterGridPf = pvMeterGridPf if pvMeterGridPf is not None else 0

            # Check if both pvGeneration and pvLoad are valid
            if pvGeneration is not None and pvLoad is not None:
                 pvImport = pvGeneration - pvLoad
                 pvExport = pvLoad - pvGeneration
            else:
                 pvImport = 0
                 pvExport = 0

            # Generate MQTT payload
            device_id = 1  # Assuming device_id is 1
            mqtt_payload = {
                "sensorID": device_id,
                "timecollected": int(time.time()),
                "pvImport": pvImport,
                "pvExport": pvExport,
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
            client.publish(mqtt_topic, json.dumps(mqtt_payload))
            client.disconnect()

            time.sleep(interval_seconds)

        except Exception as e:
            # print(f"An error occurred: {str(e)}")
            print("An error occurred:")
            print(str(e))
            traceback.print_exc()

if __name__ == "__main__":
    interval_seconds = 5
    main(interval_seconds)