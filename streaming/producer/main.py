import requests
import os
import time
from kafka import KafkaProducer
import kafka.errors
import csv, json

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "accidents"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(1)

response_API = requests.get('https://api.worldweatheronline.com/premium/v1/past-weather.ashx?q=New+York&date=2012-10-01&enddate=2012-10-30&key=2543a789e544477b9e7142621221004&format=json', verify=False)

data = response_API.json()
print(data)
for weather in data["data"]["weather"]:
    try:
        data = {}
        print(weather)
        data["Date"] = weather["date"]
        data["Sunrise"] = weather["astronomy"][0]["sunrise"]
        data["Sunset"] = weather["astronomy"][0]["sunset"]
        data["Max_temp"] = weather["maxtempC"]
        data["Min_temp"] = weather["mintempC"]
        data["Avg_temp"] = weather["avgtempC"]
        data["Total_snow"] = weather["totalSnow_cm"]
        data["Temp_0"] = weather["hourly"][0]["tempC"]
        data["Wind_0"] = weather["hourly"][0]["windspeedKmph"]
        data["Visibility_0"] = weather["hourly"][0]["visibility"]
        data["Precipitation_0"] = weather["hourly"][0]["precipMM"]
        data["Temp_1"] = weather["hourly"][1]["tempC"]
        data["Wind_1"] = weather["hourly"][1]["windspeedKmph"]
        data["Visibility_1"] = weather["hourly"][1]["visibility"]
        data["Precipitation_1"] = weather["hourly"][1]["precipMM"]
        data["Temp_2"] = weather["hourly"][2]["tempC"]
        data["Wind_2"] = weather["hourly"][2]["windspeedKmph"]
        data["Visibility_2"] = weather["hourly"][2]["visibility"]
        data["Precipitation_2"] = weather["hourly"][2]["precipMM"]
        data["Temp_3"] = weather["hourly"][3]["tempC"]
        data["Wind_3"] = weather["hourly"][3]["windspeedKmph"]
        data["Visibility_3"] = weather["hourly"][3]["visibility"]
        data["Precipitation_3"] = weather["hourly"][3]["precipMM"]
        data["Temp_4"] = weather["hourly"][4]["tempC"]
        data["Wind_4"] = weather["hourly"][4]["windspeedKmph"]
        data["Visibility_4"] = weather["hourly"][4]["visibility"]
        data["Precipitation_4"] = weather["hourly"][4]["precipMM"]
        data["Temp_5"] = weather["hourly"][5]["tempC"]
        data["Wind_5"] = weather["hourly"][5]["windspeedKmph"]
        data["Visibility_5"] = weather["hourly"][5]["visibility"]
        data["Precipitation_5"] = weather["hourly"][5]["precipMM"]
        data["Temp_6"] = weather["hourly"][6]["tempC"]
        data["Wind_6"] = weather["hourly"][6]["windspeedKmph"]
        data["Visibility_6"] = weather["hourly"][6]["visibility"]
        data["Precipitation_6"] = weather["hourly"][6]["precipMM"]
        data["Temp_7"] = weather["hourly"][7]["tempC"]
        data["Wind_7"] = weather["hourly"][7]["windspeedKmph"]
        data["Visibility_7"] = weather["hourly"][7]["visibility"]
        data["Precipitation_7"] = weather["hourly"][7]["precipMM"]

        print(data)
        data_dumps = json.dumps(data)
        print(data_dumps)
        producer.send(TOPIC,key=bytes(weather["date"], 'utf-8'),value=bytes(data_dumps,"utf-8"))
        print("POSLAO")
        time.sleep(5)
    except Exception as e:
        print('Exception')
        print(e)
        pass



