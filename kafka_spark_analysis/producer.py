import requests
import time
from kafka import KafkaProducer
import random
# URL = http://localhost:8080/TicketService/search
producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    lat = random.uniform(0, 24) + 25
    lon = random.uniform(0, 60) + 70
    lon = -lon
    URL = "http://localhost:8080/TicketService/search?lat=" + str(lat) + "&lon=" + str(lon)
    r = requests.get(url = URL)
    data = r.json()
    if len(data) ==  0:
        # print("No such area")
        continue
    for i in range(len(data)):
        categories = data[i]['categories']
        time.sleep(0.1)
        for j in range(len(categories)):
            producer.send('category', categories[j].encode('utf8'))
