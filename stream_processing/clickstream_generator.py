import random
import time 
import json 
from kafka import KafkaProducer
from datetime import datetime

producer=KafkaProducer(bootstrap_servers=['localhost:9092'])

users=["f_{i}" for i in range(1,101)]
products=["f_{i}" for i in range (1,21)]

while True:
    click_event={
    "user_id":random.choice(users),
    "product_id":random.choice(products),
    "click_time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "session_id":"f_sess{random.randint(1000,9999)}"
    }

    producer.send('user_clicks',json.dumps(click_event).encode('utf=8'))
    print(f"Sent succesfully:{click_event}")
    time.sleep(random.uniform(0.1,1.0))

