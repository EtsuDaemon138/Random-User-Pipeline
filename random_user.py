import requests
import json
from pykafka import KafkaClient
import time
def get_data():
    url ="https://randomuser.me/api/"
    res = requests.get(url)

    data = res.json()
    return data["results"][0]

def format_data(res):
    data = {}
    data["full name"] = res["name"]["title"]+'. ' +res["name"]["first"]+' '+res["name"]["last"]
    data["gender"] = res["gender"]
    data["address"]= str(res["location"]["street"]["number"])+' '+res["location"]["street"]["name"]+ ','+ \
                    res["location"]["city"]+ ','+ res["location"]["state"]+ ','+res["location"]["country"]
    data["lat,long"]= res["location"]["coordinates"]["latitude"] + ','+ res["location"]["coordinates"]["latitude"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["password"] = res["login"]["password"]
    data["dob"] = res["dob"]["date"]
    data["phone"] = res["phone"]
 
    return data

def streaming_data():
    client = KafkaClient("localhost:9092")
    topic = client.topics["user_created"]
    producer = topic.get_producer()
    while True:
        res = get_data()
        print(res)
        producer.produce(json.dumps(res).encode('utf-8'))
        time.sleep(3)
streaming_data()
