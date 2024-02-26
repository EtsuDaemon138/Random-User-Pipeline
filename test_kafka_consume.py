from pykafka import KafkaClient

client = KafkaClient("localhost:9092")
topic = client.topics["user_created"]

consumer = topic.get_simple_consumer()

latest_offset = consumer.fetch_offsets()
print(latest_offset)
consumer.fetch()

for message in consumer:
    if message is not None:
        print("Received message: {}".format(message.value.decode('utf-8')))
