from sys import argv
from threading import Thread

from kafka import KafkaConsumer
from kafka import KafkaProducer

def read_messages(consumer):
    for msg in consumer:
        print(msg.key.decode() + ': ' + msg.value.decode())

n = len(argv)
if (n != 3):
    raise Exception("This program must accept 2 arguments")

usr1, usr2 = argv[1], argv[2]
if '-' in usr1:
    raise Exception("First user name should not contain '-'")
if '-' in usr2:
    raise Exception("Second user name should not contain '-'")

TOPIC_NAME = usr1 + '-' + usr2 if usr1 < usr2 else usr2 + '-' + usr1
KAFKA_SERVER = 'localhost:9092'

consumer = KafkaConsumer(TOPIC_NAME,  auto_offset_reset='earliest')
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

thread = Thread(target=read_messages, args=(consumer,))
thread.start()

while True:
    message_input = input()
    producer.send(topic=TOPIC_NAME, key=usr1.encode('utf-8'), value=message_input.encode('utf_8'))