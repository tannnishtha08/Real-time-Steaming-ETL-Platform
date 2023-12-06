import kafka
from kafka import KafkaConsumer
import json
from json import loads
import time
from collections.abc import MutableMapping
import pandas as pd

class kafka_Consumer():

    def consume_data(self):
        consumer = KafkaConsumer(
        'stock-market',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='group_a')
        print("starting a consumer")
        #     return consumer
        msg_list=[]
        c=0
        for msg in consumer:
            print("Key", msg.key)
            print("Message: ", msg.value)
            time.sleep(3)

if __name__ == "__main__":
    kafka_consumer=kafka_Consumer()
    kafka_consumer.consume_data()
    