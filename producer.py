from kafka import KafkaProducer
import json
from faker import Faker
import time
import pandas as pd
from collections.abc import MutableMapping
from yahooquery import Ticker
from itertools import islice

class kafka_Producer():

    def flatten_dict(self,d: MutableMapping, sep: str= '.') -> MutableMapping:
        [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
        return flat_dict
    
    def take(self,n, iterable):
        "Return first n items of the iterable as a list"
        return dict(islice(iterable, n))

    def send_data(self):
        symbols=['goog']
        maang = Ticker(symbols, asynchronous=True)
        def json_serializer(data):
            return json.dumps(data).encode('utf-8')
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)
        # def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
        #     [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
        #     return flat_dict
        for i in range(0,1000):
            summary_details = maang.summary_detail
            flat_dict=self.flatten_dict(summary_details)
            n_items = self.take(5, flat_dict.items())
            n_items['goog.id']=i+1
            n_items = dict(sorted(n_items.items()))
            n_items=list(n_items.values())
            msg=' '.join(str(e) for e in n_items)
            print((msg))
            try:
                producer.send("stock-market",msg)
            except Exception as e:
                print("Producer not working: ",e)
            time.sleep(2)


if __name__ == "__main__":
    kafka_producer=kafka_Producer()
    kafka_producer.send_data()