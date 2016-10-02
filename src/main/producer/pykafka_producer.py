import sys
import random
from pykafka import KafkaClient

class Producer():
    daemon = True

    def __init__(self):
        self.data_file = open('/home/ubuntu/opt/realtimeAnomalies/src/main/test/kddcup.testdata.unlabeled', 'r')
        self.mem_data = []
        for record in self.data_file:
            self.mem_data.append(record)

    def run(self):
        client = KafkaClient("ec2-52-45-1-31.compute-1.amazonaws.com:9092,ec2-52-45-55-34.compute-1.amazonaws.com:9092,ec2-52-44-32-252.compute-1.amazonaws.com:9092,ec2-23-22-195-205.compute-1.amazonaws.com:9092")
        topic = client.topics["traffic_data4"]
        random.seed()

        with topic.get_producer() as producer:
            while True:
                idx = random.randint(0, len(self.mem_data) - 1)
                msg = self.mem_data[idx]
                producer.produce(msg)

if __name__ == "__main__":
    prod = Producer()
    prod.run()
