import random
import sys
import six
import time
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)
        self.data_file = open('/home/ubuntu/dev/massieve/src/main/test/kddcup.testdata.unlabeled', 'r')
        self.mem_data = []
        for record in self.data_file:
            self.mem_data.append(record)

    def produce_msgs(self, source_symbol):
	random.seed()
        while True:
	    idx = random.randint(0, len(self.mem_data) - 1)
            str_fmt = "{}"
            message_content = str_fmt.format(self.mem_data[idx])
            self.producer.send_messages('traffic_data4', source_symbol, message_content)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
