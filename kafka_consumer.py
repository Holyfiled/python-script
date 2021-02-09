# -*- coding: utf-8 -*-
from kafka import KafkaConsumer


class Kafka_Consumer:
    def __init__(self, bootstrap_servers, group_id, topic):
        self.consumer = KafkaConsumer(topic,
                                      bootstrap_servers = bootstrap_servers,
                                      group_id=group_id,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=False
                                      )


def main():
    bootstrap_servers = '111.229.152.122:9092'
    group_id = 'test_consumer_group_1'
    topic_id = 'topic_test_1'
    consumer_1 = Kafka_Consumer(bootstrap_servers, group_id, topic_id)



if __name__ == '__main__':
    main()
