# -*- coding: utf-8 -*-
#from kafka import KafkaProducer
#from kafka.errors import KafkaError
import kafka
import logging as logger
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Kafka_Producer:
    def __init__(self, bootstrap_servers_1):
        self.producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers_1)

    def send_msg(self, topic_id, key, value):
        self.producer.send(topic_id, key=key, value=value)


def main():
    bootstrap_servers = ['111.229.152.122:9092']
    topic_id_1 = 'topic_test_1'
    topic_id_2 = 'topic_test_2'
    topic_id_3 = 'topic_test_3'
    producer_1 = Kafka_Producer(bootstrap_servers)
    producer_2 = Kafka_Producer(bootstrap_servers)
    for i in range(0, 10):
        send_msg = 'hello kafka, this is a test msg, msg_id: ' + str(i)
        logger.info('send msg to kafka..., topic: %s, key: tag_topic_test_1, value: %s' % (topic_id_1, send_msg))
        producer_1.send_msg(topic_id_1, key=b'tag_topic_test_1', value=send_msg.encode(encoding='utf-8'))
        logger.info('send msg to kafka..., topic: %s, key: tag_topic_test_2, value: %s' % (topic_id_2, send_msg))
        producer_1.send_msg(topic_id_2, key=b'tag_topic_test_2', value=send_msg.encode(encoding='utf-8'))
        logger.info('send msg to kafka..., topic: %s, key: tag_topic_test_3, value: %s' % (topic_id_3, send_msg))
        producer_1.send_msg(topic_id_3, key=b'tag_topic_test_3', value=send_msg.encode(encoding='utf-8'))
    logger.info('Send msg End.')


if __name__ == '__main__':
    main()
