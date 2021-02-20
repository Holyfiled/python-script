# -*- coding: utf-8 -*-
from kafka.admin import KafkaAdminClient


BOOTSTRAP_SERVERS = '192.168.32.128:9092'
kafka_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)


def get_kafka_consumergroup():
    consumergrouplist = kafka_client.list_consumer_groups()
    consumer_group = []
    #print(type(consumergrouplist), consumergrouplist)
    for group_name, consumer_flag in consumergrouplist:
        if 'consumer' in consumer_flag:
            consumer_group.append(group_name)
    return consumer_group


def get_consumer_client_host():
    consumer_host_ip = set()
    topics = set()
    consumer_group = get_kafka_consumergroup()
    consumer_group_info = kafka_client.describe_consumer_groups(consumer_group)
    for line in consumer_group_info:
        for consumer_member in line.members:
            consumer_host_ip.add(consumer_member.client_host)
            for topic in consumer_member.member_metadata.subscription:
                topics.add(topic)
    print(consumer_host_ip)
    print(topics)
    #return consumer_host_ip


def main():
    get_consumer_client_host()


if __name__ == '__main__':
    main()




# def get_consumergroup_topic(group_id):
#     consumer_group_info = kafka_client.describe_consumer_groups(group_id)
#     for line in consumer_group_info:
#         print("Consumer group: %s" % (line.group))
#         consumer_member = line.members
#         #print(consumer_member)
#         for member in consumer_member:
#             consumer_ip = member.client_host
#             print("Consumer IP: %s" % (consumer_ip))
#             for topic in member.member_metadata.subscription:
#                 print(topic)
'''
dic={consumer_goup:[{ip1:{topic1,topic1,topic2,....},{ip2:{topic1,topic2,....},{ip3:{topic1,topic2,....}]}
for v in dic.values():
   for v1 in v.values():
      v1.add()
'''
