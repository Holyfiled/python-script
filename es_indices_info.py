# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import datetime
import logging as logger
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class ES:
    def __init__(self, node, Port, auth_name='', auth_pass=''):
        self.es = Elasticsearch(hosts=node, port=Port, http_auth=(auth_name, auth_pass), scheme="http")
        self.es_ping = self.es.ping()

    def get_cluster_status(self):
        cluster_status = self.es.cat.health().split(' ')[3]
        logger.info('ES cluster status is %s' % (cluster_status))
        return cluster_status

    def get_indices_name_list(self, date_yes):
        index_name_and_size = {}
        es_indices_info_list = self.es.cat.indices().split('\n')
        print(es_indices_info_list)
        for line in es_indices_info_list:
            if len(line) > 0 and (date_yes in line):
                index_name = line.split()[2]
                index_store_size = self.indices_store_size_format(line.split()[8])
                index_name_and_size[index_name] = index_store_size
        logger.info('Total indices numbers: %d, indices name and store size: %s' % (len(index_name_and_size), index_name_and_size))
        return index_name_and_size

    def get_indices_doc_count(self, index):
        indices_doc_count = self.es.count(index=index)['count']
        return indices_doc_count

    def index_doc(self, indices, body):
        self.es.index(index=indices, doc_type='_doc', body=body)

    def indices_store_size_format(self, indices_size):
        if 'tb' in indices_size:
            return float(indices_size[:-2]) * 1024
        if 'gb' in indices_size:
            return float(indices_size[:-2])
        if 'mb' in indices_size:
            return round((float(indices_size[:-2]) / 1024), 3)
        if 'kb' in indices_size or 'b' in indices_size:
            return 0
        else:
            return 0


def main():
    ES_node = ["127.0.0.1"]
    Port = 9200
    auth_user = ''
    auth_passwd = ''
    es_client = ES(ES_node, Port, auth_name=auth_user, auth_pass=auth_passwd)
    if es_client.es_ping and (es_client.get_cluster_status() != 'red'):
        # date_today = datetime.date.today().strftime('%Y.%m.%d')
        date_yesterday = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y.%m.%d')
        index_timestamp = date_yesterday.replace(".", "-")
        for index_name, index_store_size in es_client.get_indices_name_list(date_yesterday).items():
            index_pattern_name = index_name[:-11]
            index_doc_count = es_client.get_indices_doc_count(index_name)
            body = {
                '@timestamp': index_timestamp,
                'index_pattern_name': index_pattern_name,
                'indices_name': index_name,
                'indices_doc_count': index_doc_count,
                'indices_store_size': index_store_size
            }
            es_client.index_doc('indices-info', body)
            #print("@timestamp: %s, index_pattern_name: %s, indices_name: %s, doc_count: %d, indices_size: %s" % (index_timestamp, index_pattern_name, index_name, index_doc_count, index_store_size))
    else:
        logger.warn('es node connection error or cluser satus is red.')

  
if __name__ == '__main__':
    main()

