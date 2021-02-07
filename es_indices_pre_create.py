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

    def get_indices_today(self):
        date_today = datetime.date.today().strftime('%Y.%m.%d')
        indices_list = self.es.cat.indices().split('\n')
        indices_list_today = []
        for line in indices_list:
            if len(line) > 0 and date_today in line:
                index_name = line.split(' ')[2]
                indices_list_today.append(index_name)
        logger.info('Today indices total number: %d, indices list: %s' % (len(indices_list_today), indices_list_today))
        return indices_list_today

    def get_indices_tomorrow(self, date_tomorrow):
        indices_list = self.es.cat.indices().split('\n')
        indices_list_tomorrow = []
        for line in indices_list:
            if len(line) > 0 and date_tomorrow in line:
                index_name = line.split(' ')[2]
                indices_list_tomorrow.append(index_name)
        logger.info('Tomorrow indices total number: %d, indices list: %s' % (len(indices_list_tomorrow), indices_list_tomorrow))

    def get_index_pattern_list(self):
        index_patten_list = []
        for index_name in self.get_indices_today():
            if self.get_indices_doc_count(index_name) != '0':
                index_patten_name = index_name[:-10]
                index_patten_list.append(index_patten_name)
        return index_patten_list

    def get_indices_doc_count(self, index):
        index_doc_count = self.es.count(index=index)['count']
        return index_doc_count

    def get_indices_mapping(self, index):
        indices_mapping = self.es.indices.get_mapping(index=index)[index]['mappings']
        return indices_mapping

    def put_indices_mapping(self, index, mapping):
        self.es.indices.put_mapping(mapping, index=index)

    def create_indices(self, index, mapping):
        if not self.es.indices.exists(index):
            logger.info('Prepare create index %s...' % (index))
            self.es.indices.create(index)
            logger.info('Created index %s.' % (index))
            logger.info('Prepare put index %s mapping...' % (index))
            self.es.indices.put_mapping(mapping, index=index)
            logger.info('Put index %s mapping end.' % (index))
        else:
            logger.warn('index %s already exist cluster, do not repeat create.' % (index))
            pass

    # def reindex_indices(self, source_index, dest_index):
    #     reindex_body = {"source": {"index": source_index}, "dest": {"index": dest_index}}
    #     self.es.reindex(body=reindex_body, wait_for_completion=False)


def main():
    ES_node = ["111.229.152.122"]
    Port = 9200
    auth_user = ''
    auth_passwd = ''
    es_client = ES(ES_node, Port, auth_name=auth_user, auth_pass=auth_passwd)
    if es_client.es_ping and (es_client.get_cluster_status() != 'red'):
        date_today = datetime.date.today().strftime('%Y.%m.%d')
        date_tomorrow = (datetime.date.today() + datetime.timedelta(+1)).strftime('%Y.%m.%d')
        for index_pattern in es_client.get_index_pattern_list():
            index_name_today = index_pattern + date_today
            index_name_tomor = index_pattern + date_tomorrow
            index_mapping = es_client.get_indices_mapping(index_name_today)
            #logger.info(index_name_tomor)
            #es_client.create_indices(index_name_tomor, index_mapping)
        es_client.get_indices_tomorrow(date_tomorrow)
        logger.info('Prepare create indices end.')
    else:
        logger.warn('es node connection error or cluser satus is red.')


if __name__ == '__main__':
    main()
