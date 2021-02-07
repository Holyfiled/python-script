from elasticsearch import Elasticsearch
import datetime
import logging as log


class ES:
    def __init__(self, node, Port, auth_name='', auth_pass=''):
        self.es = Elasticsearch(hosts=node, port=Port, http_auth=(auth_name, auth_pass), scheme="http")
        self.es_ping = self.es.ping()

    def get_cluster_status(self):
        cluster_status = self.es.cat.health().split(' ')[3]
        return cluster_status

    def get_indices_today(self):
        date_today = datetime.date.today().strftime('%Y.%m.%d')
        date_yesterday = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y.%m.%d')
        indices_list = self.es.cat.indices().split('\n')
        indices_list_yes = []
        for line in indices_list:
            if len(line) > 0 and date_yesterday in line:
                index_name = line.split(' ')[2]
                indices_list_yes.append(index_name)
        return indices_list_yes

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
        indices_mapping = self.es.indices.get_mapping(index=index)
        return indices_mapping

    def put_indices_mapping(self, index, mapping):
        self.es.indices.put_mapping(mapping, index=index)

    def create_indices(self, index, mapping):
        self.es.indices.create(index)
        self.es.indices.put_mapping(mapping, index=index)

    def create_indices_tomorrow(self):
        date_tomor = (datetime.date.today() + datetime.timedelta(+1)).strftime('%Y.%m.%d')
        index_list_tomor = []
        for index_patter in self.get_index_pattern_list():
            index_name_tomor = index_patter+date_tomor
            index_list_tomor.append(index_name_tomor)
            self.create_indices()
        return index_list_tomor

    def reindex_indices(self, source_index, dest_index):
        reindex_body = {"source": {"index": source_index}, "dest": {"index": dest_index}}
        self.es.reindex(body=reindex_body, wait_for_completion=False)


def main():
    ES_node = ["111.229.152.122"]
    Port = 9200
    es_client = ES(ES_node, Port)
    if es_client.es_ping and (es_client.get_cluster_status() != 'red'):
        print(es_client.create_indices_tomorrow())


        # index_1 = 'test-indices-2020.01.17'
        # index_mapping = es_client.get_indices_mapping(index_1)[index_1]['mappings']
        # index_2 = ['test-indices-1-2021.02.05', 'test-indices-2-2021.02.05', 'test-indices-3-2021.02.05', 'test-indices-4-2021.02.05', 'test-indices-5-2021.02.05']
        # for index in index_2:
        #     es_client.reindex_indices(index_1, index)
        #     print(index + 'is reindex.')
        # # print(es_client.get_cat_indices())

        # print(es_client.get_indices_doc_count(index))
        # print(es_client.get_indices_doc_count(index_2))
        # #es_client.create_indices(index_2, index_mapping)
        #index_2_mapping = es_client.get_indices_mapping(index_2)[index_2]['mappings']
        #print(index_2_mapping)
    else:
        log.warn('es node connection error or cluser satus is red.')


if __name__ == '__main__':
    main()