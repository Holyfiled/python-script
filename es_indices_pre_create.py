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

    # 方法：获取某天的全部indices name，传参：时间字符串，返回值：某天的所有索引名称（list）
    def get_indices_info(self, date_day):
        indices_info_list = self.es.cat.indices().split('\n')
        indices_day_list = []
        for line in indices_info_list:
            if len(line) > 0 and (date_day in line):
                index_name = line.split(' ')[2]
                if not index_name.startswith('.'):
                    indices_day_list.append(index_name)
        logger.info('Date: %s, total indices numbers: %d, indices list: %s' % (date_day, len(indices_day_list), indices_day_list))
        return indices_day_list

    # 方法：获取某天的去掉时间后缀的indices name，传参：时间字符串，返回值：某天的所有去掉时间后缀的索引名称（list）
    def get_index_pattern_list(self, date_day):
        index_patten_list = []
        for index_name in self.get_indices_info(date_day):
            if self.get_indices_doc_count(index_name) != '0':
                index_patten_name = index_name[:-10]
                index_patten_list.append(index_patten_name)
        logger.info('Date: %s, total index pattern numbers: %d, index pattern list: %s' % (date_day, len(index_patten_list), index_patten_list))
        return index_patten_list

    def get_indices_doc_count(self, index):
        index_doc_count = self.es.count(index=index)['count']
        return index_doc_count

    def get_indices_mapping(self, index):
        indices_mapping = self.es.indices.get_mapping(index=index)[index]['mappings']
        return indices_mapping

    #方法：用于创建索引，传参两个：索引名str和索引mapping（dict body）
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
        # 获取today tomorrow 时间字符串格式，对应索引时间后缀
        date_today = datetime.date.today().strftime('%Y.%m.%d')
        date_tomorrow = (datetime.date.today() + datetime.timedelta(+1)).strftime('%Y.%m.%d')
        # 遍历今天的索引，去掉时间后缀，获取今天索引的mapping，用于创建明天的索引
        for index_pattern in es_client.get_index_pattern_list(date_today):
            index_name_today = index_pattern + date_today
            index_name_tomor = index_pattern + date_tomorrow
            index_mapping = es_client.get_indices_mapping(index_name_today)
            #logger.info(index_name_tomor)
            es_client.create_indices(index_name_tomor, index_mapping)
        es_client.get_indices_info(date_tomorrow)
        logger.info('Prepare create indices end.')
    else:
        logger.warn('es node connection error or cluser satus is red.')


if __name__ == '__main__':
    main()
