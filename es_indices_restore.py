# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
import datetime
import sys
import logging as logger
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class ES:
    def __init__(self, node, Port, auth_name='', auth_pass=''):
        self.es = Elasticsearch(hosts=node, port=Port, http_auth=(auth_name, auth_pass), scheme="http")
        self.es_ping = self.es.ping()
        self.es_status = self.es.cat.health().split(' ')[3]

    def indices_restore(self, repository, snapshot, index):
        body = {
            "indices": index,
            "ignore_unavailable": "true",
            "include_global_state": "false"
        }
        self.es.snapshot.restore(repository, snapshot, body=body)

    def get_restore_time_interval(self, y, m, d, days):
        date_time_interval_list = []
        if int(days) >= 0:
            for n in range(0,days):
                date_appoint_day = (datetime.datetime(year=y, month=m, day=d) + datetime.timedelta(days=n)).strftime('%Y.%m.%d')
                date_time_interval_list.append(date_appoint_day)
            logger.info('time_interval_list: %s' % (date_time_interval_list))
        return date_time_interval_list


def main():
    if len(sys.argv) == 4:
        ES_node = ["111.229.152.122"]
        Port = 9200
        auth_user = ''
        auth_passwd = ''
        es_client = ES(ES_node, Port, auth_name=auth_user, auth_pass=auth_passwd)
        logger.info('ES cluster status is %s' % (es_client.es_status))
        if es_client.es_ping and (es_client.es_status != 'red'):
            index_pattern_name = sys.argv[1]
            date_result = [ int(i) for i in sys.argv[2].split('.')]
            year, month, day = date_result
            days_timedelta = int(sys.argv[3])
            es_backup_repository = 's3_backup_repository'
            for date_day in es_client.get_restore_time_interval(year, month, day, days=days_timedelta):
                es_backup_snapshot = 'prod-log-backup-' + date_day
                es_indices_name = index_pattern_name + '-' + date_day
                logger.info('es_backup_repository: %s, es_backup_snapshot: %s, es_indices_name: %s' % (es_backup_repository, es_backup_snapshot, es_indices_name))
                # es_client.indices_restore(es_backup_repository, es_backup_snapshot, es_indices_name)
        else:
            logger.warn('es node connection error or cluser satus is red.')
    else:
        logger.error('Please add 3 parmeters, eg: test.py kong-access-self-prod 2021.01.01 +5')
        pass


if __name__ == '__main__':
    main()