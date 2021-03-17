from elasticsearch import Elasticsearch
import datetime
import requests
import re
import json
import logging as logger
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

'''
功能：自动创建kibana 索引模式
'''

ES_node = ["127.0.0.1"]
Port = '9200'
auth_user = 'elastic'
auth_passwd = '123456'
es_client = Elasticsearch(hosts=ES_node, port=Port, http_auth=(auth_user, auth_passwd), scheme="http")
date_today = (datetime.date.today().strftime('%Y.%m.%d'))
date_yesterday = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y.%m.%d')


# 从ES集群获取索引模式名称，返回set；
def get_index_pattern_set():
    index_pattern_set = set()
    if es_client.ping() and get_cluster_status():
        es_indices_info_list = es_client.cat.indices().split('\n')
        for line in es_indices_info_list:
            if len(line) > 0 and (date_yesterday in line):
                index_pattern = line.split()[2].replace(date_yesterday, '*')
                if not index_pattern.startswith('.'):
                    index_pattern_set.add(index_pattern)
    return index_pattern_set


def get_cluster_status():
    if es_client.cluster.health()['status'] != 'red':
        return True
    else:
        return False

# 获取kibana role信息，return dict k:space_name  v: indices list
def get_kibana_role():
    urls = 'http://elastic:123456@127.0.0.1:5601/api/security/role'
    logger.info('url: %s' % (urls))
    headers_dict = {'kbn-xsrf': 'true', 'Content-Type': 'application/json'}
    try:
        response = requests.get(urls, headers=headers_dict)
        if response.status_code not in [200, 204]:
            logger.warn('POST status code: %d' % (response.status_code))
        else:
            logger.info('POST status code: %d' % (response.status_code))
            kibana_role_dict = dict()
            for role in response.json():
                if role['name'].startswith('role_'):
                    space_name = role['kibana'][0]['spaces'][0]
                    role_include_indices = role['elasticsearch']['indices'][0]['names']
                    kibana_role_dict[space_name] = role_include_indices
            return kibana_role_dict
    except TimeoutError:
        print('Timeout.')


# 获取space与indices pattern的对应关系，return dict  k:space_name  v:index pattern
def match_role_index_pattern():
    space_index_dict = dict()
    index_pattern_set = get_index_pattern_set()
    for space, role_index_list in get_kibana_role().items():
        space_index_set = set()
        for role_index in role_index_list:
            if role_index.endswith('*') and not role_index.startswith('*'):
                for index_pattern in index_pattern_set:
                    if index_pattern.startswith(role_index.replace('*', '')):
                        space_index_set.add(index_pattern)
            if role_index.endswith('*') and role_index.startswith('*'):
                for index_pattern in index_pattern_set:
                    if re.match(role_index.replace('*', ''), index_pattern):
                        #print(index_pattern)
                        space_index_set.add(index_pattern)
        space_index_dict[space] = space_index_set
    return space_index_dict


# post请求创建kibana index pattern，无返回值，传入space_name  index_pattern 两个参数
def urls_post_index_pattern(space, index_pattern):
    urls = 'http://elastic:123456@127.0.0.1:5601/s/'+space+'/api/saved_objects/index-pattern/'+index_pattern
    logger.info('url: %s' % (urls))
    headers_dict = {'kbn-xsrf': 'true', 'Content-Type': 'application/json'}
    body_space = {"attributes": {"title": index_pattern, "timeFieldName": "@timestamp"}}
    # try:
    #     response = requests.post(urls, headers=headers_dict, json=body_space)
    #     if response.status_code not in [200, 204]:
    #         logger.warn('POST status code: %d, body: %s' % (response.status_code, body_space))
    #     else:
    #         logger.info('POST status code: %d, body: %s' % (response.status_code, body_space))
    # except TimeoutError:
    #     print('Timeout.')


def main():
    space_index_dict = match_role_index_pattern()
    for space, v in space_index_dict.items():
        if len(v) != 0:
            for index_pattern in v:
                urls_post_index_pattern(space, index_pattern)


if __name__ == '__main__':
    main()
