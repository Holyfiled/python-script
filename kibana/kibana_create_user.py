import requests
import json
import logging as logger
 
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
 
class Kibana:
    def __init__(self, user, passwd, rolename, space, index_list, url_es, url_kibana):
        self.username = user
        self.passwd = passwd
        self.rolename = rolename
        self.spacename = space
        self.index_pattern = index_list
        self.es_url = url_es
        self.kibana_url = url_kibana
 
    def create_user(self):
        urls = self.es_url + '_security/user/' + self.username
        headers_dict = {'Content-Type': 'application/json'}
        body_user = {"password": self.passwd, "roles": [self.rolename], "metadata": {"intelligence": 7}}
        logger.info('Preparing to create user account.')
        try:
            response = requests.post(urls, headers=headers_dict, json=body_user)
            if response.status_code not in [200, 204]:
                logger.warn('POST status code: %d, user: %s, body: %s' % (response.status_code, self.username, body_user))
            else:
                logger.info('POST status code: %d, user: %s, body: %s' % (response.status_code, self.username, body_user))
                logger.info('End to create user account.')
        except TimeoutError:
            logger.warn('Timeout.')
 
    def create_space(self):
        urls = self.kibana_url + 'api/spaces/space'
        headers = {'kbn-xsrf': 'true', 'Content-Type': 'application/json'}
        space = self.spacename
        logger.info('Preparing to create user space.')
        body_space = {"id": space, "name": space,
                      "disabledFeatures": ["enterpriseSearch", "maps", "infrastructure", "apm", "uptime", "siem",
                                           "monitoring", "ml", "ingestManager"]}
        try:
            response = requests.post(urls, headers=headers, json=body_space)
            if response.status_code is not 200:
                logger.warn('POST status code: %d, body: %s' % (response.status_code, body_space))
            else:
                logger.info('POST status code: %d, body: %s' % (response.status_code, body_space))
                logger.info('End to create user space.')
        except TimeoutError:
            print('Timeout.')
 
    def create_role(self):
        urls = self.kibana_url + 'api/security/role/' + self.rolename
        logger.info('Preparing to create user role.')
        headers_dict = {'kbn-xsrf': 'true', 'Content-Type': 'application/json'}
        body_space = {"metadata": {"version": 1},
                      "elasticsearch": {"cluster": [], "indices": [{"names": self.index_pattern, "privileges": ["read","view_index_metadata"]}]},
                      "kibana": [{"base": ["all"], "spaces": [self.spacename]}]}
        try:
            response = requests.put(urls, data=json.dumps(body_space), headers=headers_dict)
            #response.close()
            if response.status_code not in [200, 204]:
                logger.warn('POST status code: %d, body: %s' % (response.status_code, body_space))
            else:
                logger.info('POST status code: %d, body: %s' % (response.status_code, body_space))
                logger.info('End to create user role.')
        except TimeoutError:
            print('Timeout.')
 
 
def main():
    username = 'indexwhtest'
    passwd = '123456'
    index_list = ['kong-access-*', 'kong-access-self-*']
    rolename = 'role_' + username.split('indexwh')[1]
    space = username.split('indexwh')[1]
    basic_url_es = 'http://elastic:123456@192.168.13.155:9200/'
    basic_url_kibana = 'http://elastic:123456@192.168.13.155:5602/'
    kibana = Kibana(user=username, passwd=passwd, rolename=rolename, space=space, index_list=index_list, url_es=basic_url_es, url_kibana=basic_url_kibana)
    kibana.create_space()
    kibana.create_role()
    kibana.create_user()
 
 
if __name__ == '__main__':
    main()
