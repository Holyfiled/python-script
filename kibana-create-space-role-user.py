import yaml
import requests
import json
import logging as logger

'''
功能：通过解析searchguard 配置文件，自动创建kibana space、role、user
'''


logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


file_internal_users = 'E:\\work\\ELK升级\\sg_internal_users_bak.yml'
file_internal_role = 'E:\\work\\ELK升级\\sg_roles_bak.yml'


def parse_ymal_file_role(filename):
    user_role = dict()
    with open(filename, mode='r') as file:
        file_data = yaml.load(file)
        # for user in file_data.keys():
        #     user_set.add(user[3:-5])
        for k, v in file_data.items():
            role_name = k[10:-5]
            user_role_indices = set()
            for k1 in v['indices']:
                user_role_indices.add(k1)
            user_role[role_name] = user_role_indices
    return user_role


def parse_ymal_file_user(filename):
    user_set = set()
    with open(filename, mode='r') as file:
        file_data = yaml.load(file)
        print(file_data)
        for user in file_data.keys():
            if user.startswith('indexvg'):
                user_set.add(user)
        return user_set


def parse_ymal_file_user_pass(filename):
    user_pass = dict()
    with open(filename, mode='r') as file:
        file_data = yaml.load(file)
        for k, v in file_data.items():
            if k.startswith('indexvg'):
                for k1, v1 in v.items():
                    user_pass[k] = v1
        return user_pass


def parse_space_name():
    space_set = set()
    user_set = parse_ymal_file_user(file_internal_users)
    for user in user_set:
        #print(user[7:])
        space_set.add(user[7:])
    return space_set


def urls_post_space():
    urls = 'http://elastic:123456@192.168.32.128:5601/api/spaces/space'
    headers = {'kbn-xsrf': 'true', 'Content-Type': 'application/json'}
    space_set = parse_space_name()
    for space in space_set:
        body_space = {"id": space, "name": space,
                      "disabledFeatures": ["enterpriseSearch", "maps", "infrastructure", "apm", "uptime", "siem",
                                           "monitoring", "ml", "ingestManager"]}
        try:
            response = requests.post(urls, headers=headers, json=body_space)
            response.close()
            if response.status_code is not 200:
                logger.warn('POST status code: %d, body: %s' % (response.status_code, body_space))
            else:
                logger.info('POST status code: %d, body: %s' % (response.status_code, body_space))
        except TimeoutError:
            print('Timeout.')


def urls_post_role():
    user_role = parse_ymal_file_role(file_internal_role)
    for k, v in user_role.items():
        role_name = 'role_'+k
        space_name = k
        indices_list = list(v)
        urls = 'http://elastic:123456@192.168.32.128:5601/api/security/role/'+role_name
        logger.info('url: %s' % (urls))
        headers_dict = {'kbn-xsrf': 'true', 'Content-Type': 'application/json'}
        body_space = {"metadata":{"version": 1}, "elasticsearch": {"cluster": [],"indices": [{"names": indices_list, "privileges" : ["read"]}]}, "kibana": [{"base": ["all"], "spaces": [space_name]}]}
        try:
            response = requests.put(urls, data=json.dumps(body_space), headers=headers_dict)
            #response.close()
            if response.status_code not in [200, 204]:
                logger.warn('POST status code: %d, body: %s' % (response.status_code, body_space))
            else:
                logger.info('POST status code: %d, body: %s' % (response.status_code, body_space))
        except TimeoutError:
            print('Timeout.')


def urls_post_user_pass():
    user_pass = parse_ymal_file_user_pass(file_internal_users)
    for user, passwd in user_pass.items():
        role_name = 'role_'+user[7:]
        urls = 'http://elastic:123456@192.168.32.128:9200/_security/user/'+user
        headers_dict = {'Content-Type': 'application/json'}
        body_user = {"password": passwd, "roles": [role_name], "metadata": {"intelligence": 7}}
        try:
            response = requests.post(urls, headers=headers_dict, json=body_user)
            if response.status_code not in [200, 204]:
                logger.warn('POST status code: %d, user: %s, body: %s' % (response.status_code, user, body_user))
            else:
                logger.info('POST status code: %d, user: %s, body: %s' % (response.status_code, user, body_user))
        except TimeoutError:
            print('Timeout.')


def main():
    urls_post_space()
    # urls_post_role()
    # urls_post_user_pass()


if __name__ == '__main__':
    main()
