import redis
import datetime
import sys
import time
import logging as logger
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
 
date_today = datetime.date.today().strftime('%Y-%m-%d')
output_file = 'redis-scan-'+date_today+'.txt'
 
 
class Redis():
    def __init__(self, host, port, passwd, db):
        self.db_index = db
        self.redis_host = host
        self.redis_port = port
        # 如果是AWS加密的Redis，需要设置ssl=True
        self.redis_cli = redis.StrictRedis(
            host=host,
            port=port,
            password=passwd,
            db=db,
            ssl=False,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        if self.redis_cli.ping() is True:
            logger.info('Connect redis server success, redis instance info: [%s:%s]' % (self.redis_host, self.redis_port))
        else:
            logger.error('Connect redis server error.')
 
    def get_info(self):
        redis_info = self.redis_cli.info()
        for k, v in redis_info.items():
            print(k + ': ' + str(v))
        return redis_info
 
    '''
    scan_key_str(key_str):
    1。使用scan方式遍历Redis中每个db的key，找到目标key。
    2. key_str 是通配字符串，例如：abc_* 表示以abc_开头的key。
    3. count值可以根据redis当前负载情况调整。
    '''
    def scan_key_str(self, key_str):
        key_str_count = 0
        db_size = self.redis_cli.dbsize()
        if db_size is not 0:
            logger.info('Start scan db%d keys...' % (self.db_index))
            with open(output_file, mode='a+',) as file:
                for key in self.redis_cli.scan_iter(count=5000, match=key_str):
                    key_str_count += 1
                    file.write(str(self.db_index) + ' ' + str(self.redis_cli.type(key)) + ' ' + str(key) + '\n')
            file.close()
            logger.info('End the task of keys scan.')
            logger.info('Redis db%d keys count: %d, key %s count: %d' % (self.db_index, db_size, key_str, key_str_count))
 
    '''
    scan_key_str_del(key_str):
    1。使用scan方式遍历Redis中每个db的key，找到目标key并删除。
    2. key_str 是通配字符串，例如：abc_* 表示以abc_开头的key。
    3. count值可以根据redis当前负载情况调整。
    4. self.redis_cli.unlink(key) 代理默认被注释，真正执行删除操作时去掉注释符。
    '''
    def scan_key_str_del(self, key_str):
        key_str_count = 0
        db_size = self.redis_cli.dbsize()
        if db_size is not 0:
            logger.info('Start scan db%d keys...' % (self.db_index))
            with open(output_file, mode='a+', ) as file:
                for key in self.redis_cli.scan_iter(count=5000, match=key_str):
                    key_str_count += 1
                    #self.redis_cli.unlink(key)
                    file.write(str(self.db_index) + ' ' + str(self.redis_cli.type(key)) + ' ' + str(key) + '\n')
            file.close()
            logger.info('End the task of keys scan.')
            logger.info('Redis db%d keys count: %d, del key %s count: %d' % (self.db_index, db_size, key_str, key_str_count))
 
    '''
    scan_key_ttl():
    1。使用scan方式遍历Redis中每个db的key，找到目标key并get到key的ttl时长。
    2. 其中ttl:-1表示key未设置ttl，ttl >= 604800 表示ttl值大于7天。
    3. count值可以根据redis当前负载情况调整。
    '''
    def scan_key_ttl(self):
        key_ttl_count = 0
        key_no_ttl_count = 0
        key_ttl_more_than = 0
        db_size = self.redis_cli.dbsize()
        if db_size is not 0:
            logger.info('Start scan db%d keys...' % (self.db_index))
            with open(output_file, mode='a+', ) as file:
                for key in self.redis_cli.scan_iter(count=5000):
                    key_ttl = self.redis_cli.ttl(key)
                    if key_ttl == -1:
                        key_no_ttl_count += 1
                        file.write(str(self.db_index) + ' ' + str(self.redis_cli.type(key)) + ' ' + str(key_ttl) + ' ' + str(key) + '\n')
                    elif key_ttl >= 604800:
                        key_ttl_more_than += 1
                        file.write(str(self.db_index) + ' ' + str(self.redis_cli.type(key)) + ' ' + str(key_ttl) + ' ' + str(key) + '\n')
            file.close()
            logger.info('End the task of keys scan.')
            logger.info('Redis db%d keys count: %d, not set ttl key count: %d' % (self.db_index, db_size, key_no_ttl_count))
            logger.info('Redis db%d keys count: %d, ttl more than 604800 sec key count: %d' % (self.db_index, db_size, key_ttl_more_than))
 
    '''
    scan_key_str_ttl(key_str):
    1。使用scan方式遍历Redis中每个db的key，找到目标key并get到key的ttl时长。
    2. key_str 是通配字符串，例如：abc_* 表示以abc_开头的key。。
    3. count值可以根据redis当前负载情况调整。
    '''
    def scan_key_str_ttl(self, key_str):
        key_str_count = 0
        db_size = self.redis_cli.dbsize()
        if db_size is not 0:
            logger.info('Start scan db%d keys...' % (self.db_index))
            with open(output_file, mode='a+', ) as file:
                for key in self.redis_cli.scan_iter(count=5000, match=key_str):
                    key_ttl = self.redis_cli.ttl(key)
                    key_str_count += 1
                    file.write(str(self.db_index) + ' ' + str(self.redis_cli.type(key)) + ' ' + str(key_ttl) + ' ' + str(key) + '\n')
            file.close()
            logger.info('End the task of keys scan.')
            logger.info('Redis db%d keys count: %d, key %s count: %d' % (self.db_index, db_size, key_str, key_str_count))
 
    def timestamp_trans(self, timestamp):
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        return dt
 
    '''
    get_slowlogs(slowlog_num):
    1。获取redis的慢查询日志，并将unix时间转化成utc时间格式。
    2. slowlog_num 表示获取的慢查询日志条数，例如100。
    '''
    def get_slowlogs(self, slowlog_num):
        slowlogs = self.redis_cli.slowlog_get(slowlog_num)
        logger.info('Get redis slowlog number: %d' % (slowlog_num))
        for slowlog in slowlogs:
            start_time = self.timestamp_trans(slowlog['start_time'])
            duration = slowlog['duration'] / 1000
            command = slowlog['command']
            print('id: %d,  %s,  %s,  %s' % (slowlog['id'], start_time, duration, command))
        return slowlogs
 
 
    '''
    get_client_list():
    1.获取redis的客户端连接信息及连接数。
    '''
    def get_client_list(self):
        client_list = self.redis_cli.client_list()
        for client in client_list:
            print('addr:' + client['addr'] + " " + 'age:' + client['age'] + " " + 'idle:' + client['idle'] + " " + 'cmd:' + client['cmd'])
        print('Number of client list: %d' % (len(client_list)))

        def kill_client(self):
        client_list = self.redis_cli.client_list()
        print('Number of client list: %d' % (len(client_list)))
        if len(client_list) > 1:
            logger.info('These client connections will be killed, client list: ')
            for client in client_list:
                if client['flags'] in ('N', 'P') and str(client['cmd']).lower() != 'client':
                    client_str = 'addr:' + client['addr'] + " " + 'age:' + client['age'] + " " + 'flags:' + client['flags'] + " " + 'idle:' + client[
                    'idle'] + " " + 'cmd:' + client['cmd']
                    #self.redis_cli.client_kill(client['addr'])
                    logger.info('%s has been killed' % (client_str))
                elif str(client['cmd']).lower() != 'client':
                    client_str = 'addr:' + client['addr'] + " " + 'age:' + client['age'] + " " + 'flags:' + client['flags'] + " " + 'idle:' + client['idle'] + " " + 'cmd:' + client['cmd']
                    print(client_str)
        else:
            for client in client_list:
                client_str = 'addr:' + client['addr'] + " " + 'age:' + client['age'] + " " + 'flags:' + client[
                    'flags'] + " " + 'idle:' + client['idle'] + " " + 'cmd:' + client['cmd']
                print(client_str)

    def get_mem_used_ratio(self):
        mem_used = self.redis_cli.info()['used_memory']
        mem_max = self.redis_cli.info()['maxmemory']
        mem_used_ratio = int((mem_used / mem_max) * 100)
        return mem_used_ratio
 
 
def main():
    if len(sys.argv) == 4:
        host = sys.argv[1]
        port = int(sys.argv[2])
        passwd = sys.argv[3]
        # for db_index in range(0,16):
        #     redis = Redis(host, port, passwd, db=db_index)
        #     redis.scan_key_str_ttl('k*')
        redis = Redis(host, port, passwd, db=0)
        redis.get_info()
        redis.get_slowlogs(100)
    else:
        logger.error("Please add 3 parmeters, eg: test.py '192.168.32.128' 6379 'abcd'")
 
 
if __name__ == '__main__':
    main()
