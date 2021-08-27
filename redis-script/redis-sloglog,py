import redis
import time
import sys
import datetime
import logging as logger
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Redis():
    def __init__(self, host, port, passwd):
        self.redis_cli = redis.StrictRedis(host=host, port=port, password=passwd, db=0)
        self.redis_cli.ping()

    def get_slowlog(self):
        slowlog = self.redis_cli.slowlog_get(5)
        print(slowlog)
        return slowlog



def timestamp_trans(timestamp):
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    return dt


def main():
    host = sys.argv[1]
    passwd = sys.argv[2]
    port = 6379
    redis = Redis(host=host, port=port, passwd=passwd)
    slowlogs = redis.get_slowlog()
    for i in slowlogs:
        start_time = timestamp_trans(i['start_time'])
        duration = i['duration'] / 1000
        command = i['command']
        print('id: %d,  %s,  %s,  %s' % (i['id'], start_time, duration, command))


if __name__ == '__main__':
    main()
