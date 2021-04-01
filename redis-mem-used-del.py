import redis
import logging as logger
logger.basicConfig(filename='', level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Redis:
    def __init__(self, redis_host, redis_port, redis_auth):
        try:
            self.redis_connect_pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_auth, max_connections=10)
            self.redis_cli = redis.StrictRedis(connection_pool=self.redis_connect_pool)
            logger.info('Connect redis server success, redis instance info: [%s:%d]' % (redis_host, redis_port))
        except Exception as e:
            logger.error('Connect redis server error.')

    def get_mem_used_ratio(self):
        mem_used = self.redis_cli.info()['used_memory']
        mem_max = self.redis_cli.info()['maxmemory']
        mem_used_ratio = int((mem_used / mem_max) * 100)
        return mem_used_ratio

    def redis_flush_db(self):
        mem_used_ratio = self.get_mem_used_ratio()
        if mem_used_ratio > 90:
            logger.warn('Redis mem used ratio: %s%%' % (mem_used_ratio))
            logger.info('Prepare flush all keys in Redis...')
            #self.redis_cli.flushall()
            logger.info('End flush all keys in Redis')
        else:
            logger.info('Redis mem used ratio: %s%%, nothing to do.' % (mem_used_ratio))


def main():
    redis_host = '192.168.32.128'
    redis_port = 6379
    redis_passwd = ''
    redis = Redis(redis_host, redis_port, redis_passwd)
    redis.redis_flush_db()


if __name__ == '__main__':
    main()
