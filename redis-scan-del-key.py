import redis
import datetime
import logging as logger

logger.basicConfig(filename='redis-scan-del-key.log', level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


redis_host = '192.168.32.128'
redis_port = 6379
redis_passwd = 'Changeme_123'
date_today = datetime.date.today().strftime('%Y-%m-%d')
key_name = 'todov2:be:sessions*'

def redis_cli_init(db_index):
    redis_connect_pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_passwd, max_connections=50, db=db_index)
    redis_cli = redis.StrictRedis(connection_pool=redis_connect_pool)
    return redis_cli


def redis_cli_connect_success():
    redis_cli = redis_cli_init(0)
    if redis_cli.ping() is True:
        logger.info('Connect redis server success, redis instance info: [%s:%d]' % (redis_host, redis_port))
    else:
        logger.error('Connect redis server error.')
    return redis_cli.ping()


def del_key_by_scan(key_name):
    if redis_cli_connect_success() is True:
        if redis_cli_init(1).dbsize() is not 0:
            redis_cli = redis_cli_init(1)
            key_count = 0
            for key in redis_cli.scan_iter(key_name, count=1000):
                key_count += 1
                logger.info('redis db0 key: %s, type: %s, ttl: %d' % (key, redis_cli.type(key), redis_cli.ttl(key)))
                #redis_cli.unlink(key)
                #logger.info('del key: %s' % (key))
            logger.info('Total del keys count: %d' % (key_count))


def main():
    del_key_by_scan(key_name)


if __name__ == '__main__':
    main()
