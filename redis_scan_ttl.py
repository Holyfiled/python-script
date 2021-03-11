import redis
import os
import logging as logger

logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


redis_host = '192.168.32.128'
redis_port = 6379
key_output_file = 'E:\\redis-no-ttl-keys.txt'


def scan_ttl_key():
    redis_connect_pool = redis.ConnectionPool(host=redis_host, port=redis_port, max_connections=50)
    redis_cli = redis.StrictRedis(connection_pool=redis_connect_pool)
    key_ttl_count = 0
    key_count = 0
    if redis_cli.ping() is True:
        logger.info('Connect success, redis instance info: [%s:%d]' % (redis_host, redis_port))
        logger.info('Start scan all keys...')
        with open(key_output_file, mode='w+') as file:
            for key in redis_cli.scan_iter(count=1000):
                key_count += 1
                if redis_cli.ttl(key) == -1:
                    redis_cli.type(key)
                    file.write(str(redis_cli.type(key))+" "+str(key)+'\n')
                    key_ttl_count += 1
            file.close()
            print('redis total keys count: %d, no ttl keys count: %d' % ( key_count, key_ttl_count))
            logger.info('Write no ttl keys to file %s' % (key_output_file))
            logger.info('End the task of scanning keys.')
    else:
        logger.warn('redis connect not success.')


def main():
    scan_ttl_key()


if __name__ == '__main__':
    main()
