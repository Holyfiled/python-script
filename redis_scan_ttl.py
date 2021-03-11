import redis
import datetime
import logging as logger

logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


redis_host = '127.0.0.1'
redis_port = 6379
redis_passwd = ''
date_today = datetime.date.today().strftime('%Y-%m-%d')
key_output_file = 'E:\\redis-no-ttl-keys-'+date_today+'.txt'



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


def scan_ttl_key():
    if redis_cli_connect_success() is True:
        key_ttl_count = 0
        key_count = 0
        for db_index in range(0, 16):
            if redis_cli_init(db_index).dbsize() is not 0:
                redis_cli = redis_cli_init(db_index)
                logger.info('Start scan db%d keys...' % (db_index))
                with open(key_output_file, mode='a+') as file:
                    for key in redis_cli.scan_iter(count=1000):
                        key_count += 1
                        if redis_cli.ttl(key) == -1:
                            key_ttl_count += 1
                            file.write('db' + str(db_index) + " " + str(redis_cli.type(key)) + " " + str(key)+'\n')
                file.close()
                logger.info('Write db%d no ttl keys to file %s' % (db_index, key_output_file))
        logger.info('End the task of keys scan.')
        logger.info('redis total keys count: %d, no ttl keys count: %d' % (key_count, key_ttl_count))


def main():
    scan_ttl_key()


if __name__ == '__main__':
    main()
