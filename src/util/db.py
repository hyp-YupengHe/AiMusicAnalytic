# Connect to ClickHouse
import clickhouse_connect
import redis

from src.util.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_HOST, \
    CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
from src.util.logger import logger

clickhouse_client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
    settings={"max_partitions_per_insert_block": 1000}
)

# Connect to Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)

def close_connections():
    try:
        if clickhouse_client:
            logger.info("Closing CK connection")
            clickhouse_client.disconnect()  # Proper way to close clickhouse_driver connection
    except Exception as e:
         pass
    try:
        if redis_client:
            logger.info("Closing Redis connection")
            redis_client.close()
    except Exception as e:
        pass