import asyncio
import json
import traceback
from datetime import datetime
from typing import List

import aiohttp
from dateutil import parser as date_parser

from src.util.config import SOUNDCLOUD_CLIENT_ID, SOUNDCLOUD_APP_VERSION, CLICKHOUSE_DATABASE
from src.util.db import close_connections, redis_client, clickhouse_client
from src.util.logger import logger

CLIENT_ID = SOUNDCLOUD_CLIENT_ID
APP_VERSION = SOUNDCLOUD_APP_VERSION

TABLE_NAME = 'users'
REDIS_KEY = 'soundcloud:snowbase:ck_offset_limit'
BASE_URL="https://api-v2.soundcloud.com"
BATCH_LIMIT = 1000
MAX_CONCURRENCY = 24

def robust_parse_dt(dt_str):
    if not dt_str:
        return datetime(1970, 1, 1)
    try:
        return date_parser.parse(dt_str)
    except Exception as e:
        logger.warning(f"Failed to parse datetime '{dt_str}': {e}, using fallback.")
        try:
            return datetime.fromtimestamp(int(''.join(filter(str.isdigit, dt_str))) // 1000)
        except Exception as e2:
            logger.error(f"Fallback datetime failed for '{dt_str}': {e2}")
            return datetime(1970, 1, 1)


def get_ck_offset_limit_from_redis() -> (int, int):
    offset = redis_client.hget(REDIS_KEY, 'offset')
    limit = redis_client.hget(REDIS_KEY, 'limit')
    return (int(offset) if offset else 0, int(limit) if limit else BATCH_LIMIT)

def set_ck_offset_limit_to_redis(offset: int, limit: int):
    redis_client.hset(REDIS_KEY, mapping={'offset': offset, 'limit': limit})

def get_seed_ids_from_ck(ch_client, offset=0, limit=BATCH_LIMIT) -> List[int]:
    try:
        sql = f"SELECT id FROM {CLICKHOUSE_DATABASE}.{TABLE_NAME} ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
        result = ch_client.query(sql)
        ids = set([int(row[0]) for row in result.result_rows])
        logger.info(f"Fetched {len(ids)} seed ids from ClickHouse for snowbase. (offset={offset}, limit={limit})")
        return ids
    except Exception as e:
        logger.error(f"Error fetching seed ids from ClickHouse: {e}")
        return []

async def fetch_followers(session, user_id, url, max_retries=3, retry_backoff=2):
    attempt = 0
    while attempt < max_retries:
        try:
            async with session.get(url, timeout=60) as resp:
                if resp.status != 200:
                    t = await resp.text()
                    logger.warning(f"Failed to fetch followers for url {url}: HTTP {resp.status} - {t}")
                    if 500 <= resp.status < 600:  # Retry on server errors
                        attempt += 1
                        await asyncio.sleep(retry_backoff * attempt)
                        continue
                    return None
                return await resp.json()
        except Exception as e:
            logger.error(f"Exception in fetch_followers for user {user_id}: {e}. URL: {url}. Traceback; {traceback.format_exc()}")
            attempt += 1
            await asyncio.sleep(retry_backoff * attempt)
    logger.error(f"Exceeded max retries for fetch_followers for user {user_id}. URL: {url}")
    return None

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif type(x) is list:
            out[name[:-1]] = json.dumps(x, ensure_ascii=False)
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def none_to_empty(val):
    """Convert None to empty string, else return str(val)."""
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    return str(val)

def none_to_zero(val):
    """Convert None to zero, else return val."""
    if val is None:
        return 0
    try:
        return int(val)
    except Exception:
        return 0

def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""

def parse_dt(val):
    # Accepts ISO string or returns default datetime
    if not val:
        return datetime(1970, 1, 1, 0, 0, 0)
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        try:
            return robust_parse_dt(val)
        except Exception:
            return datetime(1970, 1, 1, 0, 0, 0)

def insert_records(records, user_id, client):
    rows = []
    for rec in records:
        flat = flatten_json(rec)
        # All .get() for non-nullable string cols are wrapped with none_to_empty
        row = {
            'id': none_to_zero(flat.get('id', 0)),
            'avatar_url': none_to_empty(flat.get('avatar_url')),
            'city': none_to_empty(flat.get('city')),
            'comments_count': none_to_zero(flat.get('comments_count')),
            'country_code': none_to_empty(flat.get('country_code')),
            'created_at': parse_dt(flat.get('created_at') or '1970-01-01 00:00:00'),
            'creator_subscriptions': [safe_json(rec.get('creator_subscriptions', []))],
            'creator_subscription': safe_json(rec.get('creator_subscription', {})),
            'description': none_to_empty(flat.get('description')),
            'followers_count': none_to_zero(flat.get('followers_count')),
            'followings_count': none_to_zero(flat.get('followings_count')),
            'first_name': none_to_empty(flat.get('first_name')),
            'full_name': none_to_empty(flat.get('full_name')),
            'groups_count': none_to_zero(flat.get('groups_count')),
            'kind': none_to_empty(flat.get('kind')),
            'last_modified': parse_dt(flat.get('last_modified') or '1970-01-01 00:00:00'),
            'last_name': none_to_empty(flat.get('last_name')),
            'likes_count': none_to_zero(flat.get('likes_count')),
            'playlist_likes_count': none_to_zero(flat.get('playlist_likes_count')),
            'permalink': none_to_empty(flat.get('permalink')),
            'permalink_url': none_to_empty(flat.get('permalink_url')),
            'playlist_count': none_to_zero(flat.get('playlist_count')),
            'reposts_count': rec.get('reposts_count', 0),  # Nullable, so leave as is
            'track_count': none_to_zero(flat.get('track_count')),
            'uri': none_to_empty(flat.get('uri')),
            'urn': none_to_empty(flat.get('urn')),
            'username': none_to_empty(flat.get('username')),
            'verified': int(flat.get('verified', False)),
            'visuals': safe_json(rec.get('visuals', {})),
            'badges': safe_json(rec.get('badges', {})),
            'station_urn': none_to_empty(flat.get('station_urn')),
            'station_permalink': none_to_empty(flat.get('station_permalink')),
            '_raw.key': [],
            '_raw.value': []
        }
        for k, v in rec.items():
            row['_raw.key'].append(none_to_empty(k))
            row['_raw.value'].append(safe_json(v))
        rows.append(tuple(row.values()))

    if rows:
        try:
            client.insert(
                TABLE_NAME,
                rows,
                column_names=[
                    'id', 'avatar_url', 'city', 'comments_count', 'country_code', 'created_at',
                    'creator_subscriptions', 'creator_subscription', 'description', 'followers_count',
                    'followings_count', 'first_name', 'full_name', 'groups_count', 'kind', 'last_modified',
                    'last_name', 'likes_count', 'playlist_likes_count', 'permalink', 'permalink_url',
                    'playlist_count', 'reposts_count', 'track_count', 'uri', 'urn', 'username', 'verified',
                    'visuals', 'badges', 'station_urn', 'station_permalink', '_raw.key', '_raw.value'
                ]
            )
            logger.info(f"ClickHouse insert success, user id: {user_id}, table name: {TABLE_NAME}, length: {len(rows)}")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

async def snowball_user(user_id, ch_client):

    async with aiohttp.ClientSession(trust_env=False) as session:
        url = f"{BASE_URL}/users/{user_id}/followers?client_id={CLIENT_ID}&offset=0&limit=100&linked_partitioning=1&app_version={APP_VERSION}&app_locale=en"
        while True:
            data = await fetch_followers(session, user_id, url)
            if not data or 'collection' not in data:
                logger.info(f"No data or collection for user {user_id}")
                break
            collections = data['collection']
            if collections:
                insert_records(collections, user_id, ch_client)
            # for u in collections:
            #     uid = u.get('id')
            #     if uid is not None and uid not in seen:
            #         queue.put_nowait(uid)
            next_href = data.get('next_href', None)
            if not next_href:
                break
            url = next_href + f'&client_id={CLIENT_ID}&linked_partitioning=1&app_version=1748345262&app_locale=en'

async def worker(queue):
    ch_client = clickhouse_client
    while True:
        user_id = await queue.get()
        logger.info(f"worker {user_id}")
        if user_id is None:
            queue.task_done()
            break
        try:
            await snowball_user(user_id, ch_client)
        except Exception as e:
            logger.error(f"Error processing user in worker: {e}")
            logger.error(traceback.format_exc())
        finally:
            queue.task_done()

async def process_batch(offset, limit):
    ch_client = clickhouse_client
    snow_ids = get_seed_ids_from_ck(ch_client, offset=offset, limit=limit)
    if not snow_ids:
        logger.info("No more seed ids from CK. All done.")
        return False

    queue = asyncio.Queue()
    for uid in snow_ids:
        await queue.put(uid)
    logger.info(f"{len(snow_ids)} seed ids from CK. All done.")
    workers = [asyncio.create_task(worker(queue)) for _ in range(MAX_CONCURRENCY)]
    await queue.join()
    # Put sentinel None for each worker to signal exit
    for _ in workers:
        await queue.put(None)
    await asyncio.gather(*workers)
    return True

async def main():
    offset, limit = get_ck_offset_limit_from_redis()
    while True:
        logger.info(f"Snowballing batch: offset={offset}, limit={limit}")
        success = await process_batch(offset, limit)
        if not success:
            logger.info("No more batches to process. Exiting.")
            break
        offset += limit
        set_ck_offset_limit_to_redis(offset, limit)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(traceback.format_exc())
        close_connections()
    except KeyboardInterrupt:
        close_connections()
