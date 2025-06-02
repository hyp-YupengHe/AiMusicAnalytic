import json
import random
import time

import httpx  # pip install httpx

from src.util.config import SOUNDCLOUD_CLIENT_ID
from src.util.db import close_connections, redis_client, clickhouse_client
from src.util.logger import logger

TABLE_NAME = "followers"
REDIS_KEY = "soundcloud:last_url"

LIMIT=100
OFFSET=0
API_URL = f"https://api-v2.soundcloud.com/users/193/followers?client_id={SOUNDCLOUD_CLIENT_ID}&limit={LIMIT}&offset={OFFSET}"


def create_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id UInt64,
        avatar_url String,
        city String,
        comments_count Int32,
        country_code String,
        created_at DateTime,
        creator_subscriptions Array(String),
        creator_subscription String,
        description String,
        followers_count UInt32,
        followings_count UInt32,
        first_name String,
        full_name String,
        groups_count UInt32,
        kind String,
        last_modified DateTime,
        last_name String,
        likes_count UInt32,
        playlist_likes_count UInt32,
        permalink String,
        permalink_url String,
        playlist_count UInt32,
        reposts_count Nullable(Int32),
        track_count UInt32,
        uri String,
        urn String,
        username String,
        verified UInt8,
        visuals String,
        badges String,
        station_urn String,
        station_permalink String,
        _raw Nested (
            key String,
            value String
        )
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(created_at)
    PRIMARY KEY id
    ORDER BY (id, username, created_at)
    SETTINGS index_granularity = 8192;
    """
    clickhouse_client.execute(ddl)

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
    """Convert None to empty string, else return val."""
    return "" if val is None else val

def none_to_zero(val):
    """Convert None to zero, else return val."""
    return 0 if val is None else val

def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""

from datetime import datetime

def parse_dt(val):
    # Accepts ISO string or returns default datetime
    if not val:
        return datetime(1970, 1, 1, 0, 0, 0)
    try:
        # Remove Z if present, parse
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return datetime(1970, 1, 1, 0, 0, 0)

def insert_records(records):
    rows = []
    for rec in records:
        flat = flatten_json(rec)
        row = {
            'id': flat.get('id', 0) or 0,
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

    # Insert remaining rows
    if rows:
        try:
            clickhouse_client.execute(
                f"INSERT INTO {TABLE_NAME} VALUES",
                rows
            )
        except Exception as e:
            logger.error(f"Final batch insert failed: {e}")


def fetch_and_store(load_from_redis=False):
    last_url = redis_client.get(REDIS_KEY)
    url = last_url.decode() if (load_from_redis and last_url) else API_URL

    with httpx.Client(timeout=30) as client:
        while url:
            logger.info(f"Fetching: {url}")
            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Error fetching or decoding JSON from {url}: {e}")
                break

            collection = data.get('collection', [])
            logger.info(f"Fetched {len(collection)} records from {url}")
            if collection:
                insert_records(collection)

            next_href = data.get('next_href')
            if next_href:
                # Append client_id only if it's not already in the URL
                if "client_id=" not in next_href:
                    if "?" in next_href:
                        next_href += '&client_id=sobAMMic36lHKNZEwXsaEj5feRIAW9Yx'
                    else:
                        next_href += '?client_id=sobAMMic36lHKNZEwXsaEj5feRIAW9Yx'
                url = next_href
            else:
                url = None
            # Save current url to redis after each fetch
            redis_client.set(REDIS_KEY, url if url else "")
            time.sleep(random.random())
    logger.info("Done fetching all data.")


def main():
    create_table()
    fetch_and_store(load_from_redis=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        close_connections()
    except KeyboardInterrupt:
        close_connections()