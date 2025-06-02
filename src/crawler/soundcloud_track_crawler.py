import asyncio
import json
import random
import traceback
from datetime import datetime

import aiohttp
from aiohttp import ClientError

from src.crawler.soundcloud_follower import clickhouse_client, redis_client
from src.util.config import PROXY_TUNNEL, PROXY_USER_NAME, PROXY_PWD, SOUNDCLOUD_CLIENT_ID, PROXY_URL
from src.util.db import close_connections
from src.util.logger import logger

CLICKHOUSE_TABLE = "tracks"
REDIS_KEY_IDENTIFIER = "lionel_2M"
REDIS_KEY = f"soundcloud:track:{REDIS_KEY_IDENTIFIER}:offset"

BATCH_SIZE = 1000
CONCURRENT_USERS = 16
TRACKS_LIMIT_PER_REQUEST = 100
RETRY_LIMIT = 10
RETRY_BACKOFF = 1

# --- SCHEMA INFO ---
TRACK_COLS = [
    "id","artwork_url","caption","commentable","comment_count","created_at","description",
    "downloadable","download_count","duration","full_duration","embeddable_by","genre",
    "has_downloads_left","kind","label_name","last_modified","license","likes_count",
    "permalink","permalink_url","playback_count","public","purchase_title","purchase_url",
    "release_date","reposts_count","secret_token","sharing","state","streamable","tag_list",
    "title","uri","urn","user_id","visuals","waveform_url","display_date","station_urn",
    "station_permalink","track_authorization","monetization_model","policy",
    "publisher_metadata_id","publisher_metadata_urn","publisher_metadata_artist",
    "publisher_metadata_album_title","publisher_metadata_contains_music",
    "publisher_metadata_upc_or_ean","publisher_metadata_isrc","publisher_metadata_explicit",
    "publisher_metadata_p_line","publisher_metadata_p_line_for_display",
    "publisher_metadata_c_line","publisher_metadata_c_line_for_display",
    "publisher_metadata_release_title"
]
NON_NULLABLE_UINT32 = [
    'id', 'comment_count', 'download_count', 'duration', 'full_duration',
    'likes_count', 'playback_count', 'reposts_count', 'user_id'
]
NON_NULLABLE_BOOL = [
    'commentable', 'downloadable', 'has_downloads_left', 'public', 'streamable'
]
NON_NULLABLE_STRING = [
    "artwork_url","embeddable_by","kind","license","permalink","permalink_url","sharing",
    "state","tag_list","title","uri","urn","waveform_url","station_urn","station_permalink",
    "track_authorization","monetization_model","policy"
]
NULLABLE_STRING_SPECIAL = ["visuals"]
DATETIME_FIELDS = [
    "created_at", "last_modified", "release_date", "display_date"
]
PM_FIELDS = [
    "id", "urn", "artist", "album_title", "contains_music", "upc_or_ean", "isrc",
    "explicit", "p_line", "p_line_for_display", "c_line", "c_line_for_display", "release_title"
]

HEADERS = {
    'Host': 'api-v2.soundcloud.com',
    'Origin': 'https://soundcloud.com',
    'Referer': 'https://soundcloud.com',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': 'https://soundcloud.com',
}

# --- CLIENTS ---
ch_client = clickhouse_client
redis_client = redis_client

# --- TYPE HELPERS ---
def parse_datetime(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        v = val.replace("Z", "")
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return None
    return None

def safe_release_date(val):
    dt = parse_datetime(val)
    if not dt or dt.year < 1970:
        return None
    return dt

def safe_int(val):
    try:
        return int(val)
    except Exception:
        return 0

def safe_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes")
    if isinstance(val, int):
        return val != 0
    return False

def safe_str(val):
    return str(val) if val is not None else ""

def safe_nullable_string(val):
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)

# --- MAIN TRACK TRANSFORM ---
def transform_track_to_ck(track: dict) -> list:
    # Flatten publisher_metadata
    pm = track.pop("publisher_metadata", {}) or {}
    for pm_field in PM_FIELDS:
        track[f"publisher_metadata_{pm_field}"] = pm.get(pm_field, None)

    # Fix datetimes
    for k in ["created_at", "last_modified", "display_date"]:
        track[k] = parse_datetime(track.get(k))
    track["release_date"] = safe_release_date(track.get("release_date"))

    # Non-nullable ints/bools/strings
    for k in NON_NULLABLE_UINT32:
        track[k] = safe_int(track.get(k, 0))
    for k in NON_NULLABLE_BOOL:
        track[k] = safe_bool(track.get(k, False))
    for k in NON_NULLABLE_STRING:
        track[k] = safe_str(track.get(k, ""))

    # Nullable string fields that may be dicts
    for k in NULLABLE_STRING_SPECIAL:
        track[k] = safe_nullable_string(track.get(k, None))

    # Output as ordered list
    return [track.get(col, None) for col in TRACK_COLS]

# --- STORAGE ---
def store_tracks(tracks):
    if tracks:
        rows = [transform_track_to_ck(track) for track in tracks]
        try:
            ch_client.insert(CLICKHOUSE_TABLE, rows, column_names=TRACK_COLS)
            logger.info(f"Inserted {len(rows)} tracks to ClickHouse")
        except Exception as e:
            logger.error(f"ClickHouse insert error: {traceback.format_exc()}")

# --- REDIS OFFSET ---
def get_next_batch_offset():
    try:
        val = redis_client.get(REDIS_KEY)
        return int(val or 2000000)
    except Exception as e:
        logger.error(f"Redis get offset error: {e}")
        return 0

def set_next_batch_offset(offset):
    try:
        redis_client.set(REDIS_KEY, str(offset))
    except Exception as e:
        logger.error(f"Redis set offset error: {e}")

def fetch_user_ids(offset, limit):
    try:
        query = f"SELECT id FROM users LIMIT {limit} OFFSET {offset}"
        return [row[0] for row in ch_client.query(query).result_rows]
    except Exception as e:
        logger.error(f"ClickHouse fetch_user_ids error: {e}")
        return []

async def fetch_json_with_retry(session, url, user_id, max_attempts=RETRY_LIMIT):
    delay = RETRY_BACKOFF
    last_exception = None
    for attempt in range(max_attempts):
        try:
            headers = HEADERS.copy()
            async with session.get(url, headers=headers, proxy=PROXY_URL) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                logger.warning(f"User {user_id}: HTTP {resp.status} for {url} - {text}")
        except (ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(
                f"User {user_id}: Attempt {attempt+1}/{max_attempts} - {e} on {url} - traceback: {traceback.format_exc()}"
            )
        await asyncio.sleep(delay)
        delay *= random.uniform(1, 2)
        delay = min(delay, 15)

    logger.error(f"User {user_id}: Failed after {max_attempts} attempts for {url}")
    if last_exception:
        raise last_exception
    raise Exception(f"User {user_id}: Unspecified download failure for {url}")

async def fetch_and_store_tracks_for_user(session, user_id):
    url = (f"https://api-v2.soundcloud.com/users/{user_id}/tracks"
           f"?client_id={SOUNDCLOUD_CLIENT_ID}&limit={TRACKS_LIMIT_PER_REQUEST}")
    while url:
        try:
            data = await fetch_json_with_retry(session, url, user_id)
        except Exception as e:
            logger.error(f"User {user_id}: Skipping due to repeated errors: {e}")
            break
        tracks = data.get("collection", [])
        store_tracks(tracks)
        next_href = data.get("next_href")
        if next_href:
            if 'client_id=' not in next_href:
                sep = '&' if '?' in next_href else '?'
                next_href += f'{sep}client_id={SOUNDCLOUD_CLIENT_ID}'
            if 'limit=' not in next_href:
                sep = '&' if '?' in next_href else '?'
                next_href += f'{sep}limit={TRACKS_LIMIT_PER_REQUEST}'
            url = next_href
        else:
            break


async def crawl_batch():
    offset = get_next_batch_offset()
    # 1000000 - 2000000 is my limit
    while offset <= 3000000:
        logger.info(f"Crawling offset ({offset}) tracks, size ({BATCH_SIZE})")
        user_ids = fetch_user_ids(offset, BATCH_SIZE)
        if not user_ids:
            logger.error("No user IDs fetched from ClickHouse. Exiting.")
            return
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
            sem = asyncio.Semaphore(CONCURRENT_USERS)
            async def sem_task(user_id):
                async with sem:
                    await fetch_and_store_tracks_for_user(session, user_id)
            tasks = [sem_task(uid) for uid in user_ids]
            await asyncio.gather(*tasks)
        set_next_batch_offset(offset + BATCH_SIZE)
        logger.info(f"Batch complete: users {offset} - {offset + BATCH_SIZE - 1}")
        offset += BATCH_SIZE

if __name__ == "__main__":
    try:
        asyncio.run(crawl_batch())
    except Exception as e:
        close_connections()
    except KeyboardInterrupt:
        close_connections()
