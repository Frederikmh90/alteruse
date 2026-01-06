#!/usr/bin/env python3
"""
MINIMAL PATCH: Just fix the language field extraction
Add this function to your working script and replace the language filtering logic
"""

# =========================
# UPDATED WORKING SCRIPT WITH GAB LANGUAGE FIX
# =========================
PLATFORMS = "auto"  # All platforms
LANG_FILTER = {"da", "de", "sv"}  # Danish, German, Swedish ONLY
ACCOUNT_PERCENTAGE = 10  # 10% of actors per platform
POST_PERCENTAGE = 10  # 5% of posts per actor
MIN_POSTS_PER_ACTOR = 50  # Minimum posts per actor
MAX_POSTS_PER_ACTOR = 30  # Maximum posts per actor
POSTS_PER_ACTOR_SOFT = 400

MAX_ACTORS_PER_PLATFORM = 100000

BATCH_FETCH_SIZE = 2000
FETCH_WORKERS = 8
CHUNK_SAVE_SIZE = 10_000

OUTPUT_DIR = "./data/all_platforms_target_langs"
RANDOM_SEED = 42

import os
import time
import random
from typing import List, Dict, Any, Set, Tuple
import pandas as pd
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

from spreadAnalysis.persistence.mongo import MongoSpread
from spreadAnalysis.persistence.schemas import Spread

DERIVED_COLUMNS = [
    "actor_id",
    "actor_username",
    "actor_name",
    "platform",
    "lang",
    "datetime",
    "message_id",
    "post_url",
    "link_to_actor",
    "text",
]

POST_PROJECTION_MIN = {
    "_id": 1,
    "method": 1,
    "platform": 1,
    # common text/time
    "text": 1,
    "message": 1,
    "caption": 1,
    "description": 1,
    "created_at": 1,
    "date": 1,
    "post_date": 1,
    "title": 1,
    # youtube specifics (nested)
    "snippet": 1,
    # ids
    "id": 1,  # many platforms
    "post_id": 1,  # vkontakte optional
    "owner_id": 1,  # vkontakte
    "platformId": 1,  # crowdtangle
    # telegram specifics
    "from_username": 1,
    "peer_id": 1,  # contains channel_id
    "media": 1,  # may hold webpage.url
    # vkontakte specifics
    "actor": 1,  # actor.screen_name / id / name / first_name / last_name
    # gab specifics
    "body": 1,
    "url": 1,
    # link helpers
    "post_url": 1,
    "postUrl": 1,
    "account": 1,  # account.url / names (for several platforms)
    # other fields already present
    "lang": 1,
    "language": 1,
    "source": 1,
    "conversation_id": 1,
    "author": 1,
    "entities": 1,
    "public_metrics": 1,
    "attachments": 1,
    "referenced_tweets": 1,
    "type_id": 1,
    "name": 1,
    "statistics": 1,
    "user_id": 1,
}


def _client_and_db():
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=4000,
        socketTimeoutMS=300000,
        connectTimeoutMS=8000,
        maxPoolSize=64,
    )
    return client, client[name]


def _normalize_platforms(db, platforms):
    if platforms is None:
        return list(db.actor_metric.distinct("platform"))
    if isinstance(platforms, str):
        s = platforms.strip()
        if s.lower() == "auto":
            vals = list(db.actor_metric.distinct("platform"))
            return vals
        parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
        return parts if parts else []
    return list(platforms)


def _safe_get(getter, method, data):
    try:
        return getter(method=method, data=data)
    except Exception as e:
        return e


def _rows_from_posts(
    posts: List[Dict[str, Any]],
    lang_filter: Set[str] | str = "all",
    actor_lang_map: Dict[str, str] | None = None,
    platform_hint: str | None = None,
) -> Tuple[list, int]:
    rows, skipped = [], 0

    # Only require platform; compute others with robust fallbacks per platform
    REQUIRED = {
        "platform": Spread._get_platform,
    }
    OPTIONAL = {
        "actor_id": Spread._get_actor_id,
        "actor_name": Spread._get_actor_name,
        "lang": Spread._get_lang,
        "post_url": Spread._get_post_url,
        "link_to_actor": Spread._get_link_to_actor,
    }

    for p in posts:
        m = p.get("method")

        # Required: platform only (fallback to document field)
        req_values = {}
        platform_val = _safe_get(Spread._get_platform, m, p)
        if isinstance(platform_val, Exception) or platform_val in (None, ""):
            platform_val = p.get("platform")
        if not platform_val:
            skipped += 1
            continue
        req_values["platform"] = platform_val

        # Optional fields
        opt_values = {}
        for key, getter in OPTIONAL.items():
            val = _safe_get(getter, m, p)
            if isinstance(val, Exception):
                opt_values[key] = None
            else:
                opt_values[key] = val

        # Text
        text_val = _safe_get(Spread._get_message_text, m, p)
        if isinstance(text_val, Exception) or text_val in (None, ""):
            text_val = (
                p.get("text")
                or p.get("message")
                or p.get("caption")
                or p.get("description")
            )

        # Robust required-like fields with fallbacks
        actor_username_val = _safe_get(Spread._get_actor_username, m, p)
        if isinstance(actor_username_val, Exception) or actor_username_val in (
            None,
            "",
        ):
            actor_username_val = (
                p.get("author") or p.get("from_username") or p.get("user_id")
            )
        if not actor_username_val:
            # As last resort, keep None; we no longer drop the row
            actor_username_val = None

        message_id_val = _safe_get(Spread._get_message_id, m, p)
        if isinstance(message_id_val, Exception) or message_id_val in (None, ""):
            message_id_val = str(p.get("_id")) if p.get("_id") is not None else None

        datetime_val = _safe_get(Spread._get_date, m, p)
        if isinstance(datetime_val, Exception) or datetime_val in (None, ""):
            datetime_val = p.get("date") or p.get("created_at")

        # FIXED LANGUAGE FILTER — prefer actor-level language when available
        platform = req_values["platform"]
        # Determine actor language from map if provided
        actor_lang = None
        if actor_lang_map and actor_username_val:
            actor_lang = actor_lang_map.get(str(actor_username_val))

        # Fallbacks for specific platforms when actor_lang not available
        if actor_lang:
            effective_lang = actor_lang
        elif platform == "gab":
            effective_lang = p.get("language") or opt_values.get("lang")
        else:
            effective_lang = opt_values.get("lang")

        # Enforce language filter strictly at actor-level when possible
        if lang_filter != "all":
            code = (effective_lang or "").split("-")[0].lower()
            if code not in lang_filter:
                continue

        # Platform-specific fallbacks for missing fields
        post_url_val = opt_values.get("post_url")
        link_to_actor_val = opt_values.get("link_to_actor")
        actor_name_val = opt_values.get("actor_name")
        actor_id_override = opt_values.get("actor_id")

        if platform == "telegram":
            if not link_to_actor_val and actor_username_val:
                link_to_actor_val = f"https://t.me/{actor_username_val}"
            if not text_val:
                text_val = p.get("message") or p.get("text") or ""
        elif platform == "gab":
            if not text_val:
                text_val = p.get("body") or ""
            if not post_url_val:
                post_url_val = p.get("url")
        elif platform == "vkontakte":
            if not link_to_actor_val and actor_username_val:
                link_to_actor_val = f"https://vk.com/{actor_username_val}"
            if not post_url_val:
                vk_post_id = p.get("id")
                owner_id = p.get("owner_id")
                if (
                    actor_username_val
                    and owner_id is not None
                    and vk_post_id is not None
                ):
                    post_url_val = f"https://vk.com/{actor_username_val}?w=wall{owner_id}_{vk_post_id}"
        elif platform == "youtube":
            snip = p.get("snippet") or {}
            # actor_id (channelId)
            ch_id = None
            if isinstance(snip, dict):
                ch_id = snip.get("channelId")
            if not actor_id_override and ch_id:
                actor_id_override = ch_id
            # link_to_actor
            if not link_to_actor_val and ch_id:
                link_to_actor_val = f"https://www.youtube.com/channel/{ch_id}"
            # text from title + description if empty
            if not text_val and isinstance(snip, dict):
                t = snip.get("title") or ""
                d = snip.get("description") or ""
                text_val = (t + " \n" + d).strip() if (t or d) else ""
            # datetime from publishedAt
            if not datetime_val and isinstance(snip, dict):
                pub = snip.get("publishedAt")
                if pub:
                    datetime_val = pub.replace("T", " ").replace("Z", "")
            # actor_name
            if not actor_name_val:
                actor_snip = (
                    p.get("actor", {}).get("snippet")
                    if isinstance(p.get("actor"), dict)
                    else None
                )
                actor_name_val = (
                    actor_snip.get("title")
                    if isinstance(actor_snip, dict) and actor_snip.get("title")
                    else snip.get("channelTitle") or actor_username_val
                )

        if not actor_name_val and actor_username_val:
            actor_name_val = actor_username_val

        rows.append(
            {
                "actor_id": actor_id_override,
                "actor_username": actor_username_val,
                "actor_name": actor_name_val,
                "platform": req_values["platform"],
                "lang": (actor_lang or effective_lang),
                "datetime": datetime_val,
                "message_id": message_id_val,
                "post_url": post_url_val,
                "link_to_actor": link_to_actor_val,
                "text": text_val,
            }
        )

    return rows, skipped


def _fetch_post_docs_by_ids(db, post_ids: List[Any]):
    if not post_ids:
        return []
    return list(db.post.find({"_id": {"$in": post_ids}}, POST_PROJECTION_MIN))


def _pick_accounts_actor_metric(
    db, platform: str, target_n: int, lang_filter: Set[str] | str | None = None
) -> List[str]:
    try:
        match_stage = {"platform": platform, "actor_username": {"$ne": None}}
        # Actor-level language filtering for ALL platforms
        if lang_filter and lang_filter != "all":
            match_stage["lang"] = {"$in": list(lang_filter)}

        docs = list(
            db.actor_metric.aggregate(
                [
                    {"$match": match_stage},
                    {"$sample": {"size": target_n}},
                    {"$project": {"_id": 0, "actor_username": 1}},
                ],
                allowDiskUse=True,
            )
        )
        usernames = [d["actor_username"] for d in docs if d.get("actor_username")]
        return list(dict.fromkeys(usernames))[:target_n]
    except Exception:
        return []


def _collect_actor_post_ids(
    db,
    actor_username: str,
    platform: str,
    min_posts_per_actor: int,
    posts_per_actor_soft: int,
    post_percentage: float,
    max_posts_per_actor,
):
    # Step 1: get post_obj_ids array size via aggregation
    size_doc = list(
        db.actor_platform_post.aggregate(
            [
                {"$match": {"actor_username": actor_username, "platform": platform}},
                {"$project": {"n": {"$size": "$post_obj_ids"}}},
            ],
            allowDiskUse=True,
        )
    )
    n = size_doc[0]["n"] if size_doc else 0
    if n < min_posts_per_actor:
        return []

    # Step 2: choose a random window and slice to avoid time bias
    window = posts_per_actor_soft if posts_per_actor_soft > 0 else n
    window = min(window, n)
    start = (
        0 if n == window else random.Random(RANDOM_SEED).randrange(0, n - window + 1)
    )

    app = db.actor_platform_post.find_one(
        {"actor_username": actor_username, "platform": platform},
        {"post_obj_ids": {"$slice": [start, window]}},
    )
    ids = (app or {}).get("post_obj_ids") or []

    # Step 3: percentage sample and caps
    if post_percentage < 100.0:
        k = max(1, int(len(ids) * (post_percentage / 100.0)))
    else:
        k = len(ids)
    if max_posts_per_actor:
        k = min(k, max_posts_per_actor)

    if k >= len(ids):
        return ids
    return random.Random(RANDOM_SEED).sample(ids, k)


def _fetch_and_transform_batch(
    db,
    batch_ids,
    lang_filter,
    actor_lang_map: Dict[str, str] | None,
    platform: str,
):
    docs = _fetch_post_docs_by_ids(db, batch_ids)
    return _rows_from_posts(
        docs,
        lang_filter=lang_filter,
        actor_lang_map=actor_lang_map,
        platform_hint=platform,
    )


def _build_actor_lang_map(
    db, platform: str, actor_usernames: List[str]
) -> Dict[str, str]:
    if not actor_usernames:
        return {}
    cur = db.actor_metric.find(
        {"platform": platform, "actor_username": {"$in": actor_usernames}},
        {"actor_username": 1, "lang": 1, "_id": 0},
    )
    return {doc.get("actor_username"): (doc.get("lang") or None) for doc in cur}


def sample_one_platform(db, platform: str, lang_filter: Set[str]):
    print(f"\n=== {platform.upper()} ===")

    # Get total actors for this platform
    try:
        total_actors = db.actor_metric.count_documents(
            {"platform": platform, "actor_username": {"$ne": None}}
        )
    except Exception:
        total_actors = 1000

    target_accounts = min(
        MAX_ACTORS_PER_PLATFORM,
        max(100, int(total_actors * (ACCOUNT_PERCENTAGE / 100.0))),
    )

    print(f"📊 Total {platform} actors: {total_actors:,}")
    print(f"🎯 Sampling {target_accounts:,} actors ({ACCOUNT_PERCENTAGE}%)")

    # Get actors (already filtered by actor-level language)
    accounts = _pick_accounts_actor_metric(db, platform, target_accounts, lang_filter)
    print(f"✅ Selected {len(accounts)} actors")

    if not accounts:
        print(f"❌ No actors found for {platform}")
        return pd.DataFrame(columns=DERIVED_COLUMNS)

    # Collect post IDs
    all_ids = []
    for a in tqdm(accounts, desc=f"[{platform}] Collecting post IDs"):
        ids = _collect_actor_post_ids(
            db,
            a,
            platform,
            MIN_POSTS_PER_ACTOR,
            POSTS_PER_ACTOR_SOFT,
            POST_PERCENTAGE,
            MAX_POSTS_PER_ACTOR,
        )
        all_ids.extend(ids)

    all_ids = list(dict.fromkeys(all_ids))
    print(f"📦 Total post IDs: {len(all_ids):,}")

    if not all_ids:
        print(f"❌ No posts found for {platform}")
        return pd.DataFrame(columns=DERIVED_COLUMNS)

    # Process in batches
    rows_all: List[Dict[str, Any]] = []
    skipped_total = 0
    part_files: List[str] = []
    part_idx = 0

    batches = [
        all_ids[i : i + BATCH_FETCH_SIZE]
        for i in range(0, len(all_ids), BATCH_FETCH_SIZE)
    ]

    # Build actor -> language map once per platform
    actor_lang_map = _build_actor_lang_map(db, platform, accounts)

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = [
            ex.submit(
                _fetch_and_transform_batch,
                db,
                batch_ids,
                lang_filter,
                actor_lang_map,
                platform,
            )
            for batch_ids in batches
        ]

        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=f"[{platform}] Processing posts",
        ):
            rows, skipped = fut.result()
            skipped_total += skipped
            rows_all.extend(rows)
            # Flush in parts to bound memory
            if len(rows_all) >= CHUNK_SAVE_SIZE:
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                part_path = os.path.join(
                    OUTPUT_DIR,
                    f"{platform}_part_{part_idx:04d}_{len(rows_all)}_{ts}.csv",
                )
                pd.DataFrame(rows_all).to_csv(part_path, index=False)
                part_files.append(part_path)
                part_idx += 1
                rows_all.clear()

    # Write any remainder
    if rows_all:
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        part_path = os.path.join(
            OUTPUT_DIR,
            f"{platform}_part_{part_idx:04d}_{len(rows_all)}_{ts}.csv",
        )
        pd.DataFrame(rows_all).to_csv(part_path, index=False)
        part_files.append(part_path)
        part_idx += 1
        rows_all.clear()

    # Report total and return a small dataframe (or empty to save memory)
    total_rows = 0
    for pth in part_files:
        try:
            total_rows += (
                sum(1 for _ in open(pth, "r", encoding="utf-8", errors="ignore")) - 1
            )
        except Exception:
            pass
    print(
        f"📈 {platform}: {total_rows:,} target language posts, {skipped_total:,} filtered out"
    )
    # Do not load all parts back; return empty DF to avoid memory blowup
    return pd.DataFrame()


# MAIN EXECUTION
print("🚀 ALL PLATFORMS TARGET LANGUAGE SAMPLING (FIXED)")
print(f"🎯 Languages: {LANG_FILTER}")
print("=" * 50)

client, db = _client_and_db()

# Normalize lang filter
lang_filter = {x.lower() for x in LANG_FILTER}

# Get all platforms
platforms_list = _normalize_platforms(db, PLATFORMS)
print(f"📊 Platforms to process: {platforms_list}")

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Process each platform
all_platform_dfs = []
overall_start = time.time()

for platform in platforms_list:
    platform_start = time.time()

    try:
        df_platform = sample_one_platform(db, platform, lang_filter)

        if not df_platform.empty:
            # Save individual platform results
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            platform_file = os.path.join(
                OUTPUT_DIR,
                f"{platform}_target_langs_{len(df_platform)}_{timestamp}.csv",
            )
            df_platform.to_csv(platform_file, index=False)

            print(f"💾 Saved {platform}: {len(df_platform):,} posts → {platform_file}")

            if "lang" in df_platform.columns:
                lang_dist = df_platform["lang"].value_counts()
                print(f"📊 {platform} language distribution: {dict(lang_dist)}")

            all_platform_dfs.append(df_platform)

        platform_elapsed = time.time() - platform_start
        print(f"⏱️ {platform} completed in {platform_elapsed:.1f}s")

    except Exception as e:
        print(f"❌ Error processing {platform}: {e}")
        continue

# Combine all platforms
if all_platform_dfs:
    df_combined = pd.concat(all_platform_dfs, ignore_index=True)

    # Save combined results
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    combined_file = os.path.join(
        OUTPUT_DIR, f"ALL_platforms_target_langs_{len(df_combined)}_{timestamp}.csv"
    )
    df_combined.to_csv(combined_file, index=False)

    print(f"\n🎉 OVERALL SUCCESS!")
    print(f"💾 Combined: {len(df_combined):,} posts → {combined_file}")

    if "lang" in df_combined.columns:
        print(f"📊 Overall language distribution:")
        print(df_combined["lang"].value_counts())

    if "platform" in df_combined.columns:
        print(f"📱 Platform distribution:")
        print(df_combined["platform"].value_counts())

else:
    print("❌ No target language posts found across any platform")

overall_elapsed = time.time() - overall_start
print(f"⏱️ Total processing time: {overall_elapsed:.1f}s")

client.close()
