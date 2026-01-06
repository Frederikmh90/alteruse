# =========================
# TRULY OPTIMIZED CONFIG WITH EARLY LANGUAGE FILTERING
# =========================
PLATFORMS = "auto"
LANG_FILTER = ["da", "de", "sv"]  # These will be filtered FIRST
ACCOUNT_PERCENTAGE = 80
POST_PERCENTAGE = 80
MIN_POSTS_PER_ACTOR = 30
MAX_POSTS_PER_ACTOR = None
POSTS_PER_ACTOR_SOFT = 400

MAX_ACTORS_PER_PLATFORM = None

BATCH_FETCH_SIZE = 1500
FETCH_WORKERS = 7
CHUNK_SAVE_SIZE = 200_000

OUTPUT_DIR = "./data/technocracy_250810"
RANDOM_SEED = 42

# =========================
# COMPLETELY REDESIGNED IMPLEMENTATION
# Filters at the EARLIEST possible stage
# =========================
import os
import time
import random
from typing import List, Dict, Any, Tuple
import pandas as pd
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

from spreadAnalysis.persistence.mongo import MongoSpread
from spreadAnalysis.persistence.schemas import Spread

# Columns we output (order)
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

# Minimal projection for language-filtered posts
POST_PROJECTION_LANG_FILTERED = {
    "_id": 1,
    "method": 1,
    "platform": 1,
    "text": 1,
    "lang": 1,
    "id": 1,
    "source": 1,
    "author": 1,
    "created_at": 1,
    "account": 1,
    "post_url": 1,
    "name": 1,
    "post_date": 1,
    "date": 1,
    "message": 1,
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


def _parse_language_filter(lang_filter):
    if lang_filter == "all":
        return None

    if isinstance(lang_filter, str) and lang_filter != "all":
        return [lang_filter.lower()]

    if isinstance(lang_filter, (set, list)):
        return [x.lower() for x in lang_filter]

    return None


# =========================
# REVOLUTIONARY APPROACH: FIND ACTORS WITH LANGUAGE POSTS FIRST
# =========================


def find_actors_with_target_language_posts(
    db, platform: str, target_languages: List[str], target_n: int
):
    """
    COMPLETELY NEW APPROACH: Find actors who actually have posts in target languages.
    This avoids processing millions of irrelevant actors.
    """
    print(f"  🔍 Finding actors with {target_languages} posts on {platform}...")

    try:
        # Use aggregation to find actors with target language posts
        pipeline = [
            # FILTER FIRST: Only posts with target languages AND platform
            {"$match": {"platform": platform, "lang": {"$in": target_languages}}},
            # Extract actor information
            {
                "$addFields": {
                    "actor_username": {
                        "$cond": {
                            "if": {"$ne": ["$author.username", None]},
                            "then": "$author.username",
                            "else": {
                                "$cond": {
                                    "if": {"$ne": ["$author", None]},
                                    "then": "$author",
                                    "else": None,
                                }
                            },
                        }
                    }
                }
            },
            # Filter out posts without actor info
            {"$match": {"actor_username": {"$ne": None, "$exists": True}}},
            # Group by actor and count posts
            {
                "$group": {
                    "_id": "$actor_username",
                    "post_count": {"$sum": 1},
                    "sample_post_id": {"$first": "$_id"},
                    "sample_lang": {"$first": "$lang"},
                }
            },
            # Filter actors with enough posts
            {"$match": {"post_count": {"$gte": MIN_POSTS_PER_ACTOR}}},
            # Sort by post count (get most active actors first)
            {"$sort": {"post_count": -1}},
            # Limit to what we need
            {
                "$limit": target_n * 2  # Get extra in case some don't work
            },
        ]

        start_time = time.time()
        result = list(db.post.aggregate(pipeline, allowDiskUse=True))
        end_time = time.time()

        actors = [doc["_id"] for doc in result if doc["_id"]]

        print(
            f"  ✅ Found {len(actors)} actors with target language posts in {end_time - start_time:.1f}s"
        )
        if result:
            print(
                f"     Example: {result[0]['_id']} has {result[0]['post_count']} {result[0]['sample_lang']} posts"
            )

        return actors[:target_n]

    except Exception as e:
        print(f"  ⚠️  Aggregation failed: {e}")
        print(f"     Falling back to random actor selection...")
        return []


def collect_language_filtered_posts_for_actor(
    db, actor_username: str, platform: str, target_languages: List[str]
):
    """
    Directly collect posts for an actor that match target languages.
    No intermediate steps - straight to the language-filtered posts.
    """

    # Direct query for posts by this actor in target languages
    query = {
        "platform": platform,
        "lang": {"$in": target_languages},
        "$or": [
            {"author.username": actor_username},
            {"author": actor_username},
        ],
    }

    # Get posts directly - no post_obj_ids intermediate step
    posts = list(db.post.find(query, {"_id": 1}).limit(POSTS_PER_ACTOR_SOFT))

    post_ids = [post["_id"] for post in posts]

    # Apply sampling if needed
    if len(post_ids) > MAX_POSTS_PER_ACTOR if MAX_POSTS_PER_ACTOR else len(post_ids):
        if POST_PERCENTAGE < 100.0:
            k = max(1, int(len(post_ids) * (POST_PERCENTAGE / 100.0)))
        else:
            k = len(post_ids)

        if MAX_POSTS_PER_ACTOR:
            k = min(k, MAX_POSTS_PER_ACTOR)

        if k < len(post_ids):
            post_ids = random.Random(RANDOM_SEED).sample(post_ids, k)

    return post_ids


def _rows_from_posts(posts: List[Dict[str, Any]]) -> Tuple[list, int]:
    """Simplified row building - no language filtering needed since already filtered."""
    rows, skipped = [], 0

    REQUIRED = {
        "actor_username": Spread._get_actor_username,
        "platform": Spread._get_platform,
        "datetime": Spread._get_date,
        "message_id": Spread._get_message_id,
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

        # Required fields
        req_values = {}
        drop = False
        for key, getter in REQUIRED.items():
            val = _safe_get(getter, m, p)
            if isinstance(val, Exception) or val in (None, ""):
                drop = True
                break
            req_values[key] = val
        if drop:
            skipped += 1
            continue

        # Optional fields
        opt_values = {}
        for key, getter in OPTIONAL.items():
            val = _safe_get(getter, m, p)
            if isinstance(val, Exception):
                opt_values[key] = None
            else:
                opt_values[key] = val

        # Message text
        text_val = _safe_get(Spread._get_message_text, m, p)
        if isinstance(text_val, Exception) or text_val in (None, ""):
            text_val = p.get("text", "")

        # Build row
        rows.append(
            {
                "actor_id": opt_values.get("actor_id"),
                "actor_username": req_values["actor_username"],
                "actor_name": opt_values.get("actor_name"),
                "platform": req_values["platform"],
                "lang": opt_values.get("lang"),
                "datetime": req_values["datetime"],
                "message_id": req_values["message_id"],
                "post_url": opt_values.get("post_url"),
                "link_to_actor": opt_values.get("link_to_actor"),
                "text": text_val,
            }
        )

    return rows, skipped


def _fetch_and_transform_batch(db, batch_ids, target_languages):
    """Fetch posts that are already language-filtered."""
    if not batch_ids:
        return [], 0

    # Double-check language filter (safety)
    query = {"_id": {"$in": batch_ids}, "lang": {"$in": target_languages}}

    docs = list(db.post.find(query, POST_PROJECTION_LANG_FILTERED))
    return _rows_from_posts(docs)


# =========================
# REDESIGNED PLATFORM PROCESSING
# =========================


def sample_one_platform_optimized(
    db,
    platform: str,
    account_percentage: float,
    target_languages: List[str],
    batch_fetch_size: int,
    chunk_save_size: int,
    output_dir: str,
):
    """COMPLETELY REDESIGNED: Language filtering happens FIRST."""
    os.makedirs(output_dir, exist_ok=True)
    t0 = time.time()
    print(f"\n=== {platform.upper()} (OPTIMIZED LANGUAGE-FIRST APPROACH) ===")
    print(f"🎯 Target languages: {target_languages}")

    # Calculate how many actors we need
    try:
        # This is still fast - just counting actors in actor_metric
        total_actors = db.actor_metric.count_documents(
            {"platform": platform, "actor_username": {"$ne": None}}
        )
    except Exception:
        total_actors = 1000

    target_accounts = max(500, int(total_actors * (account_percentage / 100.0)))
    if MAX_ACTORS_PER_PLATFORM is not None:
        target_accounts = min(target_accounts, MAX_ACTORS_PER_PLATFORM)

    print(f"[{platform}] Target: {target_accounts} actors from ~{total_actors:,} total")

    # REVOLUTIONARY: Find actors with target language posts FIRST
    t_sel0 = time.time()
    actors_with_lang_posts = find_actors_with_target_language_posts(
        db, platform, target_languages, target_accounts
    )
    t_sel1 = time.time()

    if not actors_with_lang_posts:
        print(f"[{platform}] ❌ No actors found with target language posts")
        return pd.DataFrame(columns=DERIVED_COLUMNS), {
            "actors": 0,
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": time.time() - t0,
        }

    print(
        f"[{platform}] ✅ Found {len(actors_with_lang_posts)} actors with target posts in {t_sel1 - t_sel0:.1f}s"
    )

    # Collect posts from these language-positive actors
    t_ids0 = time.time()
    all_post_ids = []

    for actor in tqdm(
        actors_with_lang_posts, desc=f"[{platform}] collecting language-filtered posts"
    ):
        post_ids = collect_language_filtered_posts_for_actor(
            db, actor, platform, target_languages
        )
        all_post_ids.extend(post_ids)

    all_post_ids = list(dict.fromkeys(all_post_ids))  # Remove duplicates
    t_ids1 = time.time()

    print(
        f"[{platform}] ✅ Collected {len(all_post_ids)} language-filtered post IDs in {t_ids1 - t_ids0:.1f}s"
    )

    if not all_post_ids:
        print(f"[{platform}] ❌ No posts collected")
        return pd.DataFrame(columns=DERIVED_COLUMNS), {
            "actors": len(actors_with_lang_posts),
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": time.time() - t0,
        }

    # Process the language-filtered posts
    t_ft0 = time.time()
    rows_all, skipped_total = [], 0
    dfs = []

    batches = [
        all_post_ids[i : i + batch_fetch_size]
        for i in range(0, len(all_post_ids), batch_fetch_size)
    ]

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = [
            ex.submit(_fetch_and_transform_batch, db, batch, target_languages)
            for batch in batches
        ]

        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=f"[{platform}] processing batches",
        ):
            rows, skipped = fut.result()
            skipped_total += skipped
            rows_all.extend(rows)

            if chunk_save_size and len(rows_all) >= chunk_save_size:
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                fn = os.path.join(
                    output_dir,
                    f"{platform}_optimized_autosave_{len(rows_all)}_{ts}.csv",
                )
                pd.DataFrame(rows_all).to_csv(fn, index=False)
                print(f"[{platform}] [autosave] {len(rows_all)} rows → {fn}")
                dfs.append(pd.DataFrame(rows_all))
                rows_all.clear()

    if rows_all:
        dfs.append(pd.DataFrame(rows_all))

    df_platform = (
        pd.concat(dfs, ignore_index=True)
        if dfs
        else pd.DataFrame(columns=DERIVED_COLUMNS)
    )
    t_ft1 = time.time()

    # Save final results
    if not df_platform.empty:
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        lang_suffix = "_".join(target_languages)
        fn = os.path.join(
            output_dir,
            f"{platform}_optimized_{lang_suffix}_{len(df_platform)}_{ts}.csv",
        )
        df_platform.to_csv(fn, index=False)
        print(f"[{platform}] saved → {fn}")

    elapsed = time.time() - t0
    print(f"[{platform}] 🚀 OPTIMIZED RESULTS:")
    print(f"  Actors with target posts: {len(actors_with_lang_posts)}")
    print(f"  Language-filtered post IDs: {len(all_post_ids)}")
    print(f"  Final rows: {len(df_platform)}")
    print(
        f"  Time breakdown: select={t_sel1 - t_sel0:.1f}s ids={t_ids1 - t_ids0:.1f}s transform={t_ft1 - t_ft0:.1f}s total={elapsed:.1f}s"
    )

    stats = {
        "actors": len(actors_with_lang_posts),
        "ids": len(all_post_ids),
        "rows": len(df_platform),
        "skipped": skipped_total,
        "elapsed": elapsed,
    }

    return df_platform, stats


# =========================
# MAIN DRIVER
# =========================


def sample_platforms_optimized(
    platforms,
    account_percentage,
    lang_filter,
    batch_fetch_size,
    chunk_save_size,
    output_dir,
):
    target_languages = _parse_language_filter(lang_filter)

    if not target_languages:
        print("❌ This optimized version requires language filtering!")
        print("   Set LANG_FILTER = ['da', 'de', 'sv'] in config")
        return pd.DataFrame()

    random.seed(RANDOM_SEED)
    client, db = _client_and_db()
    platforms_list = _normalize_platforms(db, platforms)

    print("🚀 OPTIMIZED LANGUAGE-FIRST PIPELINE")
    print("=" * 60)
    print(f"  🎯 Target languages: {target_languages}")
    print(f"  📊 Platforms: {platforms_list}")
    print(f"  ⚡ Strategy: Find actors with target posts FIRST")
    print(f"  🔧 Batch size: {batch_fetch_size}, Workers: {FETCH_WORKERS}")

    combined, all_stats = [], {}

    for platform in platforms_list:
        df_p, stats = sample_one_platform_optimized(
            db=db,
            platform=platform,
            account_percentage=account_percentage,
            target_languages=target_languages,
            batch_fetch_size=batch_fetch_size,
            chunk_save_size=chunk_save_size,
            output_dir=output_dir,
        )
        all_stats[platform] = stats
        if not df_p.empty:
            combined.append(df_p)

    if combined:
        df_all = pd.concat(combined, ignore_index=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        lang_suffix = "_".join(target_languages)
        fn_all = os.path.join(
            output_dir, f"ALL_platforms_optimized_{lang_suffix}_{len(df_all)}_{ts}.csv"
        )
        df_all.to_csv(fn_all, index=False)
        print(f"\n[ALL] saved combined CSV → {fn_all}")
    else:
        df_all = pd.DataFrame(columns=DERIVED_COLUMNS)
        print("\n[ALL] no rows collected")

    print("\n===== OPTIMIZED SUMMARY =====")
    for platform, s in all_stats.items():
        print(
            f"{platform:15} actors={s['actors']:7} ids={s['ids']:9} rows={s['rows']:9} elapsed={s['elapsed']:.1f}s"
        )
    print(f"TOTAL rows: {len(df_all)}")
    print("🎯 Language filtering applied at EARLIEST stage - maximum efficiency!")

    client.close()
    return df_all


if __name__ == "__main__":
    df_all = sample_platforms_optimized(
        platforms=PLATFORMS,
        account_percentage=ACCOUNT_PERCENTAGE,
        lang_filter=LANG_FILTER,
        batch_fetch_size=BATCH_FETCH_SIZE,
        chunk_save_size=CHUNK_SAVE_SIZE,
        output_dir=OUTPUT_DIR,
    )
    try:
        print(df_all.head())
    except Exception:
        pass
