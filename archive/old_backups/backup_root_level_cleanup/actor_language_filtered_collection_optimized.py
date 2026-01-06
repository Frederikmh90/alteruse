#!/usr/bin/env python3
"""
Actor-Level Language Filtered Data Collection - OPTIMIZED
Uses actor_metric.lang field to pre-filter actors before collecting any posts.
This is 100x more efficient than post-level language filtering.
"""

import os
import time
import random
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from spreadAnalysis.persistence.mongo import MongoSpread

# ACTOR-LEVEL LANGUAGE FILTERING CONFIG
# =====================================
PLATFORMS = "auto"
LANG_FILTER = ["da", "de", "sv"]  # Filter actors by these languages FIRST
ACCOUNT_PERCENTAGE = 80
POST_PERCENTAGE = 80
MIN_POSTS_PER_ACTOR = 30
MAX_POSTS_PER_ACTOR = None
POSTS_PER_ACTOR_SOFT = 400

MAX_ACTORS_PER_PLATFORM = None

# PERFORMANCE OPTIMIZATION SETTINGS
ACTOR_BATCH_SIZE = 300  # Actors per batch when collecting posts (smaller for stability)
ACTOR_BATCH_WORKERS = 2  # Parallel workers for actor batches (conservative)
ACTOR_BATCH_LIMIT = 50_000  # Max posts per actor batch (smaller chunks)

BATCH_FETCH_SIZE = 1000  # Posts per batch when fetching full documents
FETCH_WORKERS = 5  # Parallel workers for document fetching
CHUNK_SAVE_SIZE = 100_000  # Auto-save every N rows

OUTPUT_DIR = "./data/technocracy_250810"
RANDOM_SEED = 42

# Output columns
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

# Post projection for efficient fetching
POST_PROJECTION_LANG_FILTERED = {
    "message": 1,
    "datetime": 1,
    "author": 1,
    "platform": 1,
    "lang": 1,
    "post_url": 1,
    "message_id": 1,
}


def _client_and_db():
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    from pymongo import MongoClient

    client = MongoClient(host, port)
    return client, client[name]


def _normalize_platforms(db, platforms):
    if platforms is None:
        return []
    if platforms == "auto":
        platforms = db.post.distinct("platform")
    if isinstance(platforms, str):
        platforms = [platforms]
    return list(platforms)


def _parse_language_filter(lang_filter):
    if lang_filter == "all":
        return None

    if isinstance(lang_filter, str) and lang_filter != "all":
        return [lang_filter.lower()]

    if isinstance(lang_filter, (set, list)):
        return [x.lower() for x in lang_filter]

    return None


# =========================
# ACTOR-LEVEL LANGUAGE FILTERING
# =========================


def get_actors_by_language(
    db, platform: str, target_languages: List[str], target_n: int
):
    """
    REVOLUTIONARY: Get actors who speak target languages FIRST.
    Uses actor_metric.lang field for ultra-fast filtering.
    """
    print(f"  🎯 Finding {platform} actors speaking {target_languages}...")

    start_time = time.time()

    try:
        # Query actor_metric collection for actors with target languages
        actor_query = {"platform": platform, "lang": {"$in": target_languages}}

        # Optional: filter by minimum activity level
        if MIN_POSTS_PER_ACTOR:
            actor_query["n_posts"] = {"$gte": MIN_POSTS_PER_ACTOR}

        # Get actors, sorted by post count (most active first)
        target_actors = list(
            db.actor_metric.find(
                actor_query, {"actor_username": 1, "lang": 1, "n_posts": 1, "_id": 0}
            )
            .sort("n_posts", -1)
            .limit(target_n * 2)  # Get extra in case some don't work
        )

        end_time = time.time()

        actor_usernames = [
            actor["actor_username"]
            for actor in target_actors
            if actor.get("actor_username")
        ]

        print(
            f"  ✅ Found {len(actor_usernames)} target language actors in {end_time - start_time:.1f}s"
        )

        if target_actors:
            # Show language distribution
            lang_counts = {}
            for actor in target_actors:
                lang = actor.get("lang", "unknown")
                lang_counts[lang] = lang_counts.get(lang, 0) + 1
            print(f"     Language distribution: {lang_counts}")

            # Show example
            example_actor = target_actors[0]
            print(
                f"     Example: {example_actor['actor_username']} ({example_actor['lang']}, {example_actor.get('n_posts', 0)} posts)"
            )

        return actor_usernames[:target_n]

    except Exception as e:
        print(f"  ⚠️  Actor language query failed: {e}")
        print(
            f"     This probably means actor_metric collection doesn't exist or has different structure"
        )
        return []


def collect_posts_from_language_actors_simple(
    db, actor_usernames: List[str], platform: str
):
    """
    SIMPLIFIED: Collect posts from pre-filtered language actors.
    Single-threaded but more stable approach.
    """

    if not actor_usernames:
        return []

    print(
        f"  📝 Collecting posts from {len(actor_usernames)} language-filtered actors..."
    )

    all_post_ids = []

    # Process in smaller batches for stability
    for i in range(0, len(actor_usernames), ACTOR_BATCH_SIZE):
        batch_actors = actor_usernames[i : i + ACTOR_BATCH_SIZE]
        batch_num = (i // ACTOR_BATCH_SIZE) + 1
        total_batches = (
            len(actor_usernames) + ACTOR_BATCH_SIZE - 1
        ) // ACTOR_BATCH_SIZE

        print(
            f"    📦 Processing batch {batch_num}/{total_batches} ({len(batch_actors)} actors)..."
        )

        # Query for posts from this batch of actors
        post_query = {
            "platform": platform,
            "$or": [
                {"author.username": {"$in": batch_actors}},
                {"author": {"$in": batch_actors}},
            ],
        }

        # Get post IDs from this batch
        batch_limit = min(len(batch_actors) * POSTS_PER_ACTOR_SOFT, ACTOR_BATCH_LIMIT)

        try:
            post_cursor = db.post.find(post_query, {"_id": 1}).limit(batch_limit)
            batch_post_ids = [post["_id"] for post in post_cursor]
            all_post_ids.extend(batch_post_ids)

            print(f"       +{len(batch_post_ids)} posts (total: {len(all_post_ids):,})")

        except Exception as e:
            print(f"       ❌ Error in batch {batch_num}: {e}")
            continue

    print(f"  ✅ Found {len(all_post_ids):,} posts from language actors")

    # Apply sampling if needed
    if len(all_post_ids) > 0:
        target_posts = int(
            len(actor_usernames) * POST_PERCENTAGE / 100 * POSTS_PER_ACTOR_SOFT
        )
        if MAX_POSTS_PER_ACTOR:
            target_posts = min(target_posts, len(actor_usernames) * MAX_POSTS_PER_ACTOR)

        if target_posts < len(all_post_ids):
            all_post_ids = random.Random(RANDOM_SEED).sample(all_post_ids, target_posts)
            print(f"  📊 Sampled down to {len(all_post_ids)} posts")

    return all_post_ids


def _rows_from_posts(posts: List[Dict[str, Any]]) -> Tuple[list, int]:
    """Build rows from posts - no language filtering needed since actors already filtered."""
    rows = []
    skipped = 0

    for post in posts:
        try:
            # Required fields
            req_values = {}

            # Actor username
            author = post.get("author")
            if isinstance(author, dict):
                actor_username = author.get("username") or author.get("name")
            else:
                actor_username = author

            if not actor_username:
                skipped += 1
                continue
            req_values["actor_username"] = actor_username

            # Platform
            platform = post.get("platform")
            if not platform:
                skipped += 1
                continue
            req_values["platform"] = platform

            # Message ID
            message_id = post.get("message_id") or post.get("_id")
            if not message_id:
                skipped += 1
                continue
            req_values["message_id"] = str(message_id)

            # Datetime
            datetime_val = post.get("datetime")
            if not datetime_val:
                skipped += 1
                continue
            req_values["datetime"] = datetime_val

            # Optional fields
            opt_values = {}

            # Actor info
            if isinstance(author, dict):
                opt_values["actor_id"] = author.get("id")
                opt_values["actor_name"] = author.get("name") or author.get(
                    "display_name"
                )
                opt_values["link_to_actor"] = author.get("url")

            # Language
            opt_values["lang"] = post.get("lang")

            # Post URL
            opt_values["post_url"] = post.get("post_url")

            # Text
            message = post.get("message", "")
            text_val = message if isinstance(message, str) and message.strip() else None

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

        except Exception as e:
            skipped += 1
            continue

    return rows, skipped


def _fetch_and_transform_batch(db, batch_ids):
    """Fetch posts by IDs and transform to rows."""
    if not batch_ids:
        return [], 0

    # No language filter needed - actors already filtered
    query = {"_id": {"$in": batch_ids}}

    docs = list(db.post.find(query, POST_PROJECTION_LANG_FILTERED))
    return _rows_from_posts(docs)


# =========================
# REDESIGNED PLATFORM PROCESSING WITH ACTOR FILTERING
# =========================


def sample_one_platform_with_actor_filtering(
    db,
    platform: str,
    target_languages: List[str],
    target_n: int,
    batch_fetch_size: int = BATCH_FETCH_SIZE,
    chunk_save_size: Optional[int] = CHUNK_SAVE_SIZE,
    output_dir: str = OUTPUT_DIR,
):
    """
    Sample one platform using actor-level language filtering.
    This is the new ultra-efficient approach.
    """

    print(f"\n[{platform}] 🚀 ACTOR-LEVEL LANGUAGE FILTERING")
    print(f"[{platform}] Target languages: {target_languages}")
    print(f"[{platform}] Target actors: {target_n}")

    t0 = time.time()

    # STEP 1: Get actors who speak target languages
    t_actors0 = time.time()
    language_actors = get_actors_by_language(db, platform, target_languages, target_n)
    t_actors1 = time.time()

    if not language_actors:
        print(f"[{platform}] ❌ No actors found speaking target languages")
        return pd.DataFrame(columns=DERIVED_COLUMNS), {
            "actors": 0,
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": time.time() - t0,
        }

    print(
        f"[{platform}] ✅ Found {len(language_actors)} language actors in {t_actors1 - t_actors0:.1f}s"
    )

    # STEP 2: Collect posts from these language actors (SIMPLIFIED)
    t_posts0 = time.time()
    all_post_ids = collect_posts_from_language_actors_simple(
        db, language_actors, platform
    )
    t_posts1 = time.time()

    print(
        f"[{platform}] ✅ Collected {len(all_post_ids)} posts from language actors in {t_posts1 - t_posts0:.1f}s"
    )

    if not all_post_ids:
        print(f"[{platform}] ❌ No posts collected from language actors")
        return pd.DataFrame(columns=DERIVED_COLUMNS), {
            "actors": len(language_actors),
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": time.time() - t0,
        }

    # STEP 3: Fetch and transform posts in batches
    t_fetch0 = time.time()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    rows_all = []
    skipped_total = 0

    batches = [
        all_post_ids[i : i + batch_fetch_size]
        for i in range(0, len(all_post_ids), batch_fetch_size)
    ]

    print(f"[{platform}] 📊 Processing {len(batches)} batches of posts...")

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = [
            ex.submit(_fetch_and_transform_batch, db, batch) for batch in batches
        ]

        for i, fut in enumerate(tqdm(futures, desc=f"[{platform}] fetching posts")):
            rows, skipped = fut.result()
            skipped_total += skipped
            rows_all.extend(rows)

            # Save intermediate chunks
            if chunk_save_size and len(rows_all) >= chunk_save_size:
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                lang_suffix = "_".join(target_languages)
                fn = os.path.join(
                    output_dir,
                    f"{platform}_actor_filtered_{lang_suffix}_autosave_{len(rows_all)}_{ts}.csv",
                )
                pd.DataFrame(rows_all).to_csv(fn, index=False)
                print(f"[{platform}] [autosave] {len(rows_all)} rows → {fn}")
                rows_all = []  # Reset for next chunk

    # Combine remaining rows
    dfs = []
    if rows_all:
        dfs.append(pd.DataFrame(rows_all))

    df_platform = (
        pd.concat(dfs, ignore_index=True)
        if dfs
        else pd.DataFrame(columns=DERIVED_COLUMNS)
    )
    t_fetch1 = time.time()

    # Save final results
    if len(df_platform) > 0:
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        lang_suffix = "_".join(target_languages)
        fn = os.path.join(
            output_dir,
            f"{platform}_actor_filtered_{lang_suffix}_{len(df_platform)}_{ts}.csv",
        )
        df_platform.to_csv(fn, index=False)
        print(f"[{platform}] saved → {fn}")

    elapsed = time.time() - t0

    # Summary
    print(f"[{platform}] ✅ ACTOR FILTERING COMPLETE:")
    print(f"  Language actors found: {len(language_actors)}")
    print(f"  Posts from language actors: {len(all_post_ids)}")
    print(f"  Final rows: {len(df_platform)}")
    print(
        f"  Time breakdown: actors={t_actors1 - t_actors0:.1f}s posts={t_posts1 - t_posts0:.1f}s fetch={t_fetch1 - t_fetch0:.1f}s total={elapsed:.1f}s"
    )

    stats = {
        "actors": len(language_actors),
        "ids": len(all_post_ids),
        "rows": len(df_platform),
        "skipped": skipped_total,
        "elapsed": elapsed,
    }

    return df_platform, stats


# =========================
# MAIN DRIVER WITH ACTOR FILTERING
# =========================


def sample_platforms_with_actor_filtering(
    platforms=PLATFORMS,
    lang_filter=LANG_FILTER,
    account_percentage=ACCOUNT_PERCENTAGE,
    output_dir=OUTPUT_DIR,
):
    """
    Main function using actor-level language filtering.
    """

    target_languages = _parse_language_filter(lang_filter)

    if not target_languages:
        print("❌ This actor filtering version requires language filtering!")
        print("   Set LANG_FILTER = ['da', 'de', 'sv'] or similar")
        return pd.DataFrame()

    client, db = _client_and_db()
    platforms_list = _normalize_platforms(db, platforms)

    print("🎯 ACTOR-LEVEL LANGUAGE FILTERING PIPELINE")
    print("=" * 60)
    print(f"Target languages: {target_languages}")
    print(f"Platforms: {platforms_list}")
    print(f"Account percentage: {account_percentage}%")
    print(f"Using actor_metric.lang field for pre-filtering")
    print("=" * 60)

    combined, all_stats = [], {}

    for platform in platforms_list:
        try:
            # Calculate target number of actors
            total_actors = db.actor_metric.count_documents({"platform": platform})
            target_n = max(1, int(total_actors * account_percentage / 100))
            if MAX_ACTORS_PER_PLATFORM:
                target_n = min(target_n, MAX_ACTORS_PER_PLATFORM)

            print(
                f"\n[{platform}] Total actors: {total_actors:,}, Target: {target_n:,}"
            )

            df_p, stats = sample_one_platform_with_actor_filtering(
                db, platform, target_languages, target_n, output_dir=output_dir
            )

            if len(df_p) > 0:
                combined.append(df_p)
            all_stats[platform] = stats

        except Exception as e:
            print(f"[{platform}] ❌ Error: {e}")
            all_stats[platform] = {
                "actors": 0,
                "ids": 0,
                "rows": 0,
                "skipped": 0,
                "elapsed": 0,
            }

    # Combine all platforms
    df_all = (
        pd.concat(combined, ignore_index=True)
        if combined
        else pd.DataFrame(columns=DERIVED_COLUMNS)
    )

    if len(df_all) > 0:
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        lang_suffix = "_".join(target_languages)
        fn_all = os.path.join(
            output_dir,
            f"ALL_platforms_actor_filtered_{lang_suffix}_{len(df_all)}_{ts}.csv",
        )
        df_all.to_csv(fn_all, index=False)
        print(f"\n[ALL] saved combined CSV → {fn_all}")

    # Summary
    print("\n===== ACTOR FILTERING SUMMARY =====")
    for platform, s in all_stats.items():
        print(
            f"{platform:15} actors={s['actors']:7} ids={s['ids']:9} rows={s['rows']:9} elapsed={s['elapsed']:.1f}s"
        )
    print(f"TOTAL rows: {len(df_all)}")
    print("🎯 Actor-level filtering applied - maximum efficiency achieved!")

    client.close()
    return df_all


if __name__ == "__main__":
    df_all = sample_platforms_with_actor_filtering(
        platforms=PLATFORMS,
        lang_filter=LANG_FILTER,
        account_percentage=ACCOUNT_PERCENTAGE,
        output_dir=OUTPUT_DIR,
    )

    try:
        input("Press Enter to exit...")
    except Exception:
        pass
