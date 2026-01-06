#!/usr/bin/env python3
"""
Percentage-Based Language Sampling Script
Samples a percentage of posts from ALL Danish/German/Swedish actors across platforms.
Ensures temporal diversity and representative sampling.
"""

import os
import time
import random
import logging
import datetime
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from spreadAnalysis.persistence.mongo import MongoSpread

# PERCENTAGE-BASED SAMPLING CONFIG
# ================================
PLATFORMS = "auto"  # ["telegram"] for testing, "auto" for all
LANG_FILTER = ["da", "de", "sv"]
SAMPLE_PERCENTAGE = 50  # Sample 50% of posts from target language actors
MIN_POSTS_PER_ACTOR = 1  # Include all actors (no minimum)

# PERFORMANCE SETTINGS
ACTOR_BATCH_SIZE = 200  # Smaller batches for stability
POST_BATCH_SIZE = 1000  # Posts per batch when fetching documents
FETCH_WORKERS = 3  # Conservative parallel workers

# OUTPUT AND LOGGING
OUTPUT_DIR = "./data/percentage_sampled"
LOG_DIR = "./logs"
CHUNK_SAVE_SIZE = 50_000  # Auto-save every 50k rows
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

# MongoDB projection for efficiency
POST_PROJECTION = {
    "message": 1,
    "datetime": 1,
    "author": 1,
    "platform": 1,
    "lang": 1,
    "post_url": 1,
    "message_id": 1,
}


def setup_logging(platform: str = "test") -> logging.Logger:
    """Set up comprehensive logging system."""

    # Create log directory
    os.makedirs(LOG_DIR, exist_ok=True)

    # Create logger
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(
        LOG_DIR, f"percentage_sampling_{platform}_{timestamp}.log"
    )

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),  # Also print to console
        ],
    )

    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Starting percentage-based language sampling")
    logger.info(f"📝 Logging to: {log_filename}")
    logger.info(f"🎯 Target languages: {LANG_FILTER}")
    logger.info(f"📊 Sample percentage: {SAMPLE_PERCENTAGE}%")

    return logger


def _client_and_db():
    """Connect to MongoDB."""
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    from pymongo import MongoClient

    client = MongoClient(host, port)
    return client, client[name]


def get_all_target_language_actors(
    db, platform: str, target_languages: List[str], logger: logging.Logger
) -> List[str]:
    """
    Get ALL actors who have posts in target languages for a platform.
    No sampling at actor level - we'll sample at post level instead.
    """
    logger.info(f"🔍 Finding ALL {platform} actors with {target_languages} posts...")

    start_time = time.time()

    try:
        # Query actor_metric collection for ALL actors with target languages
        actor_query = {"platform": platform, "lang": {"$in": target_languages}}

        # Optional: filter by minimum activity level (very low threshold)
        if MIN_POSTS_PER_ACTOR > 1:
            actor_query["n_posts"] = {"$gte": MIN_POSTS_PER_ACTOR}

        # Get ALL matching actors (no limit)
        target_actors = list(
            db.actor_metric.find(
                actor_query, {"actor_username": 1, "lang": 1, "n_posts": 1, "_id": 0}
            ).sort("n_posts", -1)  # Most active first for logging
        )

        end_time = time.time()

        actor_usernames = [
            actor["actor_username"]
            for actor in target_actors
            if actor.get("actor_username")
        ]

        logger.info(
            f"✅ Found {len(actor_usernames)} target language actors in {end_time - start_time:.1f}s"
        )

        if target_actors:
            # Show language distribution
            lang_counts = {}
            total_posts = 0
            for actor in target_actors:
                lang = actor.get("lang", "unknown")
                posts = actor.get("n_posts", 0)
                lang_counts[lang] = lang_counts.get(lang, 0) + 1
                total_posts += posts

            logger.info(f"📊 Language distribution: {lang_counts}")
            logger.info(f"📈 Total posts from these actors: {total_posts:,}")
            logger.info(
                f"📊 Expected sample size: {int(total_posts * SAMPLE_PERCENTAGE / 100):,} posts"
            )

            # Show top actors
            top_actors = sorted(
                target_actors, key=lambda x: x.get("n_posts", 0), reverse=True
            )[:3]
            logger.info(f"🏆 Top actors:")
            for actor in top_actors:
                logger.info(
                    f"   - {actor['actor_username']} ({actor['lang']}, {actor.get('n_posts', 0):,} posts)"
                )

        return actor_usernames

    except Exception as e:
        logger.error(f"❌ Error finding target language actors: {e}")
        return []


def collect_and_sample_posts(
    db,
    actor_usernames: List[str],
    platform: str,
    sample_percentage: float,
    logger: logging.Logger,
) -> List:
    """
    Collect ALL post IDs from target language actors, then randomly sample the specified percentage.
    This ensures temporal diversity and representativeness.
    """
    if not actor_usernames:
        return []

    logger.info(
        f"📝 Collecting posts from {len(actor_usernames)} actors for sampling..."
    )

    all_post_ids = []

    # Process actors in batches to avoid huge queries
    total_batches = (len(actor_usernames) + ACTOR_BATCH_SIZE - 1) // ACTOR_BATCH_SIZE

    for i in range(0, len(actor_usernames), ACTOR_BATCH_SIZE):
        batch_actors = actor_usernames[i : i + ACTOR_BATCH_SIZE]
        batch_num = (i // ACTOR_BATCH_SIZE) + 1

        logger.info(
            f"📦 Processing actor batch {batch_num}/{total_batches} ({len(batch_actors)} actors)..."
        )

        # Query for posts from this batch of actors
        post_query = {
            "platform": platform,
            "$or": [
                {"author.username": {"$in": batch_actors}},
                {"author": {"$in": batch_actors}},
            ],
        }

        try:
            # Get ALL post IDs from these actors (no limit at this stage)
            post_cursor = db.post.find(post_query, {"_id": 1})
            batch_post_ids = [post["_id"] for post in post_cursor]
            all_post_ids.extend(batch_post_ids)

            logger.info(
                f"   ✅ +{len(batch_post_ids):,} posts (total: {len(all_post_ids):,})"
            )

        except Exception as e:
            logger.error(f"   ❌ Error in batch {batch_num}: {e}")
            continue

    logger.info(f"📊 Total posts collected: {len(all_post_ids):,}")

    # SAMPLING STAGE: Randomly sample the specified percentage
    if len(all_post_ids) > 0:
        target_sample_size = int(len(all_post_ids) * sample_percentage / 100)

        if target_sample_size < len(all_post_ids):
            logger.info(
                f"🎲 Randomly sampling {target_sample_size:,} posts ({sample_percentage}%) from {len(all_post_ids):,} total posts..."
            )

            random.Random(RANDOM_SEED).shuffle(all_post_ids)  # Shuffle for randomness
            sampled_post_ids = all_post_ids[:target_sample_size]

            logger.info(f"✅ Sampled {len(sampled_post_ids):,} posts for processing")
            return sampled_post_ids
        else:
            logger.info(
                f"📋 Keeping all {len(all_post_ids):,} posts (sample size >= total)"
            )
            return all_post_ids
    else:
        logger.warning(f"⚠️ No posts found for sampling")
        return []


def _rows_from_posts(
    posts: List[Dict[str, Any]], logger: logging.Logger
) -> Tuple[list, int]:
    """Transform posts to CSV rows."""
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

            # Platform, message_id, datetime
            platform = post.get("platform")
            message_id = post.get("message_id") or post.get("_id")
            datetime_val = post.get("datetime")

            if not all([platform, message_id, datetime_val]):
                skipped += 1
                continue

            req_values.update(
                {
                    "platform": platform,
                    "message_id": str(message_id),
                    "datetime": datetime_val,
                }
            )

            # Optional fields
            opt_values = {}
            if isinstance(author, dict):
                opt_values["actor_id"] = author.get("id")
                opt_values["actor_name"] = author.get("name") or author.get(
                    "display_name"
                )
                opt_values["link_to_actor"] = author.get("url")

            opt_values.update(
                {"lang": post.get("lang"), "post_url": post.get("post_url")}
            )

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

    if skipped > 0:
        logger.warning(f"⚠️ Skipped {skipped} posts due to missing required fields")

    return rows, skipped


def _fetch_and_transform_batch(db, batch_ids: List, logger: logging.Logger):
    """Fetch posts by IDs and transform to rows."""
    if not batch_ids:
        return [], 0

    query = {"_id": {"$in": batch_ids}}
    docs = list(db.post.find(query, POST_PROJECTION))
    return _rows_from_posts(docs, logger)


def process_sampled_posts(
    db, sampled_post_ids: List, platform: str, logger: logging.Logger
) -> pd.DataFrame:
    """Process the sampled posts into final dataset."""

    if not sampled_post_ids:
        logger.warning(f"⚠️ No posts to process for {platform}")
        return pd.DataFrame(columns=DERIVED_COLUMNS)

    logger.info(f"🔄 Processing {len(sampled_post_ids):,} sampled posts...")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    rows_all = []
    skipped_total = 0

    # Process in batches
    batches = [
        sampled_post_ids[i : i + POST_BATCH_SIZE]
        for i in range(0, len(sampled_post_ids), POST_BATCH_SIZE)
    ]

    logger.info(f"📊 Processing {len(batches)} batches of posts...")

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = [
            ex.submit(_fetch_and_transform_batch, db, batch, logger)
            for batch in batches
        ]

        for i, fut in enumerate(tqdm(futures, desc=f"Processing {platform} posts")):
            rows, skipped = fut.result()
            skipped_total += skipped
            rows_all.extend(rows)

            # Save intermediate chunks
            if CHUNK_SAVE_SIZE and len(rows_all) >= CHUNK_SAVE_SIZE:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                lang_suffix = "_".join(LANG_FILTER)
                chunk_filename = os.path.join(
                    OUTPUT_DIR,
                    f"{platform}_percentage_sample_{SAMPLE_PERCENTAGE}pct_{lang_suffix}_chunk_{len(rows_all)}_{timestamp}.csv",
                )
                pd.DataFrame(rows_all).to_csv(chunk_filename, index=False)
                logger.info(
                    f"💾 Autosaved chunk: {len(rows_all):,} rows → {chunk_filename}"
                )
                rows_all = []  # Reset for next chunk

            # Progress logging
            if (i + 1) % 10 == 0:
                total_processed = (i + 1) * POST_BATCH_SIZE
                logger.info(
                    f"📈 Progress: {total_processed:,}/{len(sampled_post_ids):,} posts processed"
                )

    # Create final dataframe
    df_final = (
        pd.DataFrame(rows_all) if rows_all else pd.DataFrame(columns=DERIVED_COLUMNS)
    )

    logger.info(f"✅ Final processing complete:")
    logger.info(f"   📊 Posts processed: {len(sampled_post_ids):,}")
    logger.info(f"   📋 Rows created: {len(df_final):,}")
    logger.info(f"   ⚠️ Rows skipped: {skipped_total:,}")

    return df_final


def sample_one_platform_percentage(
    platform: str,
    sample_percentage: float,
    target_languages: List[str],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Sample a percentage of posts from ALL target language actors on one platform.
    """
    logger.info(f"\n🚀 STARTING PERCENTAGE SAMPLING FOR {platform.upper()}")
    logger.info(
        f"🎯 Target: {sample_percentage}% of posts from {target_languages} actors"
    )

    start_time = time.time()

    # Connect to database
    client, db = _client_and_db()

    try:
        # Step 1: Get ALL target language actors
        target_actors = get_all_target_language_actors(
            db, platform, target_languages, logger
        )

        if not target_actors:
            logger.warning(f"❌ No target language actors found for {platform}")
            return pd.DataFrame(columns=DERIVED_COLUMNS)

        # Step 2: Collect and sample posts
        sampled_post_ids = collect_and_sample_posts(
            db, target_actors, platform, sample_percentage, logger
        )

        if not sampled_post_ids:
            logger.warning(f"❌ No posts sampled for {platform}")
            return pd.DataFrame(columns=DERIVED_COLUMNS)

        # Step 3: Process sampled posts
        df_result = process_sampled_posts(db, sampled_post_ids, platform, logger)

        # Save final results
        if len(df_result) > 0:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            lang_suffix = "_".join(target_languages)
            final_filename = os.path.join(
                OUTPUT_DIR,
                f"{platform}_percentage_sample_{sample_percentage}pct_{lang_suffix}_{len(df_result)}_{timestamp}.csv",
            )
            df_result.to_csv(final_filename, index=False)
            logger.info(f"💾 Final results saved: {final_filename}")

        elapsed = time.time() - start_time
        logger.info(f"⏱️ Platform {platform} completed in {elapsed:.1f}s")

        return df_result

    except Exception as e:
        logger.error(f"❌ Error processing {platform}: {e}")
        return pd.DataFrame(columns=DERIVED_COLUMNS)

    finally:
        client.close()


def test_single_platform():
    """Test the percentage sampling on a single platform."""

    # Setup logging
    logger = setup_logging("telegram_test")

    logger.info("🧪 TESTING PERCENTAGE SAMPLING ON SINGLE PLATFORM")
    logger.info("=" * 60)

    # Test on telegram first
    test_platform = "telegram"

    result_df = sample_one_platform_percentage(
        platform=test_platform,
        sample_percentage=SAMPLE_PERCENTAGE,
        target_languages=LANG_FILTER,
        logger=logger,
    )

    logger.info(f"\n📊 TEST RESULTS SUMMARY:")
    logger.info(f"   Platform: {test_platform}")
    logger.info(f"   Sample percentage: {SAMPLE_PERCENTAGE}%")
    logger.info(f"   Target languages: {LANG_FILTER}")
    logger.info(f"   Final dataset size: {len(result_df):,} rows")

    if len(result_df) > 0:
        # Show language distribution in final sample
        lang_dist = result_df["lang"].value_counts()
        logger.info(f"   Language distribution in sample:")
        for lang, count in lang_dist.items():
            logger.info(f"     {lang}: {count:,} posts")

        # Show temporal range
        if "datetime" in result_df.columns:
            try:
                result_df["datetime"] = pd.to_datetime(result_df["datetime"])
                date_range = (
                    f"{result_df['datetime'].min()} to {result_df['datetime'].max()}"
                )
                logger.info(f"   Temporal range: {date_range}")
            except:
                logger.info(f"   Temporal range: Could not parse dates")

    logger.info("✅ Single platform test completed!")

    return result_df


if __name__ == "__main__":
    # Run test on single platform first
    test_result = test_single_platform()

    print(f"\n🎉 Test completed! Check logs directory for detailed information.")
    print(f"📁 Results saved to: {OUTPUT_DIR}")
    print(f"📝 Logs saved to: {LOG_DIR}")
