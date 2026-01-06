#!/usr/bin/env python3
"""
FIXED: Multi-platform percentage sampling with platform-specific language handling
Based on Spread class language field patterns
"""

import os
import time
import random
import logging
import datetime
from collections import defaultdict, Counter
import pandas as pd
from tqdm import tqdm

from spreadAnalysis.persistence.mongo import MongoSpread

# =========================
# CONFIG — ALL PLATFORMS + TARGET LANGUAGES (FIXED)
# =========================
PLATFORMS = "auto"  # All platforms
LANG_FILTER = {"da", "de", "sv"}  # Danish, German, Swedish ONLY
ACCOUNT_PERCENTAGE = 10  # 10% of actors per platform
POST_PERCENTAGE = 5  # 5% of posts per actor
MIN_POSTS_PER_ACTOR = 30  # Minimum posts per actor
MAX_POSTS_PER_ACTOR = 30  # Maximum posts per actor
POSTS_PER_ACTOR_SOFT = 400

MAX_ACTORS_PER_PLATFORM = 100000

BATCH_FETCH_SIZE = 2000
FETCH_WORKERS = 8
CHUNK_SAVE_SIZE = 10_000

OUTPUT_DIR = "./data/all_platforms_fixed"
RANDOM_SEED = 42

# Setup logging
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs("./logs", exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

log_file = f"./logs/all_platforms_fixed_{timestamp}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Initialize MongoDB
mdb = MongoSpread()
db = mdb.database
random.seed(RANDOM_SEED)


def get_platform_language_query(platform, lang_filter):
    """
    Get platform-specific language query based on Spread class patterns
    """
    if platform == "gab":
        # Gab uses 'language' field according to Spread class
        return {"language": {"$in": list(lang_filter)}}
    elif platform in ["twitter", "instagram", "facebook"]:
        # These platforms use 'lang' field directly
        return {"lang": {"$in": list(lang_filter)}}
    else:
        # For other platforms, try both fields with $or
        return {
            "$or": [
                {"lang": {"$in": list(lang_filter)}},
                {"language": {"$in": list(lang_filter)}},
            ]
        }


def extract_language_from_post(post, platform):
    """
    Extract language from post based on platform-specific logic
    """
    if platform == "gab":
        return post.get("language") or post.get("lang")
    elif platform in ["twitter", "instagram", "facebook"]:
        return post.get("lang")
    else:
        return post.get("lang") or post.get("language")


def get_available_platforms():
    """Get all platforms with posts"""
    if PLATFORMS == "auto":
        # Use a more efficient query
        platforms = [
            "instagram",
            "facebook",
            "twitter",
            "gab",
            "reddit",
            "telegram",
            "youtube",
            "vkontakte",
            "fourchan",
            "tiktok",
        ]
        logger.info(f"🌍 Using predefined platforms: {platforms}")
        return platforms
    else:
        return [PLATFORMS]


def get_platform_actors(platform):
    """Get all actors for a platform"""
    try:
        actors = list(
            db.post.aggregate(
                [
                    {"$match": {"platform": platform}},
                    {"$group": {"_id": "$author", "post_count": {"$sum": 1}}},
                    {"$match": {"post_count": {"$gte": MIN_POSTS_PER_ACTOR}}},
                    {"$project": {"actor": "$_id", "post_count": 1, "_id": 0}},
                ]
            )
        )

        return [a["actor"] for a in actors]
    except Exception as e:
        logger.error(f"❌ Failed to get {platform} actors: {e}")
        return []


def sample_actors(actors, percentage):
    """Sample percentage of actors"""
    target_count = max(1, int(len(actors) * percentage / 100))
    target_count = min(target_count, MAX_ACTORS_PER_PLATFORM)

    sampled = random.sample(actors, min(target_count, len(actors)))
    return sampled


def collect_actor_post_ids(platform, actors):
    """Collect post IDs from actors with progress bar"""
    all_post_ids = []

    with tqdm(total=len(actors), desc=f"[{platform}] Collecting post IDs") as pbar:
        for actor in actors:
            try:
                # Base query for this actor
                base_query = {"platform": platform, "author": actor}

                post_ids = list(
                    db.post.find(base_query, {"_id": 1}).limit(MAX_POSTS_PER_ACTOR)
                )

                if post_ids:
                    # Sample percentage of posts from this actor
                    target_posts = max(1, int(len(post_ids) * POST_PERCENTAGE / 100))
                    sampled_ids = random.sample(
                        post_ids, min(target_posts, len(post_ids))
                    )
                    all_post_ids.extend([p["_id"] for p in sampled_ids])

            except Exception as e:
                logger.warning(f"⚠️ Error collecting posts from {actor}: {e}")

            pbar.update(1)

    return all_post_ids


def fetch_and_filter_posts(platform, post_ids):
    """Fetch posts and apply platform-specific language filtering"""

    # Get platform-specific language query
    lang_query = get_platform_language_query(platform, LANG_FILTER)

    target_posts = []
    filtered_count = 0

    # Process in batches
    batches = [
        post_ids[i : i + BATCH_FETCH_SIZE]
        for i in range(0, len(post_ids), BATCH_FETCH_SIZE)
    ]

    with tqdm(total=len(batches), desc=f"[{platform}] Processing posts") as pbar:
        for batch_ids in batches:
            try:
                # Query with platform-specific language filter
                query = {
                    "_id": {"$in": batch_ids},
                    "platform": platform,
                    **lang_query,  # Add platform-specific language filter
                }

                posts = list(db.post.find(query))

                for post in posts:
                    # Extract language using platform-specific logic
                    post_lang = extract_language_from_post(post, platform)

                    if post_lang in LANG_FILTER:
                        target_posts.append(
                            {
                                "platform": platform,
                                "author": post.get("author"),
                                "lang": post_lang,
                                "message": post.get("message", ""),
                                "date": post.get("date"),
                                "post_id": str(post.get("_id")),
                                "method": post.get("method"),
                            }
                        )
                    else:
                        filtered_count += 1

            except Exception as e:
                logger.warning(f"⚠️ Error processing batch: {e}")

            pbar.update(1)

    return target_posts, filtered_count


def process_platform(platform):
    """Process one platform completely"""
    start_time = time.time()

    logger.info(f"\n=== {platform.upper()} ===")

    try:
        # Get all actors
        all_actors = get_platform_actors(platform)
        if not all_actors:
            logger.warning(f"📊 No actors found for {platform}")
            return

        logger.info(f"📊 Total {platform} actors: {len(all_actors):,}")

        # Sample actors
        target_actors = sample_actors(all_actors, ACCOUNT_PERCENTAGE)
        logger.info(f"🎯 Sampling {len(target_actors)} actors ({ACCOUNT_PERCENTAGE}%)")
        logger.info(f"✅ Selected {len(target_actors)} actors")

        # Collect post IDs
        post_ids = collect_actor_post_ids(platform, target_actors)
        if not post_ids:
            logger.warning(f"📦 No post IDs collected for {platform}")
            return

        logger.info(f"📦 Total post IDs: {len(post_ids):,}")

        # Fetch and filter posts
        target_posts, filtered_count = fetch_and_filter_posts(platform, post_ids)

        logger.info(
            f"📈 {platform}: {len(target_posts)} target language posts, {filtered_count} filtered out"
        )

        if target_posts:
            # Save results
            df = pd.DataFrame(target_posts)

            # Language distribution
            lang_dist = df["lang"].value_counts().to_dict()
            logger.info(f"📊 {platform} language distribution: {lang_dist}")

            # Save to file
            output_file = (
                f"{OUTPUT_DIR}/{platform}_fixed_{len(target_posts)}_{timestamp}.csv"
            )
            df.to_csv(output_file, index=False)
            logger.info(
                f"💾 Saved {platform}: {len(target_posts)} posts → {output_file}"
            )

        elapsed = time.time() - start_time
        logger.info(f"⏱️  {platform} completed in {elapsed:.1f}s")

    except Exception as e:
        logger.error(f"❌ Failed to process {platform}: {e}")


def main():
    logger.info("🚀 MULTI-PLATFORM PERCENTAGE SAMPLING (FIXED FOR LANGUAGE FIELDS)")
    logger.info("=" * 70)
    logger.info(f"🎯 Target languages: {sorted(LANG_FILTER)}")
    logger.info(f"📊 Account sampling: {ACCOUNT_PERCENTAGE}% per platform")
    logger.info(f"📊 Post sampling: {POST_PERCENTAGE}% per actor")
    logger.info("🔧 Platform-specific language field handling:")
    logger.info("   - Gab: uses 'language' field")
    logger.info("   - Instagram/Twitter/Facebook: use 'lang' field")
    logger.info("   - Others: try both 'lang' and 'language' fields")
    logger.info("=" * 70)

    platforms = get_available_platforms()

    for platform in platforms:
        process_platform(platform)

    logger.info("\n🎉 ALL PLATFORMS SAMPLING COMPLETED!")
    logger.info(f"📁 Results saved to: {OUTPUT_DIR}")
    logger.info(f"📋 Log file: {log_file}")


if __name__ == "__main__":
    main()
