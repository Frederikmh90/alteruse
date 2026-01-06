#!/usr/bin/env python3
"""
Fixed Language Collection Functions

This file contains the corrected functions that filter by language
at the MongoDB level instead of in Python.
"""

import random
from typing import List, Any, Set
from tqdm.auto import tqdm


def _collect_actor_post_ids_with_language_filter(
    db,
    actor_username: str,
    platform: str,
    min_posts_per_actor: int,
    posts_per_actor_soft: int,
    post_percentage: float,
    max_posts_per_actor: int | None,
    target_languages: List[str] = None,
    random_seed: int = 42,
):
    """
    Enhanced version that filters by language at the MongoDB level.
    Only collects post IDs for posts that match target languages.
    """

    # Get the actor's post IDs (same as before)
    app = db.actor_platform_post.find_one(
        {"actor_username": actor_username, "platform": platform},
        {"post_obj_ids": {"$slice": posts_per_actor_soft}},
    )
    ids = (app or {}).get("post_obj_ids") or []

    if len(ids) < min_posts_per_actor:
        return []

    # NEW: If we have target languages, filter the post IDs by language
    if target_languages:
        # Query the actual posts to see which ones match our language criteria
        language_query = {"_id": {"$in": ids}, "lang": {"$in": target_languages}}

        # Get only the IDs of posts that match language criteria
        matching_posts = list(db.post.find(language_query, {"_id": 1}))

        # Extract just the IDs
        ids = [post["_id"] for post in matching_posts]

        # Check if we still have enough posts after language filtering
        if len(ids) < min_posts_per_actor:
            return []

    # Apply sampling logic (same as before)
    if post_percentage < 100.0:
        k = max(1, int(len(ids) * (post_percentage / 100.0)))
    else:
        k = len(ids)

    if max_posts_per_actor:
        k = min(k, max_posts_per_actor)

    if k >= len(ids):
        return ids

    return random.Random(random_seed).sample(ids, k)


def _fetch_post_docs_by_ids_with_language_filter(
    db, post_ids: List[Any], target_languages: List[str] = None, projection: dict = None
):
    """
    Enhanced version that adds language filtering at the MongoDB query level.
    """
    if not post_ids:
        return []

    # Base query
    query = {"_id": {"$in": post_ids}}

    # Add language filter if specified
    if target_languages:
        query["lang"] = {"$in": target_languages}

    # Use provided projection or default
    if projection is None:
        # Use the same projection as in the original code
        projection = {
            "_id": 1,
            "method": 1,
            "platform": 1,
            "text": 1,
            "lang": 1,
            # Add other fields as needed
            "id": 1,
            "source": 1,
            "conversation_id": 1,
            "author": 1,
            "entities": 1,
            "public_metrics": 1,
            "attachments": 1,
            "referenced_tweets": 1,
            "created_at": 1,
            "account": 1,
            "post_url": 1,
            "postUrl": 1,
            "platformId": 1,
            "type_id": 1,
            "name": 1,
            "post_date": 1,
            "statistics": 1,
            "date": 1,
            "message": 1,
            "title": 1,
            "caption": 1,
            "description": 1,
            "user_id": 1,
            "owner_id": 1,
            "post_id": 1,
            "likes": 1,
            "comments": 1,
            "reposts": 1,
            "author_fullname": 1,
            "created_utc": 1,
            "selftext": 1,
            "body": 1,
            "num_comments": 1,
            "num_crossposts": 1,
            "score": 1,
            "permalink": 1,
            "full_link": 1,
            "from_username": 1,
            "peer_id": 1,
            "media": 1,
            "views": 1,
            "forwards": 1,
            "replies": 1,
            "snippet": 1,
            "createTime": 1,
            "desc": 1,
            "video": 1,
            "stats": 1,
            "url": 1,
            "followers_count": 1,
            "num": 1,
            "thread_num": 1,
            "comment": 1,
            "timestamp": 1,
            "op": 1,
            "displayLink": 1,
            "link": 1,
            "pagemap": 1,
        }

    return list(db.post.find(query, projection))


def _pick_accounts_with_language_posts(
    db,
    platform: str,
    target_n: int,
    target_languages: List[str] = None,
    min_posts_per_actor: int = 30,
    random_seed: int = 42,
) -> List[str]:
    """
    Enhanced version that prioritizes actors who have posts in target languages.
    """

    if not target_languages:
        # Fall back to original method if no language filtering
        return _pick_accounts_actor_metric_original(db, platform, target_n, random_seed)

    try:
        # Build aggregation pipeline to find actors with language-filtered posts
        pipeline = [
            # Match posts for this platform with target languages
            {"$match": {"platform": platform, "lang": {"$in": target_languages}}},
            # Group by actor to count their posts
            {
                "$group": {
                    "_id": "$author.username",  # Try different author field paths
                    "post_count": {"$sum": 1},
                }
            },
            # Filter actors with enough posts
            {
                "$match": {
                    "_id": {"$ne": None},
                    "post_count": {"$gte": min_posts_per_actor},
                }
            },
            # Sample the actors
            {"$sample": {"size": target_n * 3}},  # Get more than needed
            # Project just the username
            {"$project": {"_id": 1}},
        ]

        # Try the aggregation
        result = list(db.post.aggregate(pipeline, allowDiskUse=True))
        usernames = [doc["_id"] for doc in result if doc["_id"]]

        if usernames:
            # Deduplicate and limit
            seen, out = set(), []
            for u in usernames:
                if u not in seen:
                    seen.add(u)
                    out.append(u)

            return out[:target_n]

    except Exception as e:
        print(f"⚠️  Language-based actor selection failed: {e}")
        print("   Falling back to original method...")

    # Fallback to original method
    return _pick_accounts_actor_metric_original(db, platform, target_n, random_seed)


def _pick_accounts_actor_metric_original(
    db, platform: str, target_n: int, random_seed: int = 42
) -> List[str]:
    """
    Original account picking method (unchanged).
    """
    # sample via $sample
    try:
        docs = list(
            db.actor_metric.aggregate(
                [
                    {"$match": {"platform": platform, "actor_username": {"$ne": None}}},
                    {"$sample": {"size": target_n}},
                    {"$project": {"_id": 0, "actor_username": 1}},
                ],
                allowDiskUse=True,
            )
        )
        usernames = [d["actor_username"] for d in docs if d.get("actor_username")]
        seen, out = set(), []
        for u in usernames:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out
    except Exception:
        pass

    # fallback: window+skip
    rng = random.Random(random_seed)
    usernames, need, window = [], target_n, 2000
    while need > 0:
        skip = rng.randint(0, max(0, window - 1))
        cursor = (
            db.actor_metric.find(
                {"platform": platform, "actor_username": {"$ne": None}},
                {"_id": 0, "actor_username": 1},
            )
            .skip(skip)
            .limit(min(need, 2000))
        )
        batch = [d["actor_username"] for d in cursor if d.get("actor_username")]
        if not batch:
            break
        usernames.extend(batch)
        need -= len(batch)
    seen, out = set(), []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:target_n]


# Helper function to parse language filter config
def parse_language_filter(lang_filter):
    """
    Convert language filter config to list format for MongoDB queries.
    """
    if lang_filter == "all":
        return None

    if isinstance(lang_filter, str) and lang_filter != "all":
        return [lang_filter.lower()]

    if isinstance(lang_filter, set):
        return [x.lower() for x in lang_filter]

    if isinstance(lang_filter, list):
        return [x.lower() for x in lang_filter]

    return None


def enhanced_sample_one_platform_with_language_filter(
    db,
    platform: str,
    account_percentage: float,
    post_percentage: float,
    min_posts_per_actor: int,
    max_posts_per_actor: int | None,
    posts_per_actor_soft: int,
    lang_filter,  # Can be "all", string, set, or list
    batch_fetch_size: int,
    output_dir: str,
    max_actors_per_platform: int = None,
    random_seed: int = 42,
):
    """
    Enhanced platform sampling that filters by language at MongoDB level.
    """
    import os
    import time
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor, as_completed

    os.makedirs(output_dir, exist_ok=True)
    t0 = time.time()
    print(f"\n=== {platform.upper()} (with language filter) ===")

    # Parse language filter
    target_languages = parse_language_filter(lang_filter)
    if target_languages:
        print(f"Target languages: {target_languages}")
    else:
        print("No language filtering (collecting all languages)")

    # --- account selection with language awareness ---
    t_sel0 = time.time()
    try:
        total_actors = db.actor_metric.count_documents(
            {"platform": platform, "actor_username": {"$ne": None}}
        )
    except Exception:
        total_actors = 1000

    target_accounts = max(1000, int(total_actors * (account_percentage / 100.0)))
    if max_actors_per_platform is not None:
        target_accounts = min(target_accounts, max_actors_per_platform)

    # Use language-aware account selection if we have target languages
    if target_languages:
        accounts = _pick_accounts_with_language_posts(
            db,
            platform,
            target_accounts,
            target_languages,
            min_posts_per_actor,
            random_seed,
        )
    else:
        accounts = _pick_accounts_actor_metric_original(
            db, platform, target_accounts, random_seed
        )

    t_sel1 = time.time()
    print(f"[{platform}] picked {len(accounts)} actors in {t_sel1 - t_sel0:.1f}s")

    if not accounts:
        print(f"[{platform}] no actors found")
        return pd.DataFrame(), {
            "actors": 0,
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": 0.0,
        }

    # --- collect language-filtered ids ---
    t_ids0 = time.time()
    all_ids = []
    actors_with_posts = 0

    for a in tqdm(accounts, desc=f"[{platform}] collecting language-filtered post ids"):
        ids = _collect_actor_post_ids_with_language_filter(
            db,
            a,
            platform=platform,
            min_posts_per_actor=min_posts_per_actor,
            posts_per_actor_soft=posts_per_actor_soft,
            post_percentage=post_percentage,
            max_posts_per_actor=max_posts_per_actor,
            target_languages=target_languages,
            random_seed=random_seed,
        )
        if ids:
            all_ids.extend(ids)
            actors_with_posts += 1

    all_ids = list(dict.fromkeys(all_ids))  # Remove duplicates
    t_ids1 = time.time()

    print(
        f"[{platform}] actors with language-filtered posts: {actors_with_posts}/{len(accounts)}"
    )
    print(
        f"[{platform}] unique language-filtered post ids: {len(all_ids)} (collected in {t_ids1 - t_ids0:.1f}s)"
    )

    if not all_ids:
        print(f"[{platform}] no posts found with target languages")
        return pd.DataFrame(), {
            "actors": len(accounts),
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": time.time() - t0,
        }

    # Note: We can remove the Python-level language filtering from _rows_from_posts
    # since we've already filtered at the MongoDB level

    print(f"✅ Language filtering applied at MongoDB level - much more efficient!")

    # Return placeholder stats - you would integrate this with your existing processing
    stats = {
        "actors": len(accounts),
        "actors_with_posts": actors_with_posts,
        "ids": len(all_ids),
        "rows": 0,  # Would be filled by actual processing
        "skipped": 0,
        "elapsed": time.time() - t0,
    }

    return pd.DataFrame(), stats  # Placeholder return


if __name__ == "__main__":
    print("🔧 Language Collection Fix Functions")
    print("These functions should be integrated into your main script.")
    print("Key improvements:")
    print("1. Language filtering at MongoDB level (much faster)")
    print("2. Actor selection prioritizes those with target language posts")
    print("3. Reduces network traffic and processing time")
    print("4. Works better on VMs with limited resources")
