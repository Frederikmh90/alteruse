#!/usr/bin/env python3
"""
Minimal Instagram Debug - Find the actual issue
"""

import logging
from spreadAnalysis.persistence.mongo import MongoSpread

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    logger.info("🔍 Minimal Instagram debug starting...")

    client = MongoSpread().client
    db = client.spreadAnalysis

    # 1. Basic stats - but avoid count_documents which times out
    logger.info("📊 Getting Instagram sample instead of count...")

    # Use find with limit instead of count
    sample_posts = list(db.post.find({"platform": "instagram"}).limit(10))
    logger.info(f"   Found at least {len(sample_posts)} Instagram posts")

    # 2. Examine the first few posts to understand structure
    if sample_posts:
        logger.info("📖 Examining Instagram post structures:")

        for i, post in enumerate(sample_posts[:3]):
            logger.info(f"   Post {i + 1}:")
            logger.info(f"     _id: {post.get('_id')}")
            logger.info(f"     platform: {post.get('platform')}")

            author = post.get("author")
            logger.info(f"     author type: {type(author)}")
            logger.info(f"     author value: {author}")

            if isinstance(author, dict):
                logger.info(f"     author keys: {list(author.keys())}")
                for key in ["username", "actor_username", "name", "id"]:
                    if key in author:
                        logger.info(f"       {key}: {author[key]}")

    # 3. Try to find posts from our known actor 'kilezmore'
    logger.info("🎯 Testing queries for 'kilezmore'...")

    test_queries = [
        {"platform": "instagram", "author": "kilezmore"},
        {"platform": "instagram", "author.username": "kilezmore"},
        {"platform": "instagram", "author.actor_username": "kilezmore"},
        {"platform": "instagram", "author.name": "kilezmore"},
    ]

    for i, query in enumerate(test_queries, 1):
        try:
            # Use find with limit instead of count_documents
            results = list(db.post.find(query).limit(1))
            logger.info(f"   Query {i}: {len(results)} results found")
            if results:
                logger.info(f"     Found post: {results[0].get('_id')}")
                logger.info(f"     Author: {results[0].get('author')}")
        except Exception as e:
            logger.info(f"   Query {i} failed: {e}")

    # 4. Try MongoDB $sample for efficient random sampling
    logger.info("🎲 Testing MongoDB $sample aggregation...")
    try:
        pipeline = [
            {"$match": {"platform": "instagram"}},
            {"$sample": {"size": 5}},
            {"$project": {"_id": 1, "author": 1}},
        ]

        sampled = list(db.post.aggregate(pipeline))
        logger.info(f"   $sample returned {len(sampled)} posts")

        for i, post in enumerate(sampled):
            author = post.get("author")
            logger.info(f"     Sample {i + 1}: {type(author)} = {author}")

    except Exception as e:
        logger.info(f"   $sample failed: {e}")

    logger.info("🎉 Debug complete!")

except Exception as e:
    logger.error(f"❌ Debug failed: {e}")
    import traceback

    traceback.print_exc()
