#!/usr/bin/env python3
"""
Single Actor Test - Fast approach to understand Instagram data structure
"""

import logging
from spreadAnalysis.persistence.mongo import MongoSpread

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    logger.info("🎯 Single Instagram actor test - FAST approach")
    
    client = MongoSpread().client
    db = client.spreadAnalysis
    
    # We know 'kilezmore' exists as Instagram actor from our previous tests
    target_actor = "kilezmore"
    
    logger.info(f"🔍 Looking for posts from single actor: {target_actor}")
    
    # Test different query formats to find the right one
    test_queries = [
        {"platform": "instagram", "author": target_actor},
        {"platform": "instagram", "author.username": target_actor},
        {"platform": "instagram", "author.actor_username": target_actor},
        {"platform": "instagram", "author.name": target_actor},
        {"platform": "instagram", "author.user": target_actor},
    ]
    
    found_posts = []
    working_query = None
    
    for i, query in enumerate(test_queries, 1):
        try:
            logger.info(f"   Testing query {i}: {query}")
            posts = list(db.post.find(query).limit(3))  # Just 3 posts max
            
            if posts:
                logger.info(f"   ✅ Query {i} WORKS! Found {len(posts)} posts")
                found_posts = posts
                working_query = query
                break
            else:
                logger.info(f"   ❌ Query {i} found 0 posts")
                
        except Exception as e:
            logger.info(f"   ❌ Query {i} failed: {e}")
    
    if found_posts:
        logger.info(f"🎉 SUCCESS! Working query: {working_query}")
        logger.info("📖 Post structure from this actor:")
        
        for i, post in enumerate(found_posts, 1):
            logger.info(f"   Post {i}:")
            logger.info(f"     _id: {post.get('_id')}")
            logger.info(f"     platform: {post.get('platform')}")
            
            author = post.get("author")
            logger.info(f"     author type: {type(author)}")
            logger.info(f"     author value: {author}")
            
            if isinstance(author, dict):
                logger.info("     author fields:")
                for key, value in author.items():
                    logger.info(f"       {key}: {value}")
    
        # Now let's create a simple sampling approach
        logger.info("🚀 Creating simple sampling for Instagram...")
        
        # Use the working query to get more posts from this actor
        all_posts_from_actor = list(db.post.find(working_query, {"_id": 1}).limit(100))
        logger.info(f"📦 Found {len(all_posts_from_actor)} posts from {target_actor}")
        
        if all_posts_from_actor:
            # Sample 10%
            import random
            sample_size = max(1, len(all_posts_from_actor) // 10)
            sampled = random.sample(all_posts_from_actor, sample_size)
            
            logger.info(f"🎲 Sampled {len(sampled)} posts (10%)")
            
            # Save sample
            import os
            import datetime
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            os.makedirs('./data/percentage_sampled', exist_ok=True)
            
            sample_file = f'./data/percentage_sampled/single_actor_test_{len(sampled)}posts_{timestamp}.txt'
            with open(sample_file, 'w') as f:
                for post in sampled:
                    f.write(f"{post['_id']}\n")
            
            logger.info(f"💾 Saved to: {sample_file}")
            logger.info("🎉 SINGLE ACTOR TEST SUCCESS!")
            
    else:
        logger.error("❌ No posts found with any query format!")
        logger.info("💡 Let's try a different approach - get ANY Instagram post")
        
        # Get just ONE Instagram post to see structure
        any_post = db.post.find_one({"platform": "instagram"})
        if any_post:
            logger.info("📖 Structure of ANY Instagram post:")
            logger.info(f"   _id: {any_post.get('_id')}")
            author = any_post.get("author")
            logger.info(f"   author: {type(author)} = {author}")
            if isinstance(author, dict):
                for key, value in author.items():
                    logger.info(f"     {key}: {value}")
    
except Exception as e:
    logger.error(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
