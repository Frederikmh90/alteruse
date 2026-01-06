#!/usr/bin/env python3
"""
Efficient Instagram Sampler - Based on debug findings
"""

import os
import time
import random
import logging
import datetime
from spreadAnalysis.persistence.mongo import MongoSpread

def main():
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs('./logs', exist_ok=True)
    log_file = f'./logs/instagram_efficient_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    logger.info('🚀 EFFICIENT INSTAGRAM SAMPLING')
    logger.info('=' * 50)

    client = MongoSpread().client
    db = client.spreadAnalysis

    try:
        # Step 1: Get our target actors (this is fast)
        logger.info('🎯 Getting target Instagram actors...')
        
        target_actors = []
        for lang in ['da', 'de', 'sv']:
            actors = list(db.actor_metric.find({
                'platform': 'instagram',
                'lang': lang
            }, {'actor_username': 1, 'lang': 1}))
            target_actors.extend(actors)
            logger.info(f'   {lang}: {len(actors)} actors')
        
        # Create set of target usernames for fast lookup
        target_usernames = {actor['actor_username'] for actor in target_actors if actor.get('actor_username')}
        logger.info(f'✅ {len(target_usernames)} unique target actors')
        
        # Step 2: Use MongoDB $sample to get random Instagram posts efficiently
        logger.info('🎲 Using MongoDB $sample for efficient random sampling...')
        
        # Start with manageable sample size
        sample_size = 10000
        logger.info(f'📦 Sampling {sample_size} random Instagram posts...')
        
        # MongoDB aggregation pipeline with $sample
        pipeline = [
            {'$match': {'platform': 'instagram'}},
            {'$sample': {'size': sample_size}},
            {'$project': {'_id': 1, 'author': 1}}
        ]
        
        sampled_posts = list(db.post.aggregate(pipeline))
        logger.info(f'✅ Got {len(sampled_posts)} random Instagram posts')
        
        # Step 3: Filter for our target actors (in memory - fast)
        logger.info('🔍 Filtering for target language actors...')
        
        matching_posts = []
        author_formats_seen = {}
        
        for post in sampled_posts:
            author = post.get('author')
            
            # Track author formats we see
            author_type = type(author).__name__
            if author_type not in author_formats_seen:
                author_formats_seen[author_type] = 0
            author_formats_seen[author_type] += 1
            
            # Extract username from different possible formats
            author_username = None
            
            if isinstance(author, dict):
                # Try common username fields
                author_username = (author.get('username') or 
                                 author.get('actor_username') or 
                                 author.get('name') or
                                 author.get('user') or
                                 author.get('handle'))
            elif isinstance(author, str):
                author_username = author
            
            # Check if this actor is one of our targets
            if author_username and author_username in target_usernames:
                matching_posts.append({
                    'post_id': post['_id'],
                    'actor': author_username,
                    'author_format': author_type
                })
                logger.info(f'   ✅ Found target actor: {author_username}')
        
        logger.info(f'📊 Author format distribution in sample:')
        for fmt, count in author_formats_seen.items():
            logger.info(f'   {fmt}: {count} posts')
        
        logger.info(f'🎉 Found {len(matching_posts)} posts from target actors!')
        
        # Step 4: Sample 10% of matching posts
        if matching_posts:
            sample_count = max(1, len(matching_posts) // 10)  # At least 1 post
            sampled_matches = random.sample(matching_posts, min(sample_count, len(matching_posts)))
            
            logger.info(f'🎲 Sampled {len(sampled_matches)} posts (10% of matches)')
            
            # Save results
            os.makedirs('./data/percentage_sampled', exist_ok=True)
            sample_file = f'./data/percentage_sampled/instagram_efficient_{len(sampled_matches)}posts_{timestamp}.txt'
            
            with open(sample_file, 'w') as f:
                for item in sampled_matches:
                    f.write(f"{item['post_id']}\t{item['actor']}\t{item['author_format']}\n")
            
            # Also save detailed log
            detail_file = f'./data/percentage_sampled/instagram_details_{timestamp}.txt'
            with open(detail_file, 'w') as f:
                f.write(f"Sample size: {sample_size}\n")
                f.write(f"Posts retrieved: {len(sampled_posts)}\n")
                f.write(f"Target actors: {len(target_usernames)}\n")
                f.write(f"Matching posts: {len(matching_posts)}\n")
                f.write(f"Final sample: {len(sampled_matches)}\n")
                f.write(f"Author formats: {author_formats_seen}\n")
                f.write("\nMatching actors found:\n")
                for item in matching_posts:
                    f.write(f"  {item['actor']} ({item['author_format']})\n")
            
            logger.info(f'💾 Results saved to: {sample_file}')
            logger.info(f'📝 Details saved to: {detail_file}')
            logger.info('🎉 EFFICIENT SAMPLING SUCCESS!')
            
        else:
            logger.warning('⚠️ No posts found from target actors in sample')
            logger.info('💡 This might mean:')
            logger.info('   - Author field format is different than expected')
            logger.info('   - Target actors have very few posts')
            logger.info('   - Need larger sample size')
        
    except Exception as e:
        logger.error(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        
    finally:
        client.close()

if __name__ == "__main__":
    main()
