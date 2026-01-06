#!/usr/bin/env python3
"""
Quick update for the running actor language filtering script.
Adds progress bars and performance optimizations.
"""

# Just copy the collect_posts_from_language_actors function from the updated script
# and replace it in your running version

OPTIMIZED_FUNCTION = '''
def collect_posts_from_language_actors(db, actor_usernames: List[str], platform: str):
    """
    Collect posts from pre-filtered language actors.
    Much more efficient than filtering posts by language.
    """

    if not actor_usernames:
        return []

    print(
        f"  📝 Collecting posts from {len(actor_usernames)} language-filtered actors..."
    )

    # OPTIMIZATION: Process actors in smaller batches to avoid huge queries
    ACTOR_BATCH_SIZE = 500  # Smaller batches for better performance
    ACTOR_BATCH_WORKERS = 3  # Parallel workers
    ACTOR_BATCH_LIMIT = 100_000  # Max posts per batch
    
    all_post_ids = []
    
    actor_batches = [
        actor_usernames[i:i + ACTOR_BATCH_SIZE] 
        for i in range(0, len(actor_usernames), ACTOR_BATCH_SIZE)
    ]
    
    print(f"  🔄 Processing {len(actor_batches)} batches of {ACTOR_BATCH_SIZE} actors each...")
    
    # Use ThreadPoolExecutor for parallel batch processing
    def process_actor_batch(actor_batch):
        """Process one batch of actors."""
        post_query = {
            "platform": platform,
            "$or": [
                {"author.username": {"$in": actor_batch}},
                {"author": {"$in": actor_batch}},  # Alternative author field structure
            ],
        }

        # Get post IDs from this batch with reasonable limit
        batch_limit = min(len(actor_batch) * POSTS_PER_ACTOR_SOFT, ACTOR_BATCH_LIMIT)
        post_cursor = db.post.find(post_query, {"_id": 1}).limit(batch_limit)
        return [post["_id"] for post in post_cursor]
    
    # Process batches with progress bar
    with ThreadPoolExecutor(max_workers=ACTOR_BATCH_WORKERS) as executor:
        futures = [executor.submit(process_actor_batch, batch) for batch in actor_batches]
        
        for i, future in enumerate(tqdm(futures, desc="  Collecting posts")):
            batch_post_ids = future.result()
            all_post_ids.extend(batch_post_ids)
            
            # Show periodic progress
            if (i + 1) % 3 == 0 or i == len(futures) - 1:
                avg_per_batch = len(all_post_ids) / (i + 1)
                print(f"    Batch {i + 1}/{len(futures)}: +{len(batch_post_ids)} posts (total: {len(all_post_ids):,}, avg: {avg_per_batch:.0f}/batch)")

    print(f"  ✅ Found {len(all_post_ids):,} posts from language actors")

    # Apply sampling if needed
    if len(all_post_ids) > MAX_POSTS_PER_ACTOR if MAX_POSTS_PER_ACTOR else len(all_post_ids):
        target_posts = int(
            len(actor_usernames) * POST_PERCENTAGE / 100 * POSTS_PER_ACTOR_SOFT
        )
        if MAX_POSTS_PER_ACTOR:
            target_posts = min(target_posts, len(actor_usernames) * MAX_POSTS_PER_ACTOR)

        if target_posts < len(all_post_ids):
            all_post_ids = random.Random(RANDOM_SEED).sample(all_post_ids, target_posts)
            print(f"  📊 Sampled down to {len(all_post_ids)} posts")

    return all_post_ids
'''

print("🔄 OPTIMIZATIONS ADDED:")
print("✅ Progress bars with tqdm")
print("✅ Parallel processing (3 workers)")
print("✅ Smaller batches (500 actors each)")
print("✅ Better progress reporting")
print("✅ Performance metrics")
print("\n📋 To apply these optimizations:")
print("1. Stop current script (Ctrl+C)")
print("2. Copy the updated actor_language_filtered_collection.py")
print("3. Restart the script")
print("\n🚀 Expected improvements:")
print("- Clear progress visibility")
print("- 2-3x faster post collection")
print("- Better memory usage")
print("- No more waiting in silence!")
