#!/usr/bin/env python3
"""
Simple Language Field Test for VM
Fast, efficient queries that won't timeout
"""

import os
import time
from pymongo import MongoClient
from spreadAnalysis.persistence.mongo import MongoSpread


def connect_to_mongo():
    """Connect with longer timeouts for VM."""
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=10000,
        socketTimeoutMS=120000,  # 2 minutes
        connectTimeoutMS=15000,
        maxPoolSize=10,  # Smaller pool for VM
    )
    return client, client[name]


def quick_language_overview(db):
    """Get a quick overview without heavy queries."""

    print("🧪 SIMPLE LANGUAGE FIELD TEST (VM-OPTIMIZED)")
    print("=" * 60)

    target_languages = ["da", "de", "sv"]

    try:
        # 1. Get available platforms (fast)
        print("📊 Available platforms:")
        platforms = list(db.actor_metric.distinct("platform"))
        print(f"Found {len(platforms)} platforms: {platforms}")

        # 2. Quick language distribution check (limited)
        print(f"\n🔍 Language field distribution (top 15):")
        lang_dist = list(
            db.post.aggregate(
                [
                    {"$group": {"_id": "$lang", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 15},
                ],
                allowDiskUse=True,
            )
        )

        total_with_lang = sum(
            doc["count"] for doc in lang_dist if doc["_id"] is not None
        )
        target_lang_count = sum(
            doc["count"]
            for doc in lang_dist
            if doc["_id"] and doc["_id"].lower() in target_languages
        )

        print(f"Posts with language info: {total_with_lang:,}")
        for doc in lang_dist:
            lang_val = doc["_id"] or "null"
            count = doc["count"]
            is_target = lang_val.lower() in target_languages if lang_val else False
            marker = "🎯" if is_target else "  "
            print(f"  {marker} {lang_val}: {count:,}")

        # 3. Test target language filtering efficiency
        print(f"\n🎯 Target language filtering test:")
        print(f"Target languages: {target_languages}")

        direct_count = db.post.count_documents({"lang": {"$in": target_languages}})
        print(f"Direct match count: {direct_count:,}")

        # Only test regex if direct count is low
        if direct_count < 1000:
            regex_count = db.post.count_documents(
                {"lang": {"$regex": "^(da|de|sv)", "$options": "i"}}
            )
            print(f"Regex match count: {regex_count:,}")
        else:
            print("Regex test skipped (direct match sufficient)")

        # 4. Platform-specific quick test (only a few platforms)
        print(f"\n📱 Platform-specific test (sample):")
        test_platforms = platforms[:3]  # Test only first 3 to avoid timeout

        for platform in test_platforms:
            print(f"\n{platform}:")
            try:
                # Use estimated count for speed
                total_est = (
                    db.post.estimated_document_count()
                    if platform == platforms[0]
                    else None
                )
                platform_count = db.post.count_documents(
                    {"platform": platform}, limit=100000
                )

                if platform_count > 0:
                    lang_count = db.post.count_documents(
                        {"platform": platform, "lang": {"$in": target_languages}}
                    )
                    percentage = (
                        (lang_count / platform_count * 100) if platform_count > 0 else 0
                    )
                    print(f"  Total posts: {platform_count:,}")
                    print(f"  Target lang posts: {lang_count:,} ({percentage:.1f}%)")
                else:
                    print(f"  No posts found")

            except Exception as e:
                print(f"  ❌ Error: {str(e)[:50]}...")

        # 5. Sample document structure
        print(f"\n📄 Sample document structure:")
        try:
            sample = db.post.find_one(
                {}, {"_id": 1, "platform": 1, "lang": 1, "method": 1, "text": 1}
            )
            if sample:
                print(f"Sample fields: {list(sample.keys())}")
                print(f"  platform: {sample.get('platform')}")
                print(f"  lang: {sample.get('lang')}")
                print(f"  method: {sample.get('method')}")
                print(f"  has text: {'text' in sample}")
            else:
                print("No sample document found")
        except Exception as e:
            print(f"❌ Sample error: {e}")

    except Exception as e:
        print(f"❌ Query error: {e}")
        return False

    return True


def test_specific_language_query(db, platform="twitter", limit=100):
    """Test a specific, optimized language query."""

    print(f"\n🔬 TESTING OPTIMIZED QUERY for {platform}:")
    print("-" * 40)

    try:
        # Optimized query with projection and limit
        query = {"platform": platform, "lang": {"$in": ["da", "de", "sv"]}}

        projection = {
            "_id": 1,
            "platform": 1,
            "lang": 1,
            "text": {"$substr": ["$text", 0, 50]},  # Only first 50 chars
        }

        start_time = time.time()
        results = list(db.post.find(query, projection).limit(limit))
        end_time = time.time()

        print(f"✅ Found {len(results)} documents in {end_time - start_time:.2f}s")

        if results:
            print("Sample results:")
            for i, doc in enumerate(results[:3]):
                lang = doc.get("lang", "None")
                text_preview = (
                    doc.get("text", "")[:30] + "..." if doc.get("text") else "No text"
                )
                print(f"  {i + 1}. Lang: {lang}, Text: {text_preview}")

        return len(results) > 0

    except Exception as e:
        print(f"❌ Optimized query failed: {e}")
        return False


def main():
    """Main test with VM-friendly approach."""

    try:
        print("🔗 Connecting to MongoDB...")
        client, db = connect_to_mongo()
        print("✅ Connected successfully")

        # Run quick overview
        success = quick_language_overview(db)

        if success:
            # Test specific optimized query
            test_specific_language_query(db, "twitter", 50)

            print(f"\n💡 RECOMMENDATIONS:")
            print(
                "1. If target language counts are very low, check language field quality"
            )
            print("2. If queries are still slow, we need to add MongoDB indexes")
            print(
                "3. Consider filtering at actor selection stage instead of post stage"
            )

        else:
            print(f"\n⚠️  Database queries failed - need to optimize approach")

        client.close()

    except Exception as e:
        print(f"❌ Connection error: {e}")
        print("\n🔧 TROUBLESHOOTING:")
        print("1. Check if MongoDB service is running")
        print("2. Verify network connectivity")
        print("3. Consider increasing VM resources")


if __name__ == "__main__":
    main()
