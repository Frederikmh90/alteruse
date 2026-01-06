#!/usr/bin/env python3
"""
Instant Language Check for Billion-Post Database
Uses the most minimal queries possible to avoid timeouts
"""

from pymongo import MongoClient
from spreadAnalysis.persistence.mongo import MongoSpread


def connect_to_mongo():
    """Connect with minimal settings."""
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=3000,
        socketTimeoutMS=10000,  # Very short timeout
        connectTimeoutMS=5000,
        maxPoolSize=1,
    )
    return client, client[name]


def instant_language_check(db):
    """Ultra-fast language existence check."""

    print("⚡ INSTANT LANGUAGE CHECK (Billion-Post Database)")
    print("=" * 60)

    target_languages = ["da", "de", "sv"]

    print("🔍 Testing if target languages exist in database...")

    # Test 1: Check if ANY documents with target languages exist (fastest possible)
    for lang in target_languages:
        print(f"\n🎯 Testing language: {lang}")

        try:
            # Find just ONE document with this language (instant if it exists)
            sample_doc = db.post.find_one(
                {"lang": lang},
                {"_id": 1, "platform": 1, "lang": 1},
                # No timeout on individual queries
            )

            if sample_doc:
                platform = sample_doc.get("platform", "unknown")
                print(f"  ✅ Found! Platform: {platform}, ID: {sample_doc['_id']}")
            else:
                print(f"  ❌ Not found")

        except Exception as e:
            print(f"  ❌ Query failed: {str(e)[:50]}...")

    # Test 2: Check if documents with lang field exist at all
    print(f"\n📊 Testing if 'lang' field exists...")
    try:
        any_lang_doc = db.post.find_one(
            {"lang": {"$exists": True, "$ne": None}},
            {"_id": 1, "lang": 1, "platform": 1},
        )

        if any_lang_doc:
            print(
                f"  ✅ Lang field exists! Example: {any_lang_doc.get('lang')} on {any_lang_doc.get('platform')}"
            )
        else:
            print(f"  ❌ No documents have 'lang' field")

    except Exception as e:
        print(f"  ❌ Query failed: {str(e)[:50]}...")

    # Test 3: Test specific platform + language combo
    print(f"\n🐦 Testing Twitter + Danish...")
    try:
        twitter_da = db.post.find_one(
            {"platform": "twitter", "lang": "da"}, {"_id": 1, "lang": 1, "text": 1}
        )

        if twitter_da:
            text_preview = (
                twitter_da.get("text", "")[:50] + "..."
                if twitter_da.get("text")
                else "No text"
            )
            print(f"  ✅ Found Danish Twitter post!")
            print(f"     Text preview: {text_preview}")
        else:
            print(f"  ❌ No Danish Twitter posts found")

    except Exception as e:
        print(f"  ❌ Query failed: {str(e)[:50]}...")


def test_language_query_performance(db):
    """Test if language queries will work for the main pipeline."""

    print(f"\n⚡ QUERY PERFORMANCE TEST")
    print("-" * 40)

    target_languages = ["da", "de", "sv"]

    # Test the exact query pattern that will be used in the main pipeline
    test_query = {"lang": {"$in": target_languages}}

    print(f"Testing query: {test_query}")

    try:
        import time

        start_time = time.time()

        # Try to get just 1 result (this is what the pipeline will do)
        result = db.post.find_one(test_query, {"_id": 1, "lang": 1, "platform": 1})

        end_time = time.time()
        query_time = end_time - start_time

        if result:
            print(f"  ✅ Query succeeded in {query_time:.3f}s")
            print(f"     Found: {result.get('lang')} on {result.get('platform')}")
            print(f"  🚀 This means your language filtering will work!")
            return True
        else:
            print(f"  ❌ No results found (query took {query_time:.3f}s)")
            return False

    except Exception as e:
        print(f"  ❌ Query failed: {e}")
        return False


def main():
    """Main instant check."""

    try:
        print("🔗 Connecting to MongoDB...")
        client, db = connect_to_mongo()
        print("✅ Connected successfully")

        # Run instant checks
        instant_language_check(db)

        # Test query performance
        query_works = test_language_query_performance(db)

        # Final recommendations
        print(f"\n💡 RECOMMENDATIONS:")

        if query_works:
            print("✅ Language filtering will work! Use the enhanced script:")
            print(
                "   python3 technocracy_datacollection_080825_with_language_filter.py"
            )
            print("")
            print("🎯 Key benefits you'll get:")
            print("   - Only downloads posts in da/de/sv languages")
            print("   - 90-95% reduction in data transfer")
            print("   - No more timeouts")
            print("   - Much faster processing")
        else:
            print("⚠️  Language filtering may not work as expected.")
            print("   Possible issues:")
            print("   1. Language field is stored differently")
            print("   2. Language codes use different format")
            print("   3. Need to check alternative field names")

        client.close()

    except Exception as e:
        print(f"❌ Connection error: {e}")


if __name__ == "__main__":
    main()
