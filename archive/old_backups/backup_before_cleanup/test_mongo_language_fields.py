#!/usr/bin/env python3
"""
Test script to examine language field locations across platforms in MongoDB
"""

import os
import json
from typing import Dict, List, Any
from pymongo import MongoClient
from spreadAnalysis.persistence.mongo import MongoSpread
from spreadAnalysis.persistence.schemas import Spread


def connect_to_mongo():
    """Connect to MongoDB using your existing configuration."""
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=4000,
        socketTimeoutMS=30000,
        connectTimeoutMS=8000,
    )
    return client, client[name]


def test_language_fields_by_platform(db):
    """Test language field locations across different platforms."""

    print("🔍 TESTING LANGUAGE FIELDS BY PLATFORM")
    print("=" * 60)

    # Get available platforms
    platforms = list(db.actor_metric.distinct("platform"))
    print(f"Available platforms: {platforms}")

    results = {}

    for platform in platforms[:5]:  # Test first 5 platforms
        print(f"\n📊 Testing platform: {platform}")
        print("-" * 40)

        # Get sample posts for this platform
        sample_posts = list(db.post.find({"platform": platform}, limit=10))

        if not sample_posts:
            print(f"  ❌ No posts found for platform: {platform}")
            continue

        print(f"  Found {len(sample_posts)} sample posts")

        # Analyze language field locations
        lang_field_analysis = {
            "direct_lang": [],
            "nested_lang_fields": [],
            "spread_getter_results": [],
            "raw_sample_docs": [],
        }

        for i, post in enumerate(sample_posts[:3]):  # Analyze first 3 posts
            print(f"\n  📄 Post {i + 1}:")

            # 1. Check direct 'lang' field
            direct_lang = post.get("lang")
            if direct_lang:
                lang_field_analysis["direct_lang"].append(direct_lang)
                print(f"    ✅ Direct 'lang': {direct_lang}")
            else:
                print(f"    ❌ No direct 'lang' field")

            # 2. Look for nested language fields
            nested_langs = find_nested_language_fields(post)
            if nested_langs:
                lang_field_analysis["nested_lang_fields"].extend(nested_langs)
                print(f"    ✅ Nested lang fields: {nested_langs}")
            else:
                print(f"    ❌ No nested language fields found")

            # 3. Test Spread getter
            try:
                spread_lang = Spread._get_lang(method=post.get("method"), data=post)
                if spread_lang:
                    lang_field_analysis["spread_getter_results"].append(spread_lang)
                    print(f"    ✅ Spread._get_lang(): {spread_lang}")
                else:
                    print(f"    ❌ Spread._get_lang(): None")
            except Exception as e:
                print(f"    ❌ Spread._get_lang() error: {e}")

            # 4. Store raw sample (first post only)
            if i == 0:
                # Store a cleaned version for analysis
                sample_doc = {
                    k: v
                    for k, v in post.items()
                    if k
                    in [
                        "_id",
                        "platform",
                        "method",
                        "lang",
                        "text",
                        "author",
                        "created_at",
                        "id",
                        "source",
                    ]
                }
                lang_field_analysis["raw_sample_docs"].append(sample_doc)

        results[platform] = lang_field_analysis

        # Summary for this platform
        unique_direct_langs = list(set(lang_field_analysis["direct_lang"]))
        unique_spread_langs = list(set(lang_field_analysis["spread_getter_results"]))

        print(f"\n  📋 Summary for {platform}:")
        print(f"    Direct lang values: {unique_direct_langs}")
        print(f"    Spread getter values: {unique_spread_langs}")
        print(
            f"    Nested field count: {len(lang_field_analysis['nested_lang_fields'])}"
        )

    return results


def find_nested_language_fields(doc, path="", max_depth=3):
    """Recursively find fields that might contain language information."""
    lang_fields = []

    if max_depth <= 0:
        return lang_fields

    if isinstance(doc, dict):
        for key, value in doc.items():
            current_path = f"{path}.{key}" if path else key

            # Check if this key looks like a language field
            if any(
                lang_key in key.lower() for lang_key in ["lang", "language", "locale"]
            ):
                lang_fields.append({"path": current_path, "value": value})

            # Recurse into nested objects
            if isinstance(value, dict):
                lang_fields.extend(
                    find_nested_language_fields(value, current_path, max_depth - 1)
                )
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Check first item in lists of objects
                lang_fields.extend(
                    find_nested_language_fields(
                        value[0], f"{current_path}[0]", max_depth - 1
                    )
                )

    return lang_fields


def test_language_filtering_queries(db):
    """Test different MongoDB queries for language filtering."""

    print("\n🔍 TESTING LANGUAGE FILTERING QUERIES")
    print("=" * 60)

    target_languages = ["da", "de", "sv"]

    # Test different query approaches
    query_tests = [
        {"name": "Direct lang field", "query": {"lang": {"$in": target_languages}}},
        {
            "name": "Lang field with regex (case insensitive)",
            "query": {"lang": {"$regex": "^(da|de|sv)", "$options": "i"}},
        },
        {
            "name": "Lang field startswith (da-, de-, sv-)",
            "query": {
                "$or": [
                    {"lang": {"$regex": "^da"}},
                    {"lang": {"$regex": "^de"}},
                    {"lang": {"$regex": "^sv"}},
                ]
            },
        },
    ]

    for test in query_tests:
        print(f"\n📊 Testing: {test['name']}")
        print(f"Query: {test['query']}")

        try:
            # Count total matches
            count = db.post.count_documents(test["query"])
            print(f"  Total matches: {count:,}")

            if count > 0:
                # Get sample documents
                samples = list(db.post.find(test["query"], limit=5))
                print(f"  Sample lang values:")
                for i, doc in enumerate(samples):
                    lang_val = doc.get("lang", "None")
                    platform = doc.get("platform", "Unknown")
                    print(f"    {i + 1}. {platform}: {lang_val}")

        except Exception as e:
            print(f"  ❌ Query failed: {e}")


def test_platform_specific_language_queries(db, platforms_to_test=None):
    """Test language queries on specific platforms."""

    print("\n🔍 TESTING PLATFORM-SPECIFIC LANGUAGE QUERIES")
    print("=" * 60)

    if platforms_to_test is None:
        platforms_to_test = ["twitter", "facebook", "telegram"]

    target_languages = ["da", "de", "sv"]

    for platform in platforms_to_test:
        print(f"\n📊 Platform: {platform}")
        print("-" * 30)

        # Base query for this platform
        base_query = {"platform": platform}

        # Test with language filter
        lang_query = {"platform": platform, "lang": {"$in": target_languages}}

        try:
            total_posts = db.post.count_documents(base_query)
            lang_filtered_posts = db.post.count_documents(lang_query)

            print(f"  Total posts: {total_posts:,}")
            print(f"  Language filtered: {lang_filtered_posts:,}")

            if total_posts > 0:
                percentage = (lang_filtered_posts / total_posts) * 100
                print(f"  Percentage with target langs: {percentage:.1f}%")

            # Sample of language values for this platform
            sample_langs = db.post.aggregate(
                [
                    {"$match": base_query},
                    {"$group": {"_id": "$lang", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10},
                ]
            )

            print(f"  Top language values:")
            for lang_doc in sample_langs:
                lang_val = lang_doc["_id"] or "None"
                count = lang_doc["count"]
                print(f"    {lang_val}: {count:,}")

        except Exception as e:
            print(f"  ❌ Error testing {platform}: {e}")


def main():
    """Main test function."""

    print("🧪 MONGODB LANGUAGE FIELD TESTING")
    print("=" * 60)

    try:
        client, db = connect_to_mongo()
        print("✅ Connected to MongoDB")

        # Test 1: Analyze language fields by platform
        platform_results = test_language_fields_by_platform(db)

        # Test 2: Test language filtering queries
        test_language_filtering_queries(db)

        # Test 3: Test platform-specific queries
        test_platform_specific_language_queries(db)

        # Save results to file
        with open("language_field_analysis.json", "w") as f:
            json.dump(platform_results, f, indent=2, default=str)
        print(f"\n💾 Detailed results saved to: language_field_analysis.json")

        client.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
