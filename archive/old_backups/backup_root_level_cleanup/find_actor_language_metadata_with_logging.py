#!/usr/bin/env python3
"""
Find Actor Language Metadata in Database - WITH COMPREHENSIVE LOGGING
Identify collections and fields that contain actor language information
"""

from pymongo import MongoClient
from spreadAnalysis.persistence.mongo import MongoSpread
import json
import datetime

# Global log file setup
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"actor_language_analysis_{timestamp}.log"


def log_and_print(message):
    """Print and log message to file."""
    print(message)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def connect_to_mongo():
    """Connect with minimal settings."""
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=15000,
        connectTimeoutMS=8000,
        maxPoolSize=1,
    )
    return client, client[name]


def find_actor_collections(db):
    """Find all collections that might contain actor information."""

    log_and_print("🔍 FINDING ACTOR-RELATED COLLECTIONS")
    log_and_print("=" * 60)

    # Get all collection names
    all_collections = db.list_collection_names()

    # Filter for actor-related collections
    actor_collections = []
    for collection_name in all_collections:
        if any(
            keyword in collection_name.lower()
            for keyword in ["actor", "user", "profile", "account"]
        ):
            actor_collections.append(collection_name)

    log_and_print(f"📊 Total collections: {len(all_collections)}")
    log_and_print(f"🎭 Actor-related collections: {len(actor_collections)}")

    for collection in actor_collections:
        log_and_print(f"  - {collection}")

    return actor_collections


def analyze_actor_collection_structure(db, collection_name):
    """Analyze the structure of an actor collection to find language fields."""

    log_and_print(f"\n🔬 ANALYZING COLLECTION: {collection_name}")
    log_and_print("-" * 50)

    try:
        collection = db[collection_name]

        # Get document count
        doc_count = collection.count_documents({})
        log_and_print(f"📊 Document count: {doc_count:,}")

        if doc_count == 0:
            log_and_print("❌ Empty collection")
            return

        # Get a sample document
        sample_doc = collection.find_one({})
        if not sample_doc:
            log_and_print("❌ No sample document found")
            return

        log_and_print(f"📄 Sample document structure:")

        # Analyze fields at root level
        root_fields = list(sample_doc.keys())
        log_and_print(f"🔑 Root fields ({len(root_fields)}): {root_fields}")

        # Look for language-related fields
        language_fields = []
        for field in root_fields:
            if any(
                lang_key in field.lower()
                for lang_key in ["lang", "language", "locale", "country", "region"]
            ):
                value = sample_doc.get(field)
                language_fields.append(
                    {"field": field, "value": value, "type": type(value).__name__}
                )

        if language_fields:
            log_and_print(f"🎯 Language-related fields found:")
            for lang_field in language_fields:
                log_and_print(
                    f"  - {lang_field['field']}: {lang_field['value']} ({lang_field['type']})"
                )
        else:
            log_and_print("❌ No obvious language fields at root level")

        # Look for nested language fields
        nested_language_fields = find_nested_language_fields(sample_doc)
        if nested_language_fields:
            log_and_print(f"🔍 Nested language fields found:")
            for nested_field in nested_language_fields:
                log_and_print(f"  - {nested_field['path']}: {nested_field['value']}")

        # Test language distribution if we found language fields
        if language_fields or nested_language_fields:
            test_language_distribution(
                collection, language_fields, nested_language_fields
            )

    except Exception as e:
        log_and_print(f"❌ Error analyzing {collection_name}: {e}")


def find_nested_language_fields(doc, path="", max_depth=3):
    """Recursively find language-related fields in nested documents."""
    language_fields = []

    if max_depth <= 0:
        return language_fields

    if isinstance(doc, dict):
        for key, value in doc.items():
            current_path = f"{path}.{key}" if path else key

            # Check if this key looks like a language field
            if any(
                lang_key in key.lower()
                for lang_key in ["lang", "language", "locale", "country", "region"]
            ):
                language_fields.append(
                    {"path": current_path, "value": value, "type": type(value).__name__}
                )

            # Recurse into nested objects
            if isinstance(value, dict):
                language_fields.extend(
                    find_nested_language_fields(value, current_path, max_depth - 1)
                )
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Check first item in lists of objects
                language_fields.extend(
                    find_nested_language_fields(
                        value[0], f"{current_path}[0]", max_depth - 1
                    )
                )

    return language_fields


def test_language_distribution(collection, language_fields, nested_language_fields):
    """Test the distribution of language values in the collection."""

    log_and_print(f"\n📊 Testing language value distributions:")

    target_languages = ["da", "de", "sv"]

    # Test each language field
    all_fields = language_fields + nested_language_fields

    for field_info in all_fields[:3]:  # Test only first 3 to avoid timeouts
        field_path = field_info.get("path", field_info.get("field"))
        log_and_print(f"\n🔍 Field: {field_path}")

        try:
            # Create aggregation pipeline based on field path
            if "." in field_path:
                # Nested field
                field_reference = f"${field_path}"
            else:
                # Root field
                field_reference = f"${field_path}"

            pipeline = [
                {"$group": {"_id": field_reference, "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ]

            result = list(collection.aggregate(pipeline, allowDiskUse=True))

            if result:
                log_and_print(f"  Top values:")
                target_found = 0
                for item in result:
                    value = item["_id"]
                    count = item["count"]
                    is_target = False

                    if value and isinstance(value, str):
                        value_lower = value.lower()
                        if any(
                            target_lang in value_lower
                            for target_lang in target_languages
                        ):
                            is_target = True
                            target_found += 1

                    marker = "🎯" if is_target else "  "
                    log_and_print(f"    {marker} {value}: {count:,}")

                if target_found > 0:
                    log_and_print(f"  ✅ Found {target_found} target language values!")
                else:
                    log_and_print(f"  ❌ No target languages found in top values")
            else:
                log_and_print(f"  ❌ No data found")

        except Exception as e:
            log_and_print(f"  ❌ Aggregation failed: {str(e)[:100]}...")


def test_actor_post_connection(db, actor_collections):
    """Test how actor collections connect to posts."""

    log_and_print(f"\n🔗 TESTING ACTOR-POST CONNECTIONS")
    log_and_print("=" * 50)

    # We know posts exist, let's see how they reference actors
    try:
        sample_post = db.post.find_one({}, {"author": 1, "_id": 1, "platform": 1})
        if sample_post:
            log_and_print(f"📄 Sample post author structure:")
            author = sample_post.get("author")
            if author:
                log_and_print(f"  Author field type: {type(author).__name__}")
                if isinstance(author, dict):
                    log_and_print(f"  Author subfields: {list(author.keys())}")
                    # Show values for first few fields
                    for key, value in list(author.items())[:5]:
                        log_and_print(f"    {key}: {value}")
                else:
                    log_and_print(f"  Author value: {author}")
            else:
                log_and_print(f"  ❌ No author field")

        log_and_print(f"\n🔍 Testing actor collection connections:")

        for collection_name in actor_collections:
            try:
                collection = db[collection_name]

                # Get a sample document
                sample_actor = collection.find_one({})
                if sample_actor:
                    actor_fields = list(sample_actor.keys())

                    # Look for username/ID fields that might match post authors
                    potential_id_fields = []
                    for field in actor_fields:
                        if any(
                            id_key in field.lower()
                            for id_key in [
                                "username",
                                "user_id",
                                "id",
                                "handle",
                                "name",
                            ]
                        ):
                            potential_id_fields.append(field)

                    log_and_print(f"  {collection_name}:")
                    log_and_print(f"    Potential ID fields: {potential_id_fields}")

                    # Show sample values
                    for field in potential_id_fields[:3]:
                        value = sample_actor.get(field)
                        log_and_print(f"      {field}: {value}")

            except Exception as e:
                log_and_print(f"    ❌ Error with {collection_name}: {str(e)[:50]}...")

    except Exception as e:
        log_and_print(f"❌ Error testing connections: {e}")


def quick_target_language_test(db, actor_collections):
    """Quick test to find Danish, German, Swedish actors."""

    log_and_print(f"\n🎯 QUICK TARGET LANGUAGE TEST")
    log_and_print("=" * 50)

    target_keywords = [
        "dansk",
        "danish",
        "denmark",
        "da",
        "dk",
        "deutsch",
        "german",
        "germany",
        "de",
        "svensk",
        "swedish",
        "sweden",
        "sv",
        "se",
    ]

    for collection_name in actor_collections:
        try:
            collection = db[collection_name]
            log_and_print(f"\n🔍 Testing {collection_name} for target language actors:")

            # Test text search for language keywords
            for keyword in target_keywords[:6]:  # Test first 6 keywords
                try:
                    count = collection.count_documents(
                        {"$text": {"$search": keyword}}, limit=1000
                    )
                    if count > 0:
                        log_and_print(f"  🎯 '{keyword}': {count} matches")

                        # Get a sample
                        sample = collection.find_one({"$text": {"$search": keyword}})
                        if sample:
                            display_name = sample.get(
                                "display_name",
                                sample.get("name", sample.get("username", "unknown")),
                            )
                            log_and_print(f"    Example: {display_name}")

                except Exception:
                    # Text search might not be available, try regex
                    try:
                        regex_query = {
                            "$or": [
                                {"display_name": {"$regex": keyword, "$options": "i"}},
                                {"name": {"$regex": keyword, "$options": "i"}},
                                {"username": {"$regex": keyword, "$options": "i"}},
                            ]
                        }
                        count = collection.count_documents(regex_query, limit=100)
                        if count > 0:
                            log_and_print(f"  🎯 '{keyword}' (regex): {count} matches")
                    except Exception:
                        pass

        except Exception as e:
            log_and_print(f"    ❌ Error with {collection_name}: {str(e)[:50]}...")


def main():
    """Main analysis function."""

    try:
        log_and_print("🔗 Connecting to MongoDB...")
        client, db = connect_to_mongo()
        log_and_print("✅ Connected successfully")
        log_and_print(f"📝 Logging to: {log_file}")

        # Find actor-related collections
        actor_collections = find_actor_collections(db)

        if not actor_collections:
            log_and_print("❌ No actor-related collections found")
            log_and_print("   Let me check some common collection names...")

            # Check some common names manually
            common_names = [
                "actor_metric",
                "users",
                "profiles",
                "accounts",
                "actors",
                "actor_info",
            ]
            for name in common_names:
                try:
                    if name in db.list_collection_names():
                        log_and_print(f"  ✅ Found: {name}")
                        actor_collections.append(name)
                except:
                    pass

        # Analyze each actor collection
        for collection in actor_collections:
            analyze_actor_collection_structure(db, collection)

        # Test connections between actors and posts
        if actor_collections:
            test_actor_post_connection(db, actor_collections)

        # Quick test for target language actors
        if actor_collections:
            quick_target_language_test(db, actor_collections)

        # Final recommendations
        log_and_print(f"\n💡 RECOMMENDATIONS:")
        log_and_print("1. Look for collections with language metadata at actor level")
        log_and_print(
            "2. Use actor language info to pre-filter actors before post collection"
        )
        log_and_print(
            "3. This will be much more efficient than post-level language filtering"
        )
        log_and_print(f"\n📁 Full analysis saved to: {log_file}")

        client.close()

    except Exception as e:
        log_and_print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
