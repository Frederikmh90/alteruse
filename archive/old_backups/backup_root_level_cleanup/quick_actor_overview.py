#!/usr/bin/env python3
"""
Quick Actor Collections Overview
Just show basic info for each actor collection
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
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=15000,
        connectTimeoutMS=8000,
        maxPoolSize=1,
    )
    return client, client[name]


def quick_collection_overview(db, collection_name):
    """Quick overview of one collection."""
    print(f"\n🔬 {collection_name}")
    print("-" * 40)

    try:
        collection = db[collection_name]

        # Count
        count = collection.count_documents({})
        print(f"📊 Documents: {count:,}")

        # Sample document
        sample = collection.find_one({})
        if sample:
            # Show key fields
            fields = list(sample.keys())
            print(
                f"🔑 Fields ({len(fields)}): {fields[:10]}{'...' if len(fields) > 10 else ''}"
            )

            # Look for language fields specifically
            lang_fields = [
                f
                for f in fields
                if any(x in f.lower() for x in ["lang", "country", "locale", "region"])
            ]
            if lang_fields:
                print(f"🎯 Language fields: {lang_fields}")
                for lf in lang_fields[:3]:
                    value = sample.get(lf)
                    print(f"   {lf}: {value}")

            # Look for name/display fields for Danish examples
            name_fields = [
                f
                for f in fields
                if any(x in f.lower() for x in ["name", "display", "username"])
            ]
            if name_fields:
                print(f"📝 Name fields: {name_fields}")
                for nf in name_fields[:2]:
                    value = sample.get(nf)
                    if value and isinstance(value, str):
                        print(f"   {nf}: {value}")
                        # Check if it contains Danish/German/Swedish indicators
                        value_lower = value.lower()
                        if any(
                            keyword in value_lower
                            for keyword in [
                                "dansk",
                                "deutsch",
                                "svensk",
                                "denmark",
                                "germany",
                                "sweden",
                            ]
                        ):
                            print(f"   🎯 POTENTIAL TARGET LANGUAGE ACTOR!")

            # Quick language value check if lang field exists
            if "lang" in fields:
                try:
                    # Check for target languages
                    target_langs = ["da", "de", "sv"]
                    for lang in target_langs:
                        count = collection.count_documents({"lang": lang}, limit=1000)
                        if count > 0:
                            print(f"   🎯 '{lang}' actors: {count}")
                except Exception:
                    pass

        else:
            print("❌ No sample document")

    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    try:
        print("🔗 Connecting to MongoDB...")
        client, db = connect_to_mongo()
        print("✅ Connected successfully")

        # Actor collections we found
        actor_collections = [
            "actor_metric",
            "actor",
            "actor_post",
            "actor_platform_post",
            "actor_info",
        ]

        print(f"\n📊 QUICK OVERVIEW OF {len(actor_collections)} ACTOR COLLECTIONS")
        print("=" * 60)

        for collection_name in actor_collections:
            quick_collection_overview(db, collection_name)

        print(f"\n💡 KEY FINDINGS SUMMARY:")
        print("- Look for collections with 'lang' field containing 'da', 'de', 'sv'")
        print("- Check display_name/name fields for Danish/German/Swedish text")
        print("- Use these to pre-filter actors before post collection")

        client.close()

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
