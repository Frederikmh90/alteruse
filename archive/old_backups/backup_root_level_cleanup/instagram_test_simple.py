#!/usr/bin/env python3
"""
Simple Instagram Test - Diagnose MongoDB connection issues
"""

import sys
import time
import logging
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, NetworkTimeout

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def test_mongodb_connection():
    """Test basic MongoDB connectivity."""

    logger.info("🔍 Testing MongoDB connection...")

    try:
        # Try connecting with very short timeout
        client = MongoClient(
            "localhost",
            27017,
            serverSelectionTimeoutMS=5000,  # 5 second timeout
            socketTimeoutMS=10000,
        )  # 10 second socket timeout

        # Test connection
        client.admin.command("ping")
        logger.info("✅ MongoDB ping successful")

        # Test database access
        db = client.spreadAnalysis
        logger.info("✅ Connected to spreadAnalysis database")

        # Test collection access with timeout
        collection_names = db.list_collection_names()
        logger.info(f"✅ Found {len(collection_names)} collections")

        # Quick test on actor_metric
        if "actor_metric" in collection_names:
            logger.info("🔍 Testing actor_metric collection...")

            # Very simple count with timeout
            try:
                count = db.actor_metric.count_documents({}, limit=1)
                logger.info(f"✅ actor_metric accessible, has documents: {count > 0}")
            except Exception as e:
                logger.error(f"❌ actor_metric test failed: {e}")

        client.close()
        return True

    except ServerSelectionTimeoutError as e:
        logger.error(f"❌ MongoDB server selection timeout: {e}")
        return False
    except NetworkTimeout as e:
        logger.error(f"❌ MongoDB network timeout: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        return False


def test_instagram_actors():
    """Test finding Instagram actors - with very conservative approach."""

    logger.info("🧪 Testing Instagram actor query...")

    try:
        client = MongoClient(
            "localhost", 27017, serverSelectionTimeoutMS=10000, socketTimeoutMS=15000
        )

        db = client.spreadAnalysis

        # Use find_one instead of find to avoid large result sets
        sample_actor = db.actor_metric.find_one(
            {"platform": "instagram", "lang": {"$in": ["da", "de", "sv"]}},
            {"actor_username": 1, "lang": 1, "n_posts": 1},
        )

        if sample_actor:
            logger.info(f"✅ Found sample Instagram actor: {sample_actor}")
            return True
        else:
            logger.warning("⚠️ No Instagram actors found with target languages")
            return False

    except Exception as e:
        logger.error(f"❌ Instagram actor test failed: {e}")
        return False
    finally:
        try:
            client.close()
        except:
            pass


def main():
    """Run diagnostic tests."""

    logger.info("🚀 MONGODB & INSTAGRAM DIAGNOSTIC TEST")
    logger.info("=" * 50)

    # Test 1: Basic MongoDB connection
    if not test_mongodb_connection():
        logger.error("❌ Basic MongoDB connection failed - stopping tests")
        return False

    # Test 2: Instagram actors
    if not test_instagram_actors():
        logger.error("❌ Instagram actor query failed")
        return False

    logger.info("🎉 All tests passed! Ready for percentage sampling.")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
