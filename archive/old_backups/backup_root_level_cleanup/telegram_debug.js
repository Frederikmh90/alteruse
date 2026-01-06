// Quick Telegram debugging in mongosh
print("🔍 TELEGRAM LANGUAGE DEBUG");

// 1. Check if Telegram posts exist at all
print("\n1. Basic Telegram post count:");
try {
    var telegramCount = db.post.countDocuments({"platform": "telegram"});
    print("Total Telegram posts: " + telegramCount);
} catch(e) {
    print("Error counting Telegram posts: " + e);
}

// 2. Get one sample Telegram post to see structure
print("\n2. Sample Telegram post structure:");
try {
    var telegramPost = db.post.findOne({"platform": "telegram"});
    if (telegramPost) {
        print("Found sample Telegram post:");
        print("  _id: " + telegramPost._id);
        print("  method: " + telegramPost.method);
        print("  lang: " + telegramPost.lang);
        print("  language: " + telegramPost.language);
        
        // Show all fields that contain 'lang'
        print("  Language-related fields:");
        Object.keys(telegramPost).forEach(key => {
            if (key.toLowerCase().includes('lang')) {
                print("    " + key + ": " + telegramPost[key]);
            }
        });
        
        // Show first 10 fields for context
        print("  All fields (first 15):");
        Object.keys(telegramPost).slice(0, 15).forEach(key => {
            print("    " + key + ": " + telegramPost[key]);
        });
    } else {
        print("No Telegram posts found!");
    }
} catch(e) {
    print("Error getting Telegram sample: " + e);
}

// 3. Quick test for target languages in lang field
print("\n3. Testing 'lang' field for target languages:");
["da", "de", "sv"].forEach(lang => {
    try {
        var post = db.post.findOne({"platform": "telegram", "lang": lang});
        if (post) {
            print("✅ Found Telegram post with lang='" + lang + "'");
            print("   Post ID: " + post._id);
            print("   Method: " + post.method);
        } else {
            print("❌ No Telegram posts with lang='" + lang + "'");
        }
    } catch(e) {
        print("Error testing lang=" + lang + ": " + e);
    }
});

// 4. Quick test for target languages in language field
print("\n4. Testing 'language' field for target languages:");
["da", "de", "sv"].forEach(lang => {
    try {
        var post = db.post.findOne({"platform": "telegram", "language": lang});
        if (post) {
            print("✅ Found Telegram post with language='" + lang + "'");
            print("   Post ID: " + post._id);
            print("   Method: " + post.method);
        } else {
            print("❌ No Telegram posts with language='" + lang + "'");
        }
    } catch(e) {
        print("Error testing language=" + lang + ": " + e);
    }
});

// 5. Check what language values actually exist
print("\n5. Sample of actual language values in Telegram:");
try {
    var langSample = db.post.aggregate([
        {"$match": {"platform": "telegram", "lang": {"$exists": true, "$ne": null}}},
        {"$group": {"_id": "$lang", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]);
    
    print("Lang field distribution (top 10):");
    langSample.forEach(item => {
        print("  '" + item._id + "': " + item.count + " posts");
    });
} catch(e) {
    print("Error getting lang distribution: " + e);
}

// 6. Check language field distribution  
print("\n6. Sample of actual 'language' values in Telegram:");
try {
    var languageSample = db.post.aggregate([
        {"$match": {"platform": "telegram", "language": {"$exists": true, "$ne": null}}},
        {"$group": {"_id": "$language", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]);
    
    print("Language field distribution (top 10):");
    languageSample.forEach(item => {
        print("  '" + item._id + "': " + item.count + " posts");
    });
} catch(e) {
    print("Error getting language distribution: " + e);
}

print("\n🎉 Telegram debug complete!");
