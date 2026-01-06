// MongoDB Shell Commands to Discover Language Fields for Each Platform
// Run these commands in mongosh to find exact language field names

// ========================================
// 1. GET ALL AVAILABLE PLATFORMS
// ========================================
print("🌍 AVAILABLE PLATFORMS:");
db.post.distinct("platform");

// ========================================
// 2. INSTAGRAM - Check language fields (we know this works)
// ========================================
print("\n📱 INSTAGRAM:");
var instagramPost = db.post.findOne({"platform": "instagram"});
if (instagramPost) {
    print("Sample post keys containing 'lang':");
    Object.keys(instagramPost).filter(key => key.toLowerCase().includes('lang')).forEach(key => {
        print("  " + key + ": " + instagramPost[key]);
    });
    print("Sample lang value: " + instagramPost.lang);
}

// Count target languages in Instagram
print("Instagram target language counts:");
["da", "de", "sv"].forEach(lang => {
    var count = db.post.countDocuments({"platform": "instagram", "lang": lang});
    print("  " + lang + ": " + count);
});

// ========================================
// 3. GAB - Check language fields
// ========================================
print("\n📱 GAB:");
var gabPost = db.post.findOne({"platform": "gab"});
if (gabPost) {
    print("Sample post keys containing 'lang':");
    Object.keys(gabPost).filter(key => key.toLowerCase().includes('lang')).forEach(key => {
        print("  " + key + ": " + gabPost[key]);
    });
    print("All Gab post fields:");
    Object.keys(gabPost).slice(0, 20).forEach(key => {
        print("  " + key + ": " + gabPost[key]);
    });
} else {
    print("No Gab posts found");
}

// Test different language field names for Gab
print("Gab language field tests:");
["da", "de", "sv"].forEach(lang => {
    var langCount = db.post.countDocuments({"platform": "gab", "lang": lang});
    var languageCount = db.post.countDocuments({"platform": "gab", "language": lang});
    print("  " + lang + " (lang field): " + langCount);
    print("  " + lang + " (language field): " + languageCount);
});

// ========================================
// 4. REDDIT - Check language fields
// ========================================
print("\n📱 REDDIT:");
var redditPost = db.post.findOne({"platform": "reddit"});
if (redditPost) {
    print("Sample post keys containing 'lang':");
    Object.keys(redditPost).filter(key => key.toLowerCase().includes('lang')).forEach(key => {
        print("  " + key + ": " + redditPost[key]);
    });
    print("Sample reddit post structure (first 15 fields):");
    Object.keys(redditPost).slice(0, 15).forEach(key => {
        print("  " + key + ": " + redditPost[key]);
    });
} else {
    print("No Reddit posts found");
}

// Test different language field names for Reddit
print("Reddit language field tests:");
["da", "de", "sv"].forEach(lang => {
    var langCount = db.post.countDocuments({"platform": "reddit", "lang": lang});
    var languageCount = db.post.countDocuments({"platform": "reddit", "language": lang});
    print("  " + lang + " (lang field): " + langCount);
    print("  " + lang + " (language field): " + languageCount);
});

// ========================================
// 5. FACEBOOK - Check language fields
// ========================================
print("\n📱 FACEBOOK:");
var facebookPost = db.post.findOne({"platform": "facebook"});
if (facebookPost) {
    print("Sample post keys containing 'lang':");
    Object.keys(facebookPost).filter(key => key.toLowerCase().includes('lang')).forEach(key => {
        print("  " + key + ": " + facebookPost[key]);
    });
    print("Sample lang value: " + facebookPost.lang);
}

// Count target languages in Facebook
print("Facebook target language counts:");
["da", "de", "sv"].forEach(lang => {
    var count = db.post.countDocuments({"platform": "facebook", "lang": lang});
    print("  " + lang + ": " + count);
});

// ========================================
// 6. TWITTER - Check language fields
// ========================================
print("\n📱 TWITTER:");
var twitterPost = db.post.findOne({"platform": "twitter"});
if (twitterPost) {
    print("Sample post keys containing 'lang':");
    Object.keys(twitterPost).filter(key => key.toLowerCase().includes('lang')).forEach(key => {
        print("  " + key + ": " + twitterPost[key]);
    });
    print("Sample lang value: " + twitterPost.lang);
}

// Count target languages in Twitter
print("Twitter target language counts:");
["da", "de", "sv"].forEach(lang => {
    var count = db.post.countDocuments({"platform": "twitter", "lang": lang});
    print("  " + lang + ": " + count);
});

// ========================================
// 7. TELEGRAM - Check language fields
// ========================================
print("\n📱 TELEGRAM:");
var telegramPost = db.post.findOne({"platform": "telegram"});
if (telegramPost) {
    print("Sample post keys containing 'lang':");
    Object.keys(telegramPost).filter(key => key.toLowerCase().includes('lang')).forEach(key => {
        print("  " + key + ": " + telegramPost[key]);
    });
    print("Sample telegram post structure (first 15 fields):");
    Object.keys(telegramPost).slice(0, 15).forEach(key => {
        print("  " + key + ": " + telegramPost[key]);
    });
}

// Test different language field names for Telegram
print("Telegram language field tests:");
["da", "de", "sv"].forEach(lang => {
    var langCount = db.post.countDocuments({"platform": "telegram", "lang": lang});
    var languageCount = db.post.countDocuments({"platform": "telegram", "language": lang});
    print("  " + lang + " (lang field): " + langCount);
    print("  " + lang + " (language field): " + languageCount);
});

// ========================================
// 8. ALL OTHER PLATFORMS - Quick check
// ========================================
print("\n🔍 OTHER PLATFORMS QUICK CHECK:");
var otherPlatforms = ["youtube", "vkontakte", "fourchan", "tiktok"];
otherPlatforms.forEach(platform => {
    print("\n📱 " + platform.toUpperCase() + ":");
    var post = db.post.findOne({"platform": platform});
    if (post) {
        var langFields = Object.keys(post).filter(key => key.toLowerCase().includes('lang'));
        print("  Language-related fields: " + langFields);
        print("  lang value: " + post.lang);
        
        // Quick count
        var totalTargetLangs = 0;
        ["da", "de", "sv"].forEach(lang => {
            var count = db.post.countDocuments({"platform": platform, "lang": lang});
            totalTargetLangs += count;
        });
        print("  Total target language posts: " + totalTargetLangs);
    } else {
        print("  No posts found");
    }
});

print("\n🎉 Language field discovery complete!");
