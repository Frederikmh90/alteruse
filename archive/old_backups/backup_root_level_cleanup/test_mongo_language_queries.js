// MongoDB Language Field Testing Script
// Run with: mongosh --file test_mongo_language_queries.js

print("🧪 MONGODB LANGUAGE FIELD TESTING");
print("=" .repeat(60));

// Test 1: Check available platforms
print("\n📊 Available Platforms:");
const platforms = db.actor_metric.distinct("platform");
print(`Found ${platforms.length} platforms: ${platforms.join(", ")}`);

// Test 2: Sample documents per platform to find language fields
print("\n🔍 ANALYZING LANGUAGE FIELDS BY PLATFORM");
print("=" .repeat(60));

const targetLanguages = ["da", "de", "sv"];

platforms.slice(0, 5).forEach(platform => {
    print(`\n📱 Platform: ${platform}`);
    print("-".repeat(40));
    
    // Get total posts for this platform
    const totalPosts = db.post.countDocuments({platform: platform});
    print(`Total posts: ${totalPosts.toLocaleString()}`);
    
    if (totalPosts === 0) {
        print("❌ No posts found for this platform");
        return;
    }
    
    // Sample documents to analyze structure
    const sampleDocs = db.post.find({platform: platform}).limit(3);
    
    print("📄 Sample document analysis:");
    let docIndex = 0;
    sampleDocs.forEach(doc => {
        docIndex++;
        print(`\n  Document ${docIndex}:`);
        
        // Check direct lang field
        if (doc.lang) {
            print(`    ✅ Direct 'lang': ${doc.lang}`);
        } else {
            print(`    ❌ No direct 'lang' field`);
        }
        
        // Check for nested language fields
        const nestedLangFields = findNestedLanguageFields(doc);
        if (nestedLangFields.length > 0) {
            print(`    ✅ Nested lang fields:`);
            nestedLangFields.forEach(field => {
                print(`      ${field.path}: ${field.value}`);
            });
        } else {
            print(`    ❌ No nested language fields found`);
        }
        
        // Show key fields for context
        print(`    📋 Key fields: platform=${doc.platform}, method=${doc.method}, _id=${doc._id}`);
    });
    
    // Test language filtering for this platform
    print(`\n  🎯 Language filtering test for ${platform}:`);
    
    // Test different query approaches
    const directLangQuery = {platform: platform, lang: {$in: targetLanguages}};
    const directLangCount = db.post.countDocuments(directLangQuery);
    
    const regexLangQuery = {platform: platform, lang: {$regex: "^(da|de|sv)", $options: "i"}};
    const regexLangCount = db.post.countDocuments(regexLangQuery);
    
    print(`    Direct match (lang in ["da","de","sv"]): ${directLangCount.toLocaleString()}`);
    print(`    Regex match (starts with da|de|sv): ${regexLangCount.toLocaleString()}`);
    
    // Show top language values for this platform
    const topLangs = db.post.aggregate([
        {$match: {platform: platform}},
        {$group: {_id: "$lang", count: {$sum: 1}}},
        {$sort: {count: -1}},
        {$limit: 10}
    ]);
    
    print(`    📊 Top language values:`);
    topLangs.forEach(langDoc => {
        const langVal = langDoc._id || "null";
        const count = langDoc.count;
        const isTarget = targetLanguages.some(tl => langVal && langVal.toLowerCase().startsWith(tl));
        const marker = isTarget ? "🎯" : "  ";
        print(`      ${marker} ${langVal}: ${count.toLocaleString()}`);
    });
});

// Test 3: Overall language distribution
print("\n🌍 OVERALL LANGUAGE DISTRIBUTION");
print("=" .repeat(60));

const overallLangDistribution = db.post.aggregate([
    {$group: {_id: "$lang", count: {$sum: 1}}},
    {$sort: {count: -1}},
    {$limit: 20}
]);

print("Top 20 language values across all platforms:");
overallLangDistribution.forEach(langDoc => {
    const langVal = langDoc._id || "null";
    const count = langDoc.count;
    const isTarget = targetLanguages.some(tl => langVal && langVal.toLowerCase().startsWith(tl));
    const marker = isTarget ? "🎯" : "  ";
    print(`  ${marker} ${langVal}: ${count.toLocaleString()}`);
});

// Test 4: Target language filtering effectiveness
print("\n🎯 TARGET LANGUAGE FILTERING TEST");
print("=" .repeat(60));

const totalPosts = db.post.countDocuments({});
const targetLangPosts = db.post.countDocuments({lang: {$in: targetLanguages}});
const targetLangRegexPosts = db.post.countDocuments({lang: {$regex: "^(da|de|sv)", $options: "i"}});

print(`Total posts in database: ${totalPosts.toLocaleString()}`);
print(`Posts with exact target languages: ${targetLangPosts.toLocaleString()} (${(targetLangPosts/totalPosts*100).toFixed(1)}%)`);
print(`Posts matching target language regex: ${targetLangRegexPosts.toLocaleString()} (${(targetLangRegexPosts/totalPosts*100).toFixed(1)}%)`);

// Test 5: Platform-specific target language counts
print("\n📊 TARGET LANGUAGES BY PLATFORM");
print("=" .repeat(60));

platforms.forEach(platform => {
    const platformTotal = db.post.countDocuments({platform: platform});
    const platformTargetLangs = db.post.countDocuments({
        platform: platform, 
        lang: {$in: targetLanguages}
    });
    
    if (platformTotal > 0) {
        const percentage = (platformTargetLangs / platformTotal * 100).toFixed(1);
        print(`${platform.padEnd(15)} ${platformTargetLangs.toLocaleString().padStart(10)} / ${platformTotal.toLocaleString().padStart(10)} (${percentage}%)`);
    }
});

// Helper function to find nested language fields
function findNestedLanguageFields(obj, path = "", maxDepth = 3) {
    const langFields = [];
    
    if (maxDepth <= 0 || typeof obj !== 'object' || obj === null) {
        return langFields;
    }
    
    for (const [key, value] of Object.entries(obj)) {
        const currentPath = path ? `${path}.${key}` : key;
        
        // Check if this key looks like a language field
        if (key.toLowerCase().includes('lang') || key.toLowerCase().includes('locale')) {
            langFields.push({
                path: currentPath,
                value: value
            });
        }
        
        // Recurse into nested objects
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
            langFields.push(...findNestedLanguageFields(value, currentPath, maxDepth - 1));
        } else if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') {
            // Check first item in arrays of objects
            langFields.push(...findNestedLanguageFields(value[0], `${currentPath}[0]`, maxDepth - 1));
        }
    }
    
    return langFields;
}

print("\n✅ Language field analysis complete!");
print("Run the Python test script for more detailed analysis.");
