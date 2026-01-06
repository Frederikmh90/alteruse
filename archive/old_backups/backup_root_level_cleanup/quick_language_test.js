// Quick Language Field Test
// Run with: mongosh your_database_name --file quick_language_test.js

print("🧪 QUICK LANGUAGE FIELD TEST");
print("Target languages: da, de, sv");
print("=" .repeat(50));

// Get available platforms
print("\n📊 Available Platforms:");
const platforms = db.actor_metric.distinct("platform");
print(`Found ${platforms.length} platforms: ${platforms.join(", ")}`);

// Quick test for each platform
print("\n🔍 LANGUAGE FIELD ANALYSIS BY PLATFORM:");
print("=" .repeat(50));

platforms.forEach(platform => {
    print(`\n📱 ${platform}:`);
    
    // Get total posts
    const total = db.post.countDocuments({platform: platform});
    if (total === 0) {
        print(`  ❌ No posts found`);
        return;
    }
    
    // Test language filtering
    const targetLangs = db.post.countDocuments({
        platform: platform, 
        lang: {$in: ["da", "de", "sv"]}
    });
    
    const percentage = total > 0 ? (targetLangs / total * 100).toFixed(1) : 0;
    
    print(`  📊 Total posts: ${total.toLocaleString()}`);
    print(`  🎯 Target langs (da/de/sv): ${targetLangs.toLocaleString()} (${percentage}%)`);
    
    // Show sample language values
    const sampleLangs = db.post.aggregate([
        {$match: {platform: platform}},
        {$group: {_id: "$lang", count: {$sum: 1}}},
        {$sort: {count: -1}},
        {$limit: 5}
    ]);
    
    print(`  📋 Top 5 language values:`);
    sampleLangs.forEach(doc => {
        const lang = doc._id || "null";
        const count = doc.count;
        const isTarget = ["da", "de", "sv"].includes(lang) || 
                        (lang && ["da", "de", "sv"].some(tl => lang.toLowerCase().startsWith(tl)));
        const marker = isTarget ? "🎯" : "  ";
        print(`    ${marker} ${lang}: ${count.toLocaleString()}`);
    });
});

// Overall summary
print("\n🌍 OVERALL SUMMARY:");
print("=" .repeat(50));

const totalPosts = db.post.countDocuments({});
const targetPosts = db.post.countDocuments({lang: {$in: ["da", "de", "sv"]}});
const regexPosts = db.post.countDocuments({lang: {$regex: "^(da|de|sv)", $options: "i"}});

print(`Total posts in database: ${totalPosts.toLocaleString()}`);
print(`Direct target language matches: ${targetPosts.toLocaleString()} (${(targetPosts/totalPosts*100).toFixed(1)}%)`);
print(`Regex target language matches: ${regexPosts.toLocaleString()} (${(regexPosts/totalPosts*100).toFixed(1)}%)`);

// Show overall top languages
print(`\n🔤 TOP 15 LANGUAGES OVERALL:`);
const overallLangs = db.post.aggregate([
    {$group: {_id: "$lang", count: {$sum: 1}}},
    {$sort: {count: -1}},
    {$limit: 15}
]);

overallLangs.forEach(doc => {
    const lang = doc._id || "null";
    const count = doc.count;
    const isTarget = ["da", "de", "sv"].includes(lang) || 
                    (lang && ["da", "de", "sv"].some(tl => lang.toLowerCase().startsWith(tl)));
    const marker = isTarget ? "🎯" : "  ";
    print(`  ${marker} ${lang}: ${count.toLocaleString()}`);
});

print("\n✅ Test complete!");
print("If you see very low percentages for target languages,");
print("we need to investigate the language field structure further.");
