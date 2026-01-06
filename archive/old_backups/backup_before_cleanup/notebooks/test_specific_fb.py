from notebooks.Processing_facebook_batch_analysis import analyze_account_activity

# Test the specific directory mentioned by the user
fb_dir = "/Users/Codebase/projects/alteruse/data/Kantar_download_398_unzipped_new/474-4477-c-146189_2025-05-02T14__4477g1746194515336sE6EzyMOWskju5307uu5307ufacebookjepperoege02052025x4jfH67T-dbBSdwf"

print(f"Testing Facebook analysis on: {fb_dir}")
result = analyze_account_activity(fb_dir)

if result:
    print(f"Account: {result['account_name']}")
    print(f"Earliest activity: {result['earliest_activity']}")
    print(f"Latest activity: {result['latest_activity']}")
    print(f"Activity days: {result['activity_days']}")
    print(f"Valid timestamps: {result['valid_timestamps']}")
    print(f"Total URLs shared: {result['total_urls_shared']}")
    print(f"Mainstream news shared: {result['mainstream_news_shared']}")
    print(f"Alternative news shared: {result['alternative_news_shared']}")
else:
    print("No valid result returned")
