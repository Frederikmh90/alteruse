# =========================
# CONFIG — edit these
# =========================
PLATFORMS = "auto"  # "auto" or comma list: "vkontakte,telegram,twitter"
LANG_FILTER = ["da", "de", "sv"]  # CHANGED: Now filters at MongoDB level
ACCOUNT_PERCENTAGE = 80  # percent of actors per platform to sample (e.g., 0.5 = 0.5%)
POST_PERCENTAGE = 80  # percent of (sliced) post ids per actor
MIN_POSTS_PER_ACTOR = 30  # skip actors with fewer than this many post ids
MAX_POSTS_PER_ACTOR = None  # hard cap per actor (None for unlimited)
POSTS_PER_ACTOR_SOFT = 400  # slice size of post_obj_ids per actor before % sampling

MAX_ACTORS_PER_PLATFORM = (
    None  # cap number of actors sampled per platform (None for no cap)
)

BATCH_FETCH_SIZE = 1500  # fewer round trips; 1500–3000 is typical
FETCH_WORKERS = 7  # parallel batches (4–8 is usually safe)
CHUNK_SAVE_SIZE = 200_000  # autosave every N rows (0/None to disable)

OUTPUT_DIR = "./data/technocracy_250810"
RANDOM_SEED = 42

# =========================
# Enhanced Implementation with MongoDB Language Filtering
# =========================
import os
import time
import random
from typing import Iterable, List, Dict, Any, Set, Tuple
import pandas as pd
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

from spreadAnalysis.persistence.mongo import MongoSpread
from spreadAnalysis.persistence.schemas import Spread

# Columns we output (order)
DERIVED_COLUMNS = [
    "actor_id",
    "actor_username",
    "actor_name",
    "platform",
    "lang",
    "datetime",
    "message_id",
    "post_url",
    "link_to_actor",
    "text",
]

# ------- Mongo projection (MUST cover every field the getters might touch) -------
POST_PROJECTION_MIN = {
    "_id": 1,
    "method": 1,
    "platform": 1,
    "text": 1,
    "lang": 1,  # IMPORTANT: Always include lang field
    # Common twitter2 fields used by Spread getters
    "id": 1,
    "source": 1,
    "conversation_id": 1,
    "author": 1,
    "entities": 1,
    "public_metrics": 1,
    "attachments": 1,
    "referenced_tweets": 1,
    "created_at": 1,
    # crowdtangle / crowdtangle_app
    "account": 1,
    "post_url": 1,
    "postUrl": 1,
    "platformId": 1,
    "type_id": 1,
    "name": 1,
    "post_date": 1,
    "statistics": 1,
    "date": 1,
    "message": 1,
    "title": 1,
    "caption": 1,
    "description": 1,
    # facebook_browser
    "user_id": 1,
    # vkontakte
    "owner_id": 1,
    "post_id": 1,
    "likes": 1,
    "comments": 1,
    "reposts": 1,
    # reddit
    "author_fullname": 1,
    "created_utc": 1,
    "selftext": 1,
    "body": 1,
    "num_comments": 1,
    "num_crossposts": 1,
    "score": 1,
    "permalink": 1,
    "full_link": 1,
    # telegram
    "from_username": 1,
    "peer_id": 1,
    "media": 1,
    "views": 1,
    "forwards": 1,
    "replies": 1,
    # youtube
    "snippet": 1,
    "actor": 1,
    # tiktok
    "createTime": 1,
    "desc": 1,
    "video": 1,
    "stats": 1,
    # gab
    "created_at": 1,
    "url": 1,
    "followers_count": 1,
    # fourchan
    "num": 1,
    "thread_num": 1,
    "comment": 1,
    "timestamp": 1,
    "op": 1,
    # google / web
    "displayLink": 1,
    "link": 1,
    "pagemap": 1,
}


# ---------- connections ----------
def _client_and_db():
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=4000,
        socketTimeoutMS=300000,  # allow slow first batch
        connectTimeoutMS=8000,
        maxPoolSize=64,  # allow parallelism
    )
    return client, client[name]


# ---------- normalize platforms ----------
def _normalize_platforms(db, platforms):
    """
    - If 'auto': detect distinct platforms in actor_metric (exact strings).
    - If comma string: split and trim.
    - If iterable: return list.
    """
    if platforms is None:
        return list(db.actor_metric.distinct("platform"))

    if isinstance(platforms, str):
        s = platforms.strip()
        if s.lower() == "auto":
            # exact strings from actor_metric
            vals = list(db.actor_metric.distinct("platform"))
            # keep stable order
            return vals
        parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
        return parts if parts else []

    return list(platforms)


# --- helper to call a Spread getter safely ---
def _safe_get(getter, method, data):
    try:
        return getter(method=method, data=data)
    except Exception as e:
        return e  # return the exception object so we can count it without crashing


# ---------- Enhanced language filter parsing ----------
def _parse_language_filter(lang_filter):
    """Convert language filter config to list format for MongoDB queries."""
    if lang_filter == "all":
        return None

    if isinstance(lang_filter, str) and lang_filter != "all":
        return [lang_filter.lower()]

    if isinstance(lang_filter, (set, list)):
        return [x.lower() for x in lang_filter]

    return None


# ---------- core transform (SIMPLIFIED - no Python-level language filtering) ----------
def _rows_from_posts(
    posts: List[Dict[str, Any]], lang_filter: Set[str] | str = "all"
) -> Tuple[list, int]:
    """
    Build rows using Spread getters.
    LANGUAGE FILTERING REMOVED - now done at MongoDB level for efficiency!
    """
    rows, skipped = [], 0

    REQUIRED = {
        "actor_username": Spread._get_actor_username,
        "platform": Spread._get_platform,
        "datetime": Spread._get_date,
        "message_id": Spread._get_message_id,
    }
    OPTIONAL = {
        "actor_id": Spread._get_actor_id,
        "actor_name": Spread._get_actor_name,
        "lang": Spread._get_lang,
        "post_url": Spread._get_post_url,
        "link_to_actor": Spread._get_link_to_actor,
    }

    # expose counters on the function for diagnostics
    if not hasattr(_rows_from_posts, "fail_counts"):
        _rows_from_posts.fail_counts = {}
    fail_counts = _rows_from_posts.fail_counts  # type: ignore[attr-defined]

    for p in posts:
        m = p.get("method")

        # 1) required fields
        req_values = {}
        drop = False
        for key, getter in REQUIRED.items():
            val = _safe_get(getter, m, p)
            if isinstance(val, Exception) or val in (None, ""):
                fail_counts[key] = fail_counts.get(key, 0) + 1
                drop = True
                break
            req_values[key] = val
        if drop:
            skipped += 1
            continue

        # 2) optional fields (never drop the row)
        opt_values = {}
        for key, getter in OPTIONAL.items():
            val = _safe_get(getter, m, p)
            if isinstance(val, Exception):
                fail_counts[key] = fail_counts.get(key, 0) + 1
                opt_values[key] = None
            else:
                opt_values[key] = val

        # 3) message text (use Spread first; fall back to raw per platform if missing)
        text_val = _safe_get(Spread._get_message_text, m, p)
        if isinstance(text_val, Exception) or text_val in (None, ""):
            text_val = p.get("text")  # generic fallback
            if not text_val:
                # platform-specific raw fallbacks (non-fatal)
                meth = (p.get("platform") or p.get("method") or "").lower()
                if meth.startswith("twitter"):
                    text_val = p.get("text")
                elif meth == "gab":
                    text_val = p.get("body")
                elif meth == "fourchan":
                    text_val = p.get("comment")
                elif meth == "reddit":
                    text_val = p.get("selftext") or p.get("body")
                elif meth == "facebook":
                    text_val = p.get("message")
                elif meth == "telegram":
                    text_val = p.get("message")

        # 4) LANGUAGE FILTERING REMOVED - done at MongoDB level now!
        # This significantly improves performance for billion-post databases

        # 5) build row
        rows.append(
            {
                "actor_id": opt_values.get("actor_id"),
                "actor_username": req_values["actor_username"],
                "actor_name": opt_values.get("actor_name"),
                "platform": req_values["platform"],
                "lang": opt_values.get("lang"),  # Will be pre-filtered
                "datetime": req_values["datetime"],
                "message_id": req_values["message_id"],
                "post_url": opt_values.get("post_url"),
                "link_to_actor": opt_values.get("link_to_actor"),
                "text": text_val,
            }
        )

    return rows, skipped


# ---------- Enhanced fetch helpers with language filtering ----------
def _fetch_post_docs_by_ids(
    db, post_ids: List[Any], target_languages: List[str] = None
):
    """ENHANCED: Now includes language filtering at MongoDB level."""
    if not post_ids:
        return []

    # Build query with language filter
    query = {"_id": {"$in": post_ids}}

    # Add language filter if specified
    if target_languages:
        query["lang"] = {"$in": target_languages}

    return list(db.post.find(query, POST_PROJECTION_MIN))


def _pick_accounts_actor_metric(db, platform: str, target_n: int) -> List[str]:
    """
    Pick ~target_n actor_usernames for this platform (exact string match).
    UNCHANGED from original.
    """
    # sample via $sample
    try:
        docs = list(
            db.actor_metric.aggregate(
                [
                    {"$match": {"platform": platform, "actor_username": {"$ne": None}}},
                    {"$sample": {"size": target_n}},
                    {"$project": {"_id": 0, "actor_username": 1}},
                ],
                allowDiskUse=True,
            )
        )
        usernames = [d["actor_username"] for d in docs if d.get("actor_username")]
        seen, out = set(), []
        for u in usernames:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out
    except Exception:
        pass

    # fallback: window+skip
    rng = random.Random(RANDOM_SEED)
    usernames, need, window = [], target_n, 2000
    while need > 0:
        skip = rng.randint(0, max(0, window - 1))
        cursor = (
            db.actor_metric.find(
                {"platform": platform, "actor_username": {"$ne": None}},
                {"_id": 0, "actor_username": 1},
            )
            .skip(skip)
            .limit(min(need, 2000))
        )
        batch = [d["actor_username"] for d in cursor if d.get("actor_username")]
        if not batch:
            break
        usernames.extend(batch)
        need -= len(batch)
    seen, out = set(), []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:target_n]


def _collect_actor_post_ids(
    db,
    actor_username: str,
    platform: str,
    min_posts_per_actor: int,
    posts_per_actor_soft: int,
    post_percentage: float,
    max_posts_per_actor: int | None,
    target_languages: List[str] = None,  # NEW parameter
):
    """
    ENHANCED: Now filters by language at MongoDB level.
    Only collects post IDs for posts that match target languages.
    """
    app = db.actor_platform_post.find_one(
        {"actor_username": actor_username, "platform": platform},
        {"post_obj_ids": {"$slice": posts_per_actor_soft}},
    )
    ids = (app or {}).get("post_obj_ids") or []

    if len(ids) < min_posts_per_actor:
        return []

    # NEW: If we have target languages, filter the post IDs by language
    if target_languages:
        # Query the actual posts to see which ones match our language criteria
        language_query = {"_id": {"$in": ids}, "lang": {"$in": target_languages}}

        # Get only the IDs of posts that match language criteria
        matching_posts = list(db.post.find(language_query, {"_id": 1}))

        # Extract just the IDs
        ids = [post["_id"] for post in matching_posts]

        # Check if we still have enough posts after language filtering
        if len(ids) < min_posts_per_actor:
            return []

    # Apply sampling logic (same as before)
    if post_percentage < 100.0:
        k = max(1, int(len(ids) * (post_percentage / 100.0)))
    else:
        k = len(ids)
    if max_posts_per_actor:
        k = min(k, max_posts_per_actor)

    if k >= len(ids):
        return ids
    return random.Random(RANDOM_SEED).sample(ids, k)


# --- Enhanced parallel batch worker ---
def _fetch_and_transform_batch(db, batch_ids, lang_filter, target_languages=None):
    """ENHANCED: Passes target_languages to fetch function."""
    docs = _fetch_post_docs_by_ids(db, batch_ids, target_languages)
    return _rows_from_posts(docs, lang_filter=lang_filter)


# ---------- Enhanced per-platform with language filtering ----------
def sample_one_platform(
    db,
    platform: str,  # exact string like "twitter", "facebook", ...
    account_percentage: float,
    post_percentage: float,
    min_posts_per_actor: int,
    max_posts_per_actor: int | None,
    posts_per_actor_soft: int,
    lang_filter,  # Can be "all", string, set, or list
    batch_fetch_size: int,
    chunk_save_size: int,
    output_dir: str,
):
    """ENHANCED: Now filters by language at MongoDB level for massive performance improvement."""
    os.makedirs(output_dir, exist_ok=True)
    t0 = time.time()
    print(f"\n=== {platform.upper()} ===")

    # Parse language filter
    target_languages = _parse_language_filter(lang_filter)
    if target_languages:
        print(f"🎯 Language filtering: {target_languages}")
    else:
        print("📊 No language filtering (collecting all languages)")

    # --- account selection ---
    t_sel0 = time.time()
    try:
        total_actors = db.actor_metric.count_documents(
            {"platform": platform, "actor_username": {"$ne": None}}
        )
    except Exception:
        total_actors = 1000

    target_accounts = max(1000, int(total_actors * (account_percentage / 100.0)))
    if MAX_ACTORS_PER_PLATFORM is not None:
        target_accounts = min(target_accounts, MAX_ACTORS_PER_PLATFORM)

    accounts = _pick_accounts_actor_metric(db, platform, target_accounts)
    t_sel1 = time.time()
    print(
        f"[{platform}] picked {len(accounts)} actors in {t_sel1 - t_sel0:.1f}s (total actors ~ {total_actors:,}, target={target_accounts:,})"
    )
    if not accounts:
        print(f"[{platform}] no actors found")
        return pd.DataFrame(columns=DERIVED_COLUMNS), {
            "actors": 0,
            "ids": 0,
            "rows": 0,
            "skipped": 0,
            "elapsed": 0.0,
        }

    # --- Enhanced collect ids with language filtering ---
    t_ids0 = time.time()
    all_ids = []
    actors_with_target_posts = 0

    for a in tqdm(accounts, desc=f"[{platform}] collecting language-filtered post ids"):
        ids = _collect_actor_post_ids(
            db,
            a,
            platform=platform,
            min_posts_per_actor=min_posts_per_actor,
            posts_per_actor_soft=posts_per_actor_soft,
            post_percentage=post_percentage,
            max_posts_per_actor=max_posts_per_actor,
            target_languages=target_languages,  # NEW: Pass language filter
        )
        if ids:
            all_ids.extend(ids)
            actors_with_target_posts += 1

    all_ids = list(dict.fromkeys(all_ids))
    t_ids1 = time.time()

    if target_languages:
        print(
            f"[{platform}] actors with target language posts: {actors_with_target_posts}/{len(accounts)}"
        )
        print(
            f"[{platform}] unique language-filtered post ids: {len(all_ids)} (collected in {t_ids1 - t_ids0:.1f}s)"
        )
    else:
        print(
            f"[{platform}] unique post ids to fetch: {len(all_ids)} (collected in {t_ids1 - t_ids0:.1f}s)"
        )

    # --- Enhanced parallel fetch+transform ---
    t_ft0 = time.time()
    rows_all, skipped_total = [], 0
    dfs = []

    if not all_ids:
        df_platform = pd.DataFrame(columns=DERIVED_COLUMNS)
    else:
        batches = [
            all_ids[i : i + batch_fetch_size]
            for i in range(0, len(all_ids), batch_fetch_size)
        ]
        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            futures = [
                ex.submit(
                    _fetch_and_transform_batch, db, b, lang_filter, target_languages
                )
                for b in batches
            ]
            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"[{platform}] fetch+transform (parallel)",
            ):
                rows, skipped = fut.result()
                skipped_total += skipped
                rows_all.extend(rows)
                if chunk_save_size and len(rows_all) >= chunk_save_size:
                    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                    fn = os.path.join(
                        output_dir,
                        f"{platform}_sample_autosave_{len(rows_all)}_{ts}.csv",
                    )
                    pd.DataFrame(rows_all).to_csv(fn, index=False)
                    print(f"[{platform}] [autosave] {len(rows_all)} rows → {fn}")
                    dfs.append(pd.DataFrame(rows_all))
                    rows_all.clear()

        if rows_all:
            dfs.append(pd.DataFrame(rows_all))
        df_platform = (
            pd.concat(dfs, ignore_index=True)
            if dfs
            else pd.DataFrame(columns=DERIVED_COLUMNS)
        )

    t_ft1 = time.time()

    # --- save & stats ---
    if not df_platform.empty:
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        lang_suffix = f"_{'_'.join(target_languages)}" if target_languages else ""
        fn = os.path.join(
            output_dir,
            f"{platform}_sample_complete{lang_suffix}_{len(df_platform)}_{ts}.csv",
        )
        df_platform.to_csv(fn, index=False)
        print(f"[{platform}] saved → {fn}")

    elapsed = time.time() - t0
    print(
        f"[{platform}] rows={len(df_platform)} skipped={skipped_total} "
        f"select={t_sel1 - t_sel0:.1f}s ids={t_ids1 - t_ids0:.1f}s fetch+transform={t_ft1 - t_ft0:.1f}s total={elapsed:.1f}s"
    )

    if target_languages:
        print(
            f"[{platform}] ✅ Language filtering applied at MongoDB level - much more efficient!"
        )

    stats = {
        "actors": len(accounts),
        "actors_with_target_posts": actors_with_target_posts,
        "ids": len(all_ids),
        "rows": len(df_platform),
        "skipped": skipped_total,
        "elapsed": elapsed,
    }
    return df_platform, stats


# ---------- multi-platform driver (ENHANCED) ----------
def sample_platforms(
    platforms,
    account_percentage,
    post_percentage,
    min_posts_per_actor,
    max_posts_per_actor,
    posts_per_actor_soft,
    lang_filter,
    batch_fetch_size,
    chunk_save_size,
    output_dir,
):
    # Parse and display language filter
    target_languages = _parse_language_filter(lang_filter)

    random.seed(RANDOM_SEED)
    client, db = _client_and_db()

    platforms_list = _normalize_platforms(db, platforms)

    print("CONFIG")
    print(f"  platforms: {platforms_list}")
    if target_languages:
        print(f"  🎯 LANGUAGE FILTER: {target_languages} (MongoDB level)")
    else:
        print(f"  📊 LANGUAGE FILTER: disabled (all languages)")
    print(f"  min/max posts per actor: {min_posts_per_actor}/{max_posts_per_actor}")
    print(f"  account% / post%: {account_percentage}% / {post_percentage}%")
    print(f"  slice per actor: {posts_per_actor_soft}")
    print(f"  max actors per platform: {MAX_ACTORS_PER_PLATFORM}")
    print(
        f"  batch fetch: {batch_fetch_size}, workers: {FETCH_WORKERS}, autosave: {chunk_save_size}, out: {output_dir}"
    )

    combined, all_stats = [], {}
    for plat in platforms_list:
        df_p, stats = sample_one_platform(
            db=db,
            platform=plat,  # exact string
            account_percentage=account_percentage,
            post_percentage=post_percentage,
            min_posts_per_actor=min_posts_per_actor,
            max_posts_per_actor=max_posts_per_actor,
            posts_per_actor_soft=posts_per_actor_soft,
            lang_filter=lang_filter,  # Pass original config
            batch_fetch_size=batch_fetch_size,
            chunk_save_size=chunk_save_size,
            output_dir=output_dir,
        )
        all_stats[plat] = stats
        if not df_p.empty:
            combined.append(df_p)

    if combined:
        df_all = pd.concat(combined, ignore_index=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        lang_suffix = f"_{'_'.join(target_languages)}" if target_languages else ""
        fn_all = os.path.join(
            output_dir, f"ALL_platforms_sample{lang_suffix}_{len(df_all)}_{ts}.csv"
        )
        df_all.to_csv(fn_all, index=False)
        print(f"\n[ALL] saved combined CSV → {fn_all}")
    else:
        df_all = pd.DataFrame(columns=DERIVED_COLUMNS)
        print("\n[ALL] no rows collected")

    print("\n===== OVERALL SUMMARY =====")
    for plat, s in all_stats.items():
        actors_with_posts = s.get("actors_with_target_posts", s["actors"])
        print(
            f"{plat:15} actors={s['actors']:7} w/posts={actors_with_posts:7} ids={s['ids']:9} rows={s['rows']:9} skipped={s['skipped']:7} elapsed={s['elapsed']:.1f}s"
        )
    print(f"TOTAL rows: {len(df_all)}")

    if target_languages:
        print(f"🎯 Language filter applied: {target_languages}")
        print("✅ Filtering done at MongoDB level for maximum efficiency!")

    client.close()
    return df_all


# ============ Run with CONFIG ============
if __name__ == "__main__":
    df_all = sample_platforms(
        platforms=PLATFORMS,
        account_percentage=ACCOUNT_PERCENTAGE,
        post_percentage=POST_PERCENTAGE,
        min_posts_per_actor=MIN_POSTS_PER_ACTOR,
        max_posts_per_actor=MAX_POSTS_PER_ACTOR,
        posts_per_actor_soft=POSTS_PER_ACTOR_SOFT,
        lang_filter=LANG_FILTER,
        batch_fetch_size=BATCH_FETCH_SIZE,
        chunk_save_size=CHUNK_SAVE_SIZE,
        output_dir=OUTPUT_DIR,
    )
    try:
        print(df_all.head())
    except Exception:
        pass
