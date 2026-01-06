# =========================
# CONFIG — edit these
# =========================
PLATFORMS = "auto"  # "auto" or comma list: "vkontakte,telegram,twitter"
LANG_FILTER = "all"  # "all" or a set like {"da","en","sv","de"}
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
# Enhanced Checkpoint System
# =========================
import os
import time
import random
import json
import pickle
from pathlib import Path
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
    # Common twitter2 fields used by Spread getters
    "id": 1,
    "lang": 1,
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
    "actor": 1,
    "owner_id": 1,
    "post_id": 1,
    "id": 1,
    "likes": 1,
    "comments": 1,
    "reposts": 1,
    # reddit
    "author_fullname": 1,
    "author": 1,
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
    "message": 1,
    "date": 1,
    # youtube
    "snippet": 1,
    "statistics": 1,
    "actor": 1,
    # tiktok
    "createTime": 1,
    "desc": 1,
    "video": 1,
    "stats": 1,
    # gab
    "created_at": 1,
    "body": 1,
    "url": 1,
    "followers_count": 1,
    # fourchan
    "num": 1,
    "thread_num": 1,
    "comment": 1,
    "name": 1,
    "timestamp": 1,
    "op": 1,
    # google / web
    "displayLink": 1,
    "link": 1,
    "pagemap": 1,
}


class CheckpointManager:
    """Handles saving and loading checkpoint state to survive crashes."""

    def __init__(self, output_dir: str, run_id: str = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if run_id is None:
            run_id = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        self.run_id = run_id

        self.checkpoint_file = self.output_dir / f"checkpoint_{self.run_id}.json"
        self.state_file = self.output_dir / f"state_{self.run_id}.pkl"

        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Load existing checkpoint state or create new one."""
        default_state = {
            "run_id": self.run_id,
            "start_time": time.time(),
            "platforms_completed": [],
            "current_platform": None,
            "current_platform_state": {
                "accounts_selected": False,
                "accounts": [],
                "post_ids_collected": False,
                "all_post_ids": [],
                "batches_processed": 0,
                "total_batches": 0,
                "rows_saved": 0,
                "chunk_files_saved": [],
            },
            "total_rows_across_platforms": 0,
            "platform_stats": {},
        }

        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    state = json.load(f)
                print(f"📁 Loaded checkpoint from {self.checkpoint_file}")
                print(
                    f"   - Platforms completed: {state.get('platforms_completed', [])}"
                )
                print(f"   - Current platform: {state.get('current_platform')}")
                if state.get("current_platform_state"):
                    cp_state = state["current_platform_state"]
                    print(
                        f"   - Batches processed: {cp_state.get('batches_processed', 0)}/{cp_state.get('total_batches', 0)}"
                    )
                    print(f"   - Rows saved: {cp_state.get('rows_saved', 0)}")
                return state
            except Exception as e:
                print(f"⚠️  Error loading checkpoint: {e}")
                print("   Starting fresh...")

        return default_state

    def save_checkpoint(self):
        """Save current state to checkpoint file."""
        self.state["last_save_time"] = time.time()

        # Save JSON checkpoint (human readable)
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.state, f, indent=2, default=str)

        # Save pickle state (for complex objects)
        with open(self.state_file, "wb") as f:
            pickle.dump(self.state, f)

    def mark_platform_completed(self, platform: str, stats: Dict[str, Any]):
        """Mark a platform as completed."""
        if platform not in self.state["platforms_completed"]:
            self.state["platforms_completed"].append(platform)
        self.state["platform_stats"][platform] = stats
        self.state["current_platform"] = None
        self.state["current_platform_state"] = {
            "accounts_selected": False,
            "accounts": [],
            "post_ids_collected": False,
            "all_post_ids": [],
            "batches_processed": 0,
            "total_batches": 0,
            "rows_saved": 0,
            "chunk_files_saved": [],
        }
        self.save_checkpoint()

    def start_platform(self, platform: str):
        """Start processing a platform."""
        self.state["current_platform"] = platform
        self.save_checkpoint()

    def save_accounts(self, accounts: List[str]):
        """Save selected accounts."""
        self.state["current_platform_state"]["accounts"] = accounts
        self.state["current_platform_state"]["accounts_selected"] = True
        self.save_checkpoint()

    def save_post_ids(self, post_ids: List[Any]):
        """Save collected post IDs."""
        self.state["current_platform_state"]["all_post_ids"] = [
            str(pid) for pid in post_ids
        ]  # Convert to strings for JSON
        self.state["current_platform_state"]["post_ids_collected"] = True
        self.save_checkpoint()

    def set_total_batches(self, total_batches: int):
        """Set total number of batches."""
        self.state["current_platform_state"]["total_batches"] = total_batches
        self.save_checkpoint()

    def mark_batch_completed(self, batch_num: int, rows_added: int):
        """Mark a batch as completed."""
        self.state["current_platform_state"]["batches_processed"] = batch_num + 1
        self.state["current_platform_state"]["rows_saved"] += rows_added
        self.save_checkpoint()

    def add_chunk_file(self, filename: str, row_count: int):
        """Record a chunk file that was saved."""
        self.state["current_platform_state"]["chunk_files_saved"].append(
            {"filename": filename, "row_count": row_count, "timestamp": time.time()}
        )
        self.save_checkpoint()

    def get_resume_info(self) -> Tuple[List[str], str, Dict[str, Any]]:
        """Get information needed to resume processing."""
        platforms_completed = self.state.get("platforms_completed", [])
        current_platform = self.state.get("current_platform")
        current_state = self.state.get("current_platform_state", {})
        return platforms_completed, current_platform, current_state

    def cleanup(self):
        """Clean up checkpoint files after successful completion."""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
            if self.state_file.exists():
                self.state_file.unlink()
            print(f"✅ Cleaned up checkpoint files")
        except Exception as e:
            print(f"⚠️  Error cleaning up checkpoint files: {e}")


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


# ---------- core transform ----------
def _rows_from_posts(
    posts: List[Dict[str, Any]], lang_filter: Set[str] | str = "all"
) -> Tuple[list, int]:
    """
    Build rows using Spread getters, but only enforce the minimal required fields.
    Optional fields failing do NOT drop the row; we record the failure and set None.
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

        # 4) language filter (only drop if you set a filter and lang is not in it)
        lang_val = opt_values.get("lang")
        if lang_filter != "all":
            code = (lang_val or "").split("-")[0].lower()
            if code not in lang_filter:
                continue

        # 5) build row
        rows.append(
            {
                "actor_id": opt_values.get("actor_id"),
                "actor_username": req_values["actor_username"],
                "actor_name": opt_values.get("actor_name"),
                "platform": req_values["platform"],
                "lang": lang_val,
                "datetime": req_values["datetime"],
                "message_id": req_values["message_id"],
                "post_url": opt_values.get("post_url"),
                "link_to_actor": opt_values.get("link_to_actor"),
                "text": text_val,
            }
        )

    return rows, skipped


# ---------- fetch helpers ----------
def _fetch_post_docs_by_ids(db, post_ids: List[Any]):
    if not post_ids:
        return []
    return list(db.post.find({"_id": {"$in": post_ids}}, POST_PROJECTION_MIN))


def _pick_accounts_actor_metric(db, platform: str, target_n: int) -> List[str]:
    """
    Pick ~target_n actor_usernames for this platform (exact string match).
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
):
    """
    actor_platform_post is keyed by exact strings for platform.
    """
    app = db.actor_platform_post.find_one(
        {"actor_username": actor_username, "platform": platform},
        {"post_obj_ids": {"$slice": posts_per_actor_soft}},
    )
    ids = (app or {}).get("post_obj_ids") or []
    if len(ids) < min_posts_per_actor:
        return []

    if post_percentage < 100.0:
        k = max(1, int(len(ids) * (post_percentage / 100.0)))
    else:
        k = len(ids)
    if max_posts_per_actor:
        k = min(k, max_posts_per_actor)

    if k >= len(ids):
        return ids
    return random.Random(RANDOM_SEED).sample(ids, k)


# --- parallel batch worker ---
def _fetch_and_transform_batch(db, batch_ids, lang_filter):
    docs = _fetch_post_docs_by_ids(db, batch_ids)
    return _rows_from_posts(docs, lang_filter=lang_filter)


# ---------- per-platform (ENHANCED WITH CHECKPOINTS) ----------
def sample_one_platform(
    db,
    platform: str,  # exact string like "twitter", "facebook", ...
    account_percentage: float,
    post_percentage: float,
    min_posts_per_actor: int,
    max_posts_per_actor: int | None,
    posts_per_actor_soft: int,
    lang_filter: Set[str] | str,
    batch_fetch_size: int,
    chunk_save_size: int,
    output_dir: str,
    checkpoint_mgr: CheckpointManager,
):
    os.makedirs(output_dir, exist_ok=True)
    t0 = time.time()
    print(f"\n=== {platform.upper()} ===")

    # Check if this platform should be resumed
    platforms_completed, current_platform, current_state = (
        checkpoint_mgr.get_resume_info()
    )

    if platform in platforms_completed:
        print(f"[{platform}] ✅ Already completed, skipping...")
        return pd.DataFrame(columns=DERIVED_COLUMNS), checkpoint_mgr.state[
            "platform_stats"
        ].get(platform, {})

    # Mark platform as started
    checkpoint_mgr.start_platform(platform)

    # --- account selection (with resume) ---
    if current_platform == platform and current_state.get("accounts_selected"):
        accounts = current_state["accounts"]
        print(
            f"[{platform}] 📁 Resumed with {len(accounts)} previously selected actors"
        )
    else:
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
        checkpoint_mgr.save_accounts(accounts)
        t_sel1 = time.time()
        print(
            f"[{platform}] picked {len(accounts)} actors in {t_sel1 - t_sel0:.1f}s (total actors ~ {total_actors:,}, target={target_accounts:,})"
        )

    if not accounts:
        print(f"[{platform}] no actors found")
        stats = {"actors": 0, "ids": 0, "rows": 0, "skipped": 0, "elapsed": 0.0}
        checkpoint_mgr.mark_platform_completed(platform, stats)
        return pd.DataFrame(columns=DERIVED_COLUMNS), stats

    # --- collect ids (with resume) ---
    if current_platform == platform and current_state.get("post_ids_collected"):
        all_ids = [
            pid for pid in current_state["all_post_ids"]
        ]  # Convert back from strings
        print(
            f"[{platform}] 📁 Resumed with {len(all_ids)} previously collected post IDs"
        )
    else:
        t_ids0 = time.time()
        all_ids = []
        for a in tqdm(accounts, desc=f"[{platform}] collecting post ids"):
            ids = _collect_actor_post_ids(
                db,
                a,
                platform=platform,
                min_posts_per_actor=min_posts_per_actor,
                posts_per_actor_soft=posts_per_actor_soft,
                post_percentage=post_percentage,
                max_posts_per_actor=max_posts_per_actor,
            )
            all_ids.extend(ids)
        all_ids = list(dict.fromkeys(all_ids))
        checkpoint_mgr.save_post_ids(all_ids)
        t_ids1 = time.time()
        print(
            f"[{platform}] unique post ids to fetch: {len(all_ids)} (collected in {t_ids1 - t_ids0:.1f}s)"
        )

    # --- parallel fetch+transform (with resume) ---
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
        checkpoint_mgr.set_total_batches(len(batches))

        # Resume from where we left off
        start_batch = (
            current_state.get("batches_processed", 0)
            if current_platform == platform
            else 0
        )
        if start_batch > 0:
            print(f"[{platform}] 📁 Resuming from batch {start_batch}/{len(batches)}")

        # Load existing chunk files if resuming
        existing_chunk_files = []
        if current_platform == platform and current_state.get("chunk_files_saved"):
            for chunk_info in current_state["chunk_files_saved"]:
                chunk_file = os.path.join(output_dir, chunk_info["filename"])
                if os.path.exists(chunk_file):
                    try:
                        chunk_df = pd.read_csv(chunk_file)
                        existing_chunk_files.append(chunk_df)
                        print(
                            f"[{platform}] 📁 Loaded existing chunk: {chunk_info['filename']} ({chunk_info['row_count']} rows)"
                        )
                    except Exception as e:
                        print(
                            f"[{platform}] ⚠️  Error loading chunk {chunk_info['filename']}: {e}"
                        )

        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            # Only process remaining batches
            remaining_batches = batches[start_batch:]
            futures = [
                ex.submit(_fetch_and_transform_batch, db, b, lang_filter)
                for b in remaining_batches
            ]

            for i, fut in enumerate(
                tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=f"[{platform}] fetch+transform (parallel)",
                )
            ):
                actual_batch_num = start_batch + i
                rows, skipped = fut.result()
                skipped_total += skipped
                rows_all.extend(rows)

                # Mark batch as completed
                checkpoint_mgr.mark_batch_completed(actual_batch_num, len(rows))

                if chunk_save_size and len(rows_all) >= chunk_save_size:
                    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                    chunk_filename = (
                        f"{platform}_sample_autosave_{len(rows_all)}_{ts}.csv"
                    )
                    chunk_path = os.path.join(output_dir, chunk_filename)
                    pd.DataFrame(rows_all).to_csv(chunk_path, index=False)
                    print(
                        f"[{platform}] [autosave] {len(rows_all)} rows → {chunk_filename}"
                    )

                    # Record this chunk file
                    checkpoint_mgr.add_chunk_file(chunk_filename, len(rows_all))
                    dfs.append(pd.DataFrame(rows_all))
                    rows_all.clear()

        # Combine all data
        if rows_all:
            dfs.append(pd.DataFrame(rows_all))

        # Add existing chunk files
        all_dfs = existing_chunk_files + dfs
        df_platform = (
            pd.concat(all_dfs, ignore_index=True)
            if all_dfs
            else pd.DataFrame(columns=DERIVED_COLUMNS)
        )

    t_ft1 = time.time()

    # --- save final & stats ---
    if not df_platform.empty:
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        fn = os.path.join(
            output_dir, f"{platform}_sample_complete_{len(df_platform)}_{ts}.csv"
        )
        df_platform.to_csv(fn, index=False)
        print(f"[{platform}] saved → {fn}")

    elapsed = time.time() - t0
    print(
        f"[{platform}] rows={len(df_platform)} skipped={skipped_total} "
        f"fetch+transform={t_ft1 - t_ft0:.1f}s total={elapsed:.1f}s"
    )

    stats = {
        "actors": len(accounts),
        "ids": len(all_ids),
        "rows": len(df_platform),
        "skipped": skipped_total,
        "elapsed": elapsed,
    }

    # Mark platform as completed
    checkpoint_mgr.mark_platform_completed(platform, stats)

    return df_platform, stats


# ---------- multi-platform driver (ENHANCED WITH CHECKPOINTS) ----------
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
    # Initialize checkpoint manager
    checkpoint_mgr = CheckpointManager(output_dir)

    # normalize lang filter
    if isinstance(lang_filter, str) and lang_filter != "all":
        lang_filter = {lang_filter.lower()}
    elif isinstance(lang_filter, set):
        lang_filter = {x.lower() for x in lang_filter}

    random.seed(RANDOM_SEED)
    client, db = _client_and_db()

    platforms_list = _normalize_platforms(db, platforms)

    print("CONFIG")
    print(f"  platforms: {platforms_list}")
    print(
        f"  langs:     {lang_filter if lang_filter == 'all' else sorted(lang_filter)}"
    )
    print(f"  min/max posts per actor: {min_posts_per_actor}/{max_posts_per_actor}")
    print(f"  account% / post%: {account_percentage}% / {post_percentage}%")
    print(f"  slice per actor: {posts_per_actor_soft}")
    print(f"  max actors per platform: {MAX_ACTORS_PER_PLATFORM}")
    print(
        f"  batch fetch: {batch_fetch_size}, workers: {FETCH_WORKERS}, autosave: {chunk_save_size}, out: {output_dir}"
    )
    print(f"  checkpoint: {checkpoint_mgr.checkpoint_file}")

    combined, all_stats = [], {}

    # Check for resume
    platforms_completed, current_platform, current_state = (
        checkpoint_mgr.get_resume_info()
    )
    if platforms_completed or current_platform:
        print(f"\n🔄 RESUMING from checkpoint:")
        print(f"   - Completed platforms: {platforms_completed}")
        print(f"   - Current platform: {current_platform}")

    for plat in platforms_list:
        try:
            df_p, stats = sample_one_platform(
                db=db,
                platform=plat,  # exact string
                account_percentage=account_percentage,
                post_percentage=post_percentage,
                min_posts_per_actor=min_posts_per_actor,
                max_posts_per_actor=max_posts_per_actor,
                posts_per_actor_soft=posts_per_actor_soft,
                lang_filter=lang_filter,
                batch_fetch_size=batch_fetch_size,
                chunk_save_size=chunk_save_size,
                output_dir=output_dir,
                checkpoint_mgr=checkpoint_mgr,
            )
            all_stats[plat] = stats
            if not df_p.empty:
                combined.append(df_p)
        except Exception as e:
            print(f"❌ Error processing platform {plat}: {e}")
            print(f"   Checkpoint saved. You can resume by running the script again.")
            raise

    if combined:
        df_all = pd.concat(combined, ignore_index=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        fn_all = os.path.join(
            output_dir, f"ALL_platforms_sample_{len(df_all)}_{ts}.csv"
        )
        df_all.to_csv(fn_all, index=False)
        print(f"\n[ALL] saved combined CSV → {fn_all}")
    else:
        df_all = pd.DataFrame(columns=DERIVED_COLUMNS)
        print("\n[ALL] no rows collected")

    print("\n===== OVERALL SUMMARY =====")
    for plat, s in all_stats.items():
        print(
            f"{plat:15} actors={s['actors']:7} ids={s['ids']:9} rows={s['rows']:9} skipped={s['skipped']:7} elapsed={s['elapsed']:.1f}s"
        )
    print(f"TOTAL rows: {len(df_all)}")

    # Clean up checkpoint files on successful completion
    checkpoint_mgr.cleanup()

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
