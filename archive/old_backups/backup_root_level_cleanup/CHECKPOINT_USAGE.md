# Robust Checkpoint System for MongoDB Sampler

This enhanced version of your MongoDB sampler includes a comprehensive checkpoint system that can survive kernel crashes and resume exactly where it left off.

## 🔄 Key Features

- **Automatic Checkpointing**: Saves state after every significant step
- **Crash Recovery**: Survives kernel crashes, memory issues, connection drops
- **Granular Resume**: Resumes at the exact batch level, not just platform level
- **Chunk File Management**: Automatically loads existing chunk files when resuming
- **Progress Tracking**: Detailed logging of what's been completed

## 📁 How It Works

The checkpoint system creates two files per run:
- `checkpoint_YYYYMMDD_HHMMSS.json` - Human-readable state
- `state_YYYYMMDD_HHMMSS.pkl` - Complete Python objects

These files track:
- ✅ Completed platforms
- 🔄 Current platform progress
- 📊 Selected actors list
- 🆔 Collected post IDs
- 📦 Processed batches
- 💾 Saved chunk files

## 🚀 Usage

### 1. Run Your Script Normally
```bash
python enhanced_mongo_sampler_with_checkpoints.py
```

The script will run normally, creating checkpoints automatically.

### 2. If It Crashes/Gets Killed
Simply run the same command again:
```bash
python enhanced_mongo_sampler_with_checkpoints.py
```

The script will:
- 🔍 Detect existing checkpoints
- 📋 Show what's already completed
- ⏭️ Skip completed platforms  
- 🔄 Resume current platform from exact batch
- 📂 Load existing chunk files
- ▶️ Continue processing

### 3. Monitor Progress
Use the checkpoint manager utility:

```bash
# List all checkpoints
python checkpoint_manager.py list

# Show detailed status of latest checkpoint  
python checkpoint_manager.py status

# Show specific checkpoint status
python checkpoint_manager.py status 20250810_143022

# Show how to resume
python checkpoint_manager.py resume
```

### 4. Clean Up After Completion
```bash
# Clean all checkpoints
python checkpoint_manager.py clean

# Clean specific run
python checkpoint_manager.py clean 20250810_143022
```

## 📊 Example Console Output

### First Run
```
CONFIG
  platforms: ['twitter', 'facebook', 'telegram']
  batch fetch: 1500, workers: 7, autosave: 200000
  checkpoint: ./data/technocracy_250810/checkpoint_20250810_143022.json

=== TWITTER ===
[twitter] picked 15000 actors in 12.3s
[twitter] unique post ids to fetch: 2500000 (collected in 45.2s)
[twitter] [autosave] 200000 rows → twitter_sample_autosave_200000_20250810_144512.csv
[twitter] [autosave] 400000 rows → twitter_sample_autosave_400000_20250810_145234.csv
❌ KERNEL CRASH!
```

### Resume Run
```
📁 Loaded checkpoint from ./data/technocracy_250810/checkpoint_20250810_143022.json
   - Platforms completed: []
   - Current platform: twitter
   - Batches processed: 267/1667
   - Rows saved: 400000

🔄 RESUMING from checkpoint:
   - Completed platforms: []
   - Current platform: twitter

=== TWITTER ===
📁 Resumed with 15000 previously selected actors
📁 Resumed with 2500000 previously collected post IDs
📁 Resuming from batch 267/1667
📁 Loaded existing chunk: twitter_sample_autosave_200000_20250810_144512.csv (200000 rows)
📁 Loaded existing chunk: twitter_sample_autosave_400000_20250810_145234.csv (200000 rows)
[twitter] fetch+transform (parallel): 84%|████████▍ | 1400/1667 [12:34<02:15, 1.97it/s]
```

## ⚙️ Configuration

You can adjust checkpoint frequency by modifying:

```python
# In your script config
CHUNK_SAVE_SIZE = 200_000    # Save chunks every N rows (more frequent = more checkpoints)
BATCH_FETCH_SIZE = 1500      # Smaller batches = more granular resume points
```

## 🛠️ Troubleshooting

### "No checkpoint found" but you expect one
- Check the `OUTPUT_DIR` path matches
- Ensure checkpoint files weren't accidentally deleted

### Script seems to start over despite checkpoints
- Verify the checkpoint files are in the correct directory
- Check that `RANDOM_SEED` hasn't changed (affects actor selection)

### Checkpoint files are huge
- This is normal with large datasets
- The `.pkl` files can be several MB for millions of post IDs

### Want to force a fresh start
```bash
# Clean all checkpoints first
python checkpoint_manager.py clean
# Then run normally
python enhanced_mongo_sampler_with_checkpoints.py
```

## 💡 Pro Tips

1. **Monitor disk space** - Checkpoint files can be large with big datasets
2. **Keep chunk files** - They're automatically reloaded on resume
3. **Use tmux/screen** - Even better protection against connection drops
4. **Check status regularly** - Use `checkpoint_manager.py status` to monitor progress
5. **Don't change config mid-run** - Changing `RANDOM_SEED`, `BATCH_FETCH_SIZE`, etc. may cause issues

The checkpoint system makes your long-running MongoDB sampling jobs much more reliable! 🚀
