#!/usr/bin/env python3
"""
Checkpoint Management Utility

This script helps you manage checkpoints for the MongoDB sampler.
Use this to check status, resume, or clean up checkpoint files.
"""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any


class CheckpointInspector:
    """Utility to inspect and manage checkpoint files."""

    def __init__(self, output_dir: str = "./data/technocracy_250810"):
        self.output_dir = Path(output_dir)

    def list_checkpoints(self):
        """List all checkpoint files in the output directory."""
        checkpoint_files = list(self.output_dir.glob("checkpoint_*.json"))

        if not checkpoint_files:
            print("No checkpoint files found.")
            return []

        print(f"Found {len(checkpoint_files)} checkpoint file(s):")
        checkpoints = []

        for checkpoint_file in sorted(checkpoint_files):
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)

                run_id = data.get("run_id", "unknown")
                start_time = data.get("start_time", 0)
                platforms_completed = data.get("platforms_completed", [])
                current_platform = data.get("current_platform")
                current_state = data.get("current_platform_state", {})

                print(f"\n📁 {checkpoint_file.name}")
                print(f"   Run ID: {run_id}")
                print(f"   Started: {self._format_timestamp(start_time)}")
                print(f"   Completed platforms: {platforms_completed}")
                print(f"   Current platform: {current_platform}")

                if current_platform and current_state:
                    batches_done = current_state.get("batches_processed", 0)
                    total_batches = current_state.get("total_batches", 0)
                    rows_saved = current_state.get("rows_saved", 0)
                    print(
                        f"   Progress: {batches_done}/{total_batches} batches, {rows_saved} rows"
                    )

                checkpoints.append({"file": checkpoint_file, "data": data})

            except Exception as e:
                print(f"❌ Error reading {checkpoint_file}: {e}")

        return checkpoints

    def show_detailed_status(self, checkpoint_file: Path):
        """Show detailed status of a specific checkpoint."""
        try:
            with open(checkpoint_file, "r") as f:
                data = json.load(f)

            print(f"\n🔍 DETAILED STATUS: {checkpoint_file.name}")
            print("=" * 60)

            run_id = data.get("run_id", "unknown")
            start_time = data.get("start_time", 0)
            last_save = data.get("last_save_time", 0)

            print(f"Run ID: {run_id}")
            print(f"Started: {self._format_timestamp(start_time)}")
            print(f"Last saved: {self._format_timestamp(last_save)}")

            platforms_completed = data.get("platforms_completed", [])
            current_platform = data.get("current_platform")
            platform_stats = data.get("platform_stats", {})

            print(f"\nCompleted platforms ({len(platforms_completed)}):")
            for platform in platforms_completed:
                stats = platform_stats.get(platform, {})
                actors = stats.get("actors", 0)
                rows = stats.get("rows", 0)
                elapsed = stats.get("elapsed", 0)
                print(f"  ✅ {platform}: {actors} actors, {rows} rows, {elapsed:.1f}s")

            if current_platform:
                current_state = data.get("current_platform_state", {})
                print(f"\nCurrent platform: {current_platform}")

                accounts_selected = current_state.get("accounts_selected", False)
                num_accounts = len(current_state.get("accounts", []))
                print(
                    f"  Accounts selected: {accounts_selected} ({num_accounts} accounts)"
                )

                ids_collected = current_state.get("post_ids_collected", False)
                num_ids = len(current_state.get("all_post_ids", []))
                print(f"  Post IDs collected: {ids_collected} ({num_ids} IDs)")

                batches_done = current_state.get("batches_processed", 0)
                total_batches = current_state.get("total_batches", 0)
                rows_saved = current_state.get("rows_saved", 0)

                if total_batches > 0:
                    progress_pct = (batches_done / total_batches) * 100
                    print(
                        f"  Batch progress: {batches_done}/{total_batches} ({progress_pct:.1f}%)"
                    )
                else:
                    print(f"  Batch progress: {batches_done}/? batches")

                print(f"  Rows saved: {rows_saved}")

                chunk_files = current_state.get("chunk_files_saved", [])
                if chunk_files:
                    print(f"  Chunk files saved: {len(chunk_files)}")
                    for chunk in chunk_files[-3:]:  # Show last 3 chunks
                        print(f"    - {chunk['filename']} ({chunk['row_count']} rows)")
                    if len(chunk_files) > 3:
                        print(f"    ... and {len(chunk_files) - 3} more")

            print("\n" + "=" * 60)

        except Exception as e:
            print(f"❌ Error reading checkpoint: {e}")

    def clean_checkpoints(self, run_id: str = None):
        """Clean up checkpoint files."""
        if run_id:
            # Clean specific run
            checkpoint_file = self.output_dir / f"checkpoint_{run_id}.json"
            state_file = self.output_dir / f"state_{run_id}.pkl"

            removed = 0
            for file in [checkpoint_file, state_file]:
                if file.exists():
                    file.unlink()
                    print(f"🗑️  Removed {file.name}")
                    removed += 1

            if removed == 0:
                print(f"No checkpoint files found for run_id: {run_id}")
            else:
                print(f"Cleaned up {removed} file(s) for run_id: {run_id}")
        else:
            # Clean all checkpoints
            checkpoint_files = list(self.output_dir.glob("checkpoint_*.json"))
            state_files = list(self.output_dir.glob("state_*.pkl"))

            all_files = checkpoint_files + state_files
            if not all_files:
                print("No checkpoint files to clean.")
                return

            print(f"Found {len(all_files)} checkpoint file(s) to remove:")
            for file in all_files:
                print(f"  - {file.name}")

            confirm = input("\nAre you sure you want to delete these files? (y/N): ")
            if confirm.lower() in ["y", "yes"]:
                for file in all_files:
                    file.unlink()
                print(f"✅ Removed {len(all_files)} checkpoint file(s)")
            else:
                print("Cancelled.")

    def _format_timestamp(self, timestamp: float) -> str:
        """Format timestamp for display."""
        if timestamp == 0:
            return "N/A"

        import datetime

        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="Checkpoint Management Utility")
    parser.add_argument(
        "--output-dir",
        default="./data/technocracy_250810",
        help="Output directory containing checkpoint files",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # List command
    list_parser = subparsers.add_parser("list", help="List all checkpoint files")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show detailed status")
    status_parser.add_argument(
        "run_id", nargs="?", help="Run ID to show status for (latest if not provided)"
    )

    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean up checkpoint files")
    clean_parser.add_argument(
        "run_id", nargs="?", help="Run ID to clean (all if not provided)"
    )

    # Resume command
    resume_parser = subparsers.add_parser(
        "resume", help="Show how to resume from a checkpoint"
    )
    resume_parser.add_argument(
        "run_id", nargs="?", help="Run ID to resume from (latest if not provided)"
    )

    args = parser.parse_args()

    inspector = CheckpointInspector(args.output_dir)

    if args.command == "list" or args.command is None:
        inspector.list_checkpoints()

    elif args.command == "status":
        checkpoints = inspector.list_checkpoints()
        if not checkpoints:
            return

        if args.run_id:
            checkpoint_file = inspector.output_dir / f"checkpoint_{args.run_id}.json"
            if checkpoint_file.exists():
                inspector.show_detailed_status(checkpoint_file)
            else:
                print(f"Checkpoint file not found for run_id: {args.run_id}")
        else:
            # Show latest
            latest = max(checkpoints, key=lambda x: x["data"].get("last_save_time", 0))
            inspector.show_detailed_status(latest["file"])

    elif args.command == "clean":
        inspector.clean_checkpoints(args.run_id)

    elif args.command == "resume":
        checkpoints = inspector.list_checkpoints()
        if not checkpoints:
            return

        if args.run_id:
            checkpoint_file = inspector.output_dir / f"checkpoint_{args.run_id}.json"
            if not checkpoint_file.exists():
                print(f"Checkpoint file not found for run_id: {args.run_id}")
                return
        else:
            # Use latest
            latest = max(checkpoints, key=lambda x: x["data"].get("last_save_time", 0))
            checkpoint_file = latest["file"]

        print(f"\n🔄 TO RESUME FROM CHECKPOINT:")
        print(
            f"Simply run your enhanced script again. It will automatically detect and resume from:"
        )
        print(f"📁 {checkpoint_file}")
        print(f"\nThe script will:")
        print(f"  ✅ Skip completed platforms")
        print(f"  ✅ Resume current platform from last saved batch")
        print(f"  ✅ Load existing chunk files")
        print(f"  ✅ Continue where it left off")


if __name__ == "__main__":
    main()
