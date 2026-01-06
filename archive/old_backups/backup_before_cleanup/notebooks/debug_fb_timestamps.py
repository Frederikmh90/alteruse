import json
import os
from datetime import datetime


def debug_facebook_timestamps(directory):
    """Debug Facebook timestamps to find problematic values."""

    problematic_timestamps = []
    valid_timestamps = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    # Extract all timestamp values
                    def extract_all_timestamps(obj, path=""):
                        timestamps = []
                        if isinstance(obj, dict):
                            for key, value in obj.items():
                                if key in ["timestamp", "time", "date", "created_time"]:
                                    timestamps.append((f"{path}.{key}", value))
                                elif isinstance(value, (dict, list)):
                                    timestamps.extend(
                                        extract_all_timestamps(value, f"{path}.{key}")
                                    )
                        elif isinstance(obj, list):
                            for i, item in enumerate(obj):
                                timestamps.extend(
                                    extract_all_timestamps(item, f"{path}[{i}]")
                                )
                        return timestamps

                    timestamps = extract_all_timestamps(data, file)

                    for path, ts in timestamps:
                        if isinstance(ts, (int, float)):
                            if ts <= 100:  # Very small timestamp (likely invalid)
                                problematic_timestamps.append(
                                    (file, path, ts, "too_small")
                                )
                            elif (
                                ts < 946684800
                            ):  # Before year 2000 (Unix timestamp 946684800 = 2000-01-01)
                                problematic_timestamps.append(
                                    (file, path, ts, "pre_2000")
                                )
                            else:
                                # Try to parse it
                                try:
                                    dt = datetime.fromtimestamp(ts)
                                    if dt.year >= 2000:
                                        valid_timestamps.append((file, path, ts, dt))
                                except:
                                    problematic_timestamps.append(
                                        (file, path, ts, "parse_error")
                                    )

                except Exception as e:
                    print(f"Error processing {file}: {e}")

    print(f"Found {len(problematic_timestamps)} problematic timestamps")
    print(f"Found {len(valid_timestamps)} valid timestamps")

    print("\nProblematic timestamps (first 20):")
    for file, path, ts, reason in problematic_timestamps[:20]:
        print(f"  {file}: {path} = {ts} ({reason})")

    if valid_timestamps:
        print(f"\nValid timestamp range:")
        dates = [dt for _, _, _, dt in valid_timestamps]
        print(f"  Earliest: {min(dates)}")
        print(f"  Latest: {max(dates)}")

    return problematic_timestamps, valid_timestamps


# Test the specific directory
fb_dir = "/Users/Codebase/projects/alteruse/data/Kantar_download_398_unzipped_new/474-4477-c-146189_2025-05-02T14__4477g1746194515336sE6EzyMOWskju5307uu5307ufacebookjepperoege02052025x4jfH67T-dbBSdwf"

print(f"Debugging timestamps in: {os.path.basename(fb_dir)}")
problematic, valid = debug_facebook_timestamps(fb_dir)
