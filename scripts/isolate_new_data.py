import os
import shutil
import sys
from pathlib import Path
from typing import Set, List

# ==========================================
# CONFIGURATION
# ==========================================

# Define the paths here.
# "Old" folders (Absolute paths as provided)
OLD_BROWSER_PATH = Path("/Samlet_06112025/Browser")
OLD_FACEBOOK_PATH = Path("/Samlet_06112025/Facebook")

# "New" folders (Relative to current working directory, or absolute)
NEW_BROWSER_PATH = Path("Browser_251126")
NEW_FACEBOOK_PATH = Path("Facebook_251126")

# Output configuration
SHOW_DETAILS_LIMIT = 20
OUTPUT_FILE = "dataset_comparison_report.txt"

# Isolation folder configuration
ISOLATION_BASE_DIR = Path("Isolated_New_Data_251126")
ISOLATION_BROWSER_DIR = ISOLATION_BASE_DIR / "Browser"
ISOLATION_FACEBOOK_DIR = ISOLATION_BASE_DIR / "Facebook"

# ==========================================

def get_items_in_dir(directory: Path, item_type: str = 'file') -> Set[str]:
    """
    Returns a set of names in the directory.
    item_type: 'file' for files only, 'dir' for directories only.
    """
    if not directory.exists():
        print(f"Warning: Directory not found: {directory}")
        return set()
    
    items = set()
    for p in directory.iterdir():
        if p.name.startswith('.'):
            continue
            
        if item_type == 'file' and p.is_file():
            items.add(p.name)
        elif item_type == 'dir' and p.is_dir():
            items.add(p.name)
            
    return items

def compare_and_isolate(old_path: Path, new_path: Path, isolation_path: Path, label: str, item_type: str = 'file') -> str:
    """
    Compares folders and copies new items to the isolation path.
    """
    
    old_items = get_items_in_dir(old_path, item_type)
    new_items = get_items_in_dir(new_path, item_type)
    
    common_items = old_items.intersection(new_items)
    only_in_old = old_items - new_items
    only_in_new = new_items - old_items
    
    # Prepare report sections
    lines = []
    lines.append(f"\n{'='*60}")
    lines.append(f"COMPARISON & ISOLATION: {label}")
    lines.append(f"Type: {'Directories' if item_type == 'dir' else 'Files'}")
    lines.append(f"{'='*60}")
    lines.append(f"Old Path: {old_path}")
    lines.append(f"New Path: {new_path}")
    lines.append(f"Isolation Path: {isolation_path}")
    lines.append(f"-"*60)
    lines.append(f"Total items in Old: {len(old_items)}")
    lines.append(f"Total items in New: {len(new_items)}")
    lines.append(f"Items matching exactly: {len(common_items)}")
    
    # Analysis of Differences
    lines.append(f"--- SUMMARY OF DIFFERENCES ---")
    lines.append(f"Items ONLY in NEW (To be isolated): {len(only_in_new)}")
    lines.append(f"Items ONLY in OLD (Missing in new): {len(only_in_old)}")
    
    # Isolate new items
    if only_in_new:
        lines.append(f"\n--- ISOLATION PROCESS ---")
        
        # Create isolation directory if it doesn't exist
        if not isolation_path.exists():
            isolation_path.mkdir(parents=True, exist_ok=True)
            lines.append(f"Created directory: {isolation_path}")
        
        count = 0
        for item_name in sorted(list(only_in_new)):
            src_item = new_path / item_name
            dst_item = isolation_path / item_name
            
            try:
                if item_type == 'file':
                    shutil.copy2(src_item, dst_item)
                elif item_type == 'dir':
                    if dst_item.exists():
                        shutil.rmtree(dst_item)
                    shutil.copytree(src_item, dst_item)
                count += 1
            except Exception as e:
                lines.append(f"ERROR copying {item_name}: {e}")
        
        lines.append(f"Successfully copied {count} items to {isolation_path}")

        # Save detailed list
        new_list_file = f"new_{item_type}s_{label.lower().replace(' ', '_')}.txt"
        with open(new_list_file, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(list(only_in_new))))
        lines.append(f"[!] List of NEW items saved to: {new_list_file}")

    else:
        lines.append("\nNo new items to isolate.")

    return "\n".join(lines)

def main():
    # Print header
    print("Starting Dataset Isolation Pipeline...")
    print(f"Working Directory: {os.getcwd()}")
    print(f"Output Base Directory: {ISOLATION_BASE_DIR}")
    
    # Run comparisons and isolation
    browser_report = compare_and_isolate(
        OLD_BROWSER_PATH, 
        NEW_BROWSER_PATH, 
        ISOLATION_BROWSER_DIR,
        "BROWSER DATA", 
        item_type='file'
    )
    
    facebook_report = compare_and_isolate(
        OLD_FACEBOOK_PATH, 
        NEW_FACEBOOK_PATH, 
        ISOLATION_FACEBOOK_DIR,
        "FACEBOOK DATA", 
        item_type='dir'
    )
    
    full_report = browser_report + "\n" + facebook_report
    
    # Print to console
    print(full_report)
    
    # Save to file
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(full_report)
        print(f"\n{'-'*60}")
        print(f"Full report saved to: {OUTPUT_FILE}")
        print(f"{'-'*60}")
    except Exception as e:
        print(f"Error saving report: {e}")

if __name__ == "__main__":
    main()
