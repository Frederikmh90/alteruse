import os
import sys
from pathlib import Path
from typing import Set, Tuple, List

# ==========================================
# CONFIGURATION
# ==========================================

# Define the paths here.
# "Old" folders (Absolute paths as provided)
OLD_BROWSER_PATH = Path("/Samlet_06112025/Browser")
OLD_FACEBOOK_PATH = Path("/Samlet_06112025/Facebook")

# "New" folders (Relative to current working directory, or absolute)
# Assuming these are in the directory where the script is run
NEW_BROWSER_PATH = Path("Browser_251126")
NEW_FACEBOOK_PATH = Path("Facebook_251126")

# Output configuration
SHOW_DETAILS_LIMIT = 20  # How many filenames to list in the console summary
OUTPUT_FILE = "dataset_comparison_report.txt"

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

def compare_folders(old_path: Path, new_path: Path, label: str, item_type: str = 'file') -> str:
    """Compares two folders and returns a formatted report string."""
    
    old_items = get_items_in_dir(old_path, item_type)
    new_items = get_items_in_dir(new_path, item_type)
    
    common_items = old_items.intersection(new_items)
    only_in_old = old_items - new_items
    only_in_new = new_items - old_items
    
    # Prepare report sections
    lines = []
    lines.append(f"\n{'='*60}")
    lines.append(f"COMPARISON: {label}")
    lines.append(f"Type: {'Directories' if item_type == 'dir' else 'Files'}")
    lines.append(f"{'='*60}")
    lines.append(f"Old Path: {old_path}")
    lines.append(f"New Path: {new_path}")
    lines.append(f"-"*60)
    lines.append(f"Total items in Old: {len(old_items)}")
    lines.append(f"Total items in New: {len(new_items)}")
    lines.append(f"Items matching exactly: {len(common_items)}")
    lines.append(f"")
    
    # Analysis of Differences
    lines.append(f"--- SUMMARY OF DIFFERENCES ---")
    lines.append(f"Items ONLY in NEW (Potential new data): {len(only_in_new)}")
    lines.append(f"Items ONLY in OLD (Missing in new):     {len(only_in_old)}")
    
    # Detailed lists
    lines.append(f"\n--- DETAILS: New {item_type.title()}s (First {SHOW_DETAILS_LIMIT}) ---")
    if only_in_new:
        for f in sorted(list(only_in_new))[:SHOW_DETAILS_LIMIT]:
            lines.append(f"  + {f}")
        if len(only_in_new) > SHOW_DETAILS_LIMIT:
            lines.append(f"  ... and {len(only_in_new) - SHOW_DETAILS_LIMIT} more.")
        
        # Save detailed lists to separate files for pipeline usage
        new_list_file = f"new_{item_type}s_{label.lower().replace(' ', '_')}.txt"
        with open(new_list_file, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(list(only_in_new))))
        lines.append(f"\n[!] List of NEW items saved to: {new_list_file}")

    else:
        lines.append("  (None)")
        
    lines.append(f"\n--- DETAILS: Missing {item_type.title()}s (First {SHOW_DETAILS_LIMIT}) ---")
    if only_in_old:
        for f in sorted(list(only_in_old))[:SHOW_DETAILS_LIMIT]:
            lines.append(f"  - {f}")
        if len(only_in_old) > SHOW_DETAILS_LIMIT:
            lines.append(f"  ... and {len(only_in_old) - SHOW_DETAILS_LIMIT} more.")
            
        missing_list_file = f"missing_{item_type}s_{label.lower().replace(' ', '_')}.txt"
        with open(missing_list_file, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(list(only_in_old))))
        lines.append(f"[!] List of MISSING items saved to: {missing_list_file}")

    else:
        lines.append("  (None)")

    return "\n".join(lines)

def main():
    # Print header
    print("Starting Dataset Comparison...")
    print(f"Working Directory: {os.getcwd()}")
    
    # Run comparisons
    # Browser: typically flat files
    browser_report = compare_folders(OLD_BROWSER_PATH, NEW_BROWSER_PATH, "BROWSER DATA", item_type='file')
    
    # Facebook: User specified it is structured as folders
    facebook_report = compare_folders(OLD_FACEBOOK_PATH, NEW_FACEBOOK_PATH, "FACEBOOK DATA", item_type='dir')
    
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
