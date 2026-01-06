import os
from pathlib import Path
import pandas as pd


def analyze_folder_contents(folder_path: str) -> dict:
    """Analyze what types of files are in a folder."""
    file_types = {}
    total_files = 0

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.startswith(".") or file.startswith("._"):
                continue

            total_files += 1
            ext = os.path.splitext(file)[1].lower()
            if ext == "":
                ext = "no_extension"

            if ext not in file_types:
                file_types[ext] = []
            file_types[ext].append(file)

    return {
        "total_files": total_files,
        "file_types": file_types,
        "extensions": list(file_types.keys()),
    }


def is_valid_facebook_folder(folder_path: str) -> tuple[bool, str, dict]:
    """Check if a folder contains valid Facebook data for processing."""
    # Skip folders that just contain 'your_facebook_activity'
    if "your_facebook_activity" in folder_path.lower():
        return False, "Contains 'your_facebook_activity' in path", {}

    # Analyze folder contents
    folder_analysis = analyze_folder_contents(folder_path)

    # Check for essential JSON data files
    required_patterns = [
        "posts_and_comments.json",
        "posts.json",
        "comments.json",
    ]

    has_required_file = False
    found_files = []
    for root, _, files in os.walk(folder_path):
        for pattern in required_patterns:
            matching_files = [f for f in files if pattern in f.lower()]
            if matching_files:
                has_required_file = True
                found_files.extend(matching_files)
                break
        if has_required_file:
            break

    if has_required_file:
        return (
            True,
            f"Found required JSON files: {', '.join(found_files)}",
            folder_analysis,
        )
    else:
        # Check what files ARE in the folder
        all_json_files = []
        all_html_files = []
        for root, _, files in os.walk(folder_path):
            json_files = [f for f in files if f.endswith(".json")]
            html_files = [f for f in files if f.endswith(".html") or f.endswith(".htm")]
            all_json_files.extend(json_files)
            all_html_files.extend(html_files)

        if all_json_files:
            return (
                False,
                f"JSON files found but none match required patterns: {', '.join(all_json_files[:5])}{'...' if len(all_json_files) > 5 else ''}",
                folder_analysis,
            )
        elif all_html_files:
            return (
                False,
                f"No JSON files, but found HTML files: {', '.join(all_html_files[:5])}{'...' if len(all_html_files) > 5 else ''}",
                folder_analysis,
            )
        else:
            return False, "No JSON or HTML files found in folder", folder_analysis


def analyze_facebook_folders(base_dir: str):
    """Analyze all folders in the base directory to see which are valid Facebook folders."""
    results = []

    print(f"Analyzing Facebook folders in: {base_dir}")

    # Get all subdirectories
    all_folders = []
    for item in os.listdir(base_dir):
        item_path = os.path.join(base_dir, item)
        if os.path.isdir(item_path) and not item.startswith("."):
            all_folders.append(item_path)

    print(f"Found {len(all_folders)} total subdirectories")

    valid_count = 0
    invalid_count = 0
    html_only_count = 0

    for folder_path in sorted(all_folders):
        folder_name = os.path.basename(folder_path)
        is_valid, reason, folder_analysis = is_valid_facebook_folder(folder_path)

        # Check if this folder has HTML files (potential HTML-format Facebook data)
        has_html = ".html" in folder_analysis.get(
            "extensions", []
        ) or ".htm" in folder_analysis.get("extensions", [])

        results.append(
            {
                "folder_name": folder_name,
                "full_path": folder_path,
                "is_valid": is_valid,
                "reason": reason,
                "total_files": folder_analysis.get("total_files", 0),
                "file_extensions": ", ".join(folder_analysis.get("extensions", [])),
                "has_html_files": has_html,
                "html_files": len(
                    folder_analysis.get("file_types", {}).get(".html", [])
                )
                + len(folder_analysis.get("file_types", {}).get(".htm", [])),
                "json_files": len(
                    folder_analysis.get("file_types", {}).get(".json", [])
                ),
            }
        )

        if is_valid:
            valid_count += 1
            print(f"✅ VALID: {folder_name}")
        else:
            invalid_count += 1
            if has_html:
                html_only_count += 1
                print(f"🌐 HTML-ONLY: {folder_name}")
            else:
                print(f"❌ INVALID: {folder_name}")
            print(f"   Reason: {reason}")
            print(
                f"   Files: {folder_analysis.get('total_files', 0)} total, Extensions: {', '.join(folder_analysis.get('extensions', []))}"
            )

    print(f"\nSUMMARY:")
    print(f"Total folders: {len(all_folders)}")
    print(f"Valid folders (JSON): {valid_count}")
    print(f"Invalid folders (HTML-only): {html_only_count}")
    print(f"Invalid folders (other): {invalid_count - html_only_count}")
    print(f"Total invalid: {invalid_count}")

    # Save detailed results
    df = pd.DataFrame(results)
    output_file = "/Users/Codebase/projects/alteruse/data/final_11june/facebook_folder_analysis_detailed.xlsx"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_excel(output_file, index=False)
    print(f"\nDetailed analysis saved to: {output_file}")

    # Show HTML-only folders specifically
    html_folders = df[df["has_html_files"] == True]
    if len(html_folders) > 0:
        print(f"\nFolders with HTML files (potential for HTML parsing):")
        for _, row in html_folders.iterrows():
            print(
                f"  {row['folder_name']}: {row['html_files']} HTML files, {row['json_files']} JSON files"
            )

    # Show completely invalid folders
    completely_invalid = df[(df["is_valid"] == False) & (df["has_html_files"] == False)]
    if len(completely_invalid) > 0:
        print(f"\nCompletely invalid folders (no JSON or HTML):")
        for _, row in completely_invalid.head(5).iterrows():
            print(f"  {row['folder_name']}: {row['reason']}")


if __name__ == "__main__":
    facebook_dir = "/Users/Codebase/projects/alteruse/data/Samlet_06112025/Facebook/"
    analyze_facebook_folders(facebook_dir)
