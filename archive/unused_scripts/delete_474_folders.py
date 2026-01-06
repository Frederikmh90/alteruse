import shutil
from pathlib import Path

def delete_specific_folders(target_dir: str):
    """
    Deletes folders starting with "474" in the specified directory (non-recursive).
    """
    directory = Path(target_dir)
    
    if not directory.exists():
        print(f"Directory not found: {directory}")
        return

    print(f"Scanning '{directory}' for folders starting with '474'...")
    
    count = 0
    for item in directory.iterdir():
        # Check if it's a directory and starts with "474"
        if item.is_dir() and item.name.startswith("474"):
            try:
                print(f"Deleting: {item.name}")
                shutil.rmtree(item)
                count += 1
            except Exception as e:
                print(f"Error deleting {item.name}: {e}")
                
    print(f"Finished. Deleted {count} folders.")

if __name__ == "__main__":
    # Change this path to your target folder
    TARGET_FOLDER = "."  # Current folder by default
    delete_specific_folders(TARGET_FOLDER)

