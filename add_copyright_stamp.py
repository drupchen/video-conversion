import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
SEARCH_DIR = "."  # Current directory (or change to your path)
MAX_PARALLEL_TASKS = 4

# --- COPYRIGHT INFO ---
COPYRIGHT_HOLDER = "Copyright © Shechen Archives. All Rights Reserved."
PROJECT_NAME = "Khyentse Önang"
DESCRIPTION = "Preserved by the Khyentse Önang Project. Original media from Shechen Archives."


def needs_stamping(filepath):
    """Checks if the file already has the correct copyright tag."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-show_entries", "format_tags=copyright",
        "-of", "default=noprint_wrappers=1:nokey=1",
        filepath
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True)
        existing_copyright = result.stdout.strip()
        # If the tag is missing or different, we need to stamp it
        return COPYRIGHT_HOLDER not in existing_copyright
    except:
        return True


def stamp_file(filepath):
    """Creates a temporary copy with metadata, then replaces the original."""
    if not needs_stamping(filepath):
        print(f"  [OK] Already Stamped: {os.path.basename(filepath)}")
        return

    # Create a temp filename
    directory, filename = os.path.split(filepath)
    temp_path = os.path.join(directory, f"temp_{filename}")

    cmd = [
        "ffmpeg", "-n", "-i", filepath,
        "-c", "copy",  # <--- CRITICAL: Copies video/audio without re-encoding (Instant)
        "-map", "0",
        "-map_metadata", "0",
        # New Tags
        "-metadata", f"copyright={COPYRIGHT_HOLDER}",
        "-metadata", f"artist={PROJECT_NAME}",
        "-metadata", f"comment={DESCRIPTION}",
        temp_path
    ]

    print(f"  > Stamping: {filename}")
    try:
        # Run FFmpeg (silently)
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

        # If successful, replace the original file
        os.replace(temp_path, filepath)
        print(f"  [DONE] Updated: {filename}")
    except subprocess.CalledProcessError:
        print(f"  [ERROR] Failed to stamp: {filename}")
        if os.path.exists(temp_path):
            os.remove(temp_path)


def main():
    print(f"Scanning for Masters and Proxies in '{SEARCH_DIR}'...")

    files_to_process = []

    # 1. Find all relevant files
    for root, dirs, files in os.walk(SEARCH_DIR):
        # Only look inside "Masters" and "Proxies" folders
        if "Masters" in root or "Proxies" in root:
            for file in files:
                if file.lower().endswith(('.mov', '.mp4')):
                    files_to_process.append(os.path.join(root, file))

    print(f"Found {len(files_to_process)} candidate files.")
    print(f"Starting rapid metadata update with {MAX_PARALLEL_TASKS} workers...")

    # 2. Process in parallel
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS) as executor:
        executor.map(stamp_file, files_to_process)

    print("\nAll files updated.")


if __name__ == "__main__":
    main()