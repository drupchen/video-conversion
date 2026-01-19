import pandas as pd
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---------------------------------------------------------
# Mode 1 = Masters only (ProRes)
# Mode 2 = Proxies only (H.264 MP4)
# Mode 3 = Both Masters and Proxies
PROCESSING_MODE = 2

# How many files to convert at the same time?
# Start with 4. If CPU is still low, try 8. If NAS slows down, go back to 2.
MAX_PARALLEL_TASKS = 4

# --- CATALOG FILE -----------------------------------------------------------
CATALOG_FILE = "Archive_Catalog_BRILLIANT Moon-MOV.csv"

# --- COPYRIGHT INFO ---------------------------------------------------------
COPYRIGHT_HOLDER = "Copyright © Shechen Archives. All Rights Reserved."
PROJECT_NAME = "Khyentse Önang"
DESCRIPTION = "Preserved by the Khyentse Önang Project. Original media from Shechen Archives."

# Lock to prevent messy printing when multiple threads write to the console/file at once
print_lock = threading.Lock()
log_file_path = "conversion_errors.log"


def log_error(message):
    """Writes error messages to a file so they aren't lost."""
    with print_lock:
        print(message)  # Print to screen
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")


def run_ffmpeg_safe(command, output_file):
    """
    Runs FFmpeg, checks for errors, and cleans up bad files.
    """
    try:
        # Run FFmpeg and capture the error output (stderr)
        result = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,  # Hide standard output
            stderr=subprocess.PIPE,  # Capture errors
            text=True  # Decode output as text
        )

        # CHECK 1: Did FFmpeg report a failure? (Non-zero exit code)
        if result.returncode != 0:
            error_snippet = result.stderr[-600:]  # Get the last 600 characters of the error
            log_error(f"\n❌ [CRASH] Failed to convert: {os.path.basename(output_file)}")
            log_error(f"   Command Exit Code: {result.returncode}")
            log_error(f"   FFmpeg Error Log:\n{error_snippet}\n" + "-" * 40)

            # Cleanup: Delete the 0-byte or corrupted file
            if os.path.exists(output_file):
                os.remove(output_file)
                log_error(f"   🗑️  Deleted broken file: {output_file}")
            return False

        # CHECK 2: Did it create a 0-byte file despite saying "Success"?
        if os.path.exists(output_file):
            if os.path.getsize(output_file) == 0:
                log_error(f"\n⚠️ [EMPTY] FFmpeg finished but file is 0 bytes: {os.path.basename(output_file)}")
                os.remove(output_file)
                log_error(f"   🗑️  Deleted empty file.")
                return False
        else:
            log_error(f"\n❓ [MISSING] FFmpeg finished but output file not found: {output_file}")
            return False

        return True

    except Exception as e:
        log_error(f"\n🔥 [EXCEPTION] Python script error on {output_file}: {str(e)}")
        return False


def process_single_file(row):
    """
    This function handles the logic for ONE file.
    It will be run in parallel by the Executor.
    """
    directory = row['Directory Path']
    filename = row['Filename']
    input_path = os.path.join(directory, filename)

    if not os.path.exists(input_path):
        return f"SKIP: Missing file {filename}"

    # Create Output Folders (Thread-safe enough for this usage)
    if PROCESSING_MODE == 1 or PROCESSING_MODE == 3:
        masters_dir = os.path.join(directory, "Masters")
        os.makedirs(masters_dir, exist_ok=True)
    if PROCESSING_MODE == 2 or PROCESSING_MODE == 3:
        proxies_dir = os.path.join(directory, "Proxies")
        os.makedirs(proxies_dir, exist_ok=True)

    base_name = os.path.splitext(filename)[0]

    # Logic setup
    codec = str(row['Video Codec']).lower()
    field_order = str(row['Field Order']).lower()

    # --- METADATA FLAGS ---
    # We add these to every single command
    metadata_flags = [
        "-metadata", f"copyright={COPYRIGHT_HOLDER}",
        "-metadata", f"artist={PROJECT_NAME}",
        "-metadata", f"comment={DESCRIPTION}",
        # Also map original metadata (like dates) from source
        "-map_metadata", "0"
    ]

    if PROCESSING_MODE == 1 or PROCESSING_MODE == 3:
        # 1. MASTER ARCHIVE
        archive_out = os.path.join(masters_dir, f"{base_name}_Master.mov")

        if not os.path.exists(archive_out):
            # Build Archive Command
            if 'prores' in codec:
                # Copy Mode
                cmd_archive = [
                                  "ffmpeg", "-n", "-i", input_path,
                                  "-c", "copy",
                                  "-map", "0"
                              ] + metadata_flags + [archive_out]  # Add metadata flags here
            else:
                # Transcode Mode
                cmd_archive = [
                                  "ffmpeg", "-n", "-i", input_path,
                                  "-c:v", "prores_ks", "-profile:v", "2", "-vendor", "apl0",
                                  "-bits_per_mb", "8000", "-pix_fmt", "yuv422p10le",
                                  "-c:a", "pcm_s16le", "-ar", "48000",
                                  "-map", "0"
                              ] + metadata_flags + [archive_out]  # Add metadata flags here

            # Run Archive
            success = run_ffmpeg_safe(cmd_archive, archive_out)
            if success:
                print(f"  [DONE] Master: {filename}")
            print(f"  [DONE] Master: {filename}")
        else:
            print(f"  [SKIP] Master exists: {filename}")

    if PROCESSING_MODE == 2 or PROCESSING_MODE == 3:
        # 2. PROXY SHARING
        sharing_out = os.path.join(proxies_dir, f"{base_name}_Share.mp4")

        if not os.path.exists(sharing_out):
            # Build Proxy Command
            vf_filters = ["format=yuv420p"]
            is_interlaced = False

            if 'dvvideo' in codec or 'mpeg2video' in codec:
                is_interlaced = True
            elif 'interlaced' in field_order or 'bb' in field_order or 'tt' in field_order:
                is_interlaced = True

            if is_interlaced:
                vf_filters.insert(0, "yadif")

            cmd_sharing = [
                              "ffmpeg", "-n", "-i", input_path,
                              "-c:v", "libx264", "-crf", "23", "-preset", "slow",
                              "-vf", ",".join(vf_filters),
                              "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
                              "-movflags", "+faststart",
                              # FIXED: Map ALL video and ALL audio, but exclude Data streams
                              "-map", "0:v", "-map", "0:a"
                          ] + metadata_flags + [sharing_out]  # Add metadata flags here

            # Run Proxy
            success = run_ffmpeg_safe(cmd_sharing, sharing_out)
            if success:
                print(f"  [DONE] Proxy:  {filename}")
        else:
            print(f"  [SKIP] Proxy exists:  {filename}")

    return f"COMPLETED: {filename}"


def main():
    try:
        df = pd.read_csv(CATALOG_FILE)
    except FileNotFoundError:
        print(f"Error: Could not find {CATALOG_FILE}")
        return

    print(f"Found {len(df)} files. Starting Parallel Process with {MAX_PARALLEL_TASKS} workers...")
    print("Output might be quiet while working. Please wait...")

    # The Executor handles the parallel magic
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS) as executor:
        # Map the function to the rows
        # We convert the dataframe to a list of dicts (rows) to iterate easily
        rows = [row for index, row in df.iterrows()]
        executor.map(process_single_file, rows)

    print("\nAll tasks complete.")


if __name__ == "__main__":
    main()