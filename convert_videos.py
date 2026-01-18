import pandas as pd
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
# How many files to convert at the same time?
# Start with 4. If CPU is still low, try 8. If NAS slows down, go back to 2.
MAX_PARALLEL_TASKS = 4


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
    masters_dir = os.path.join(directory, "Masters")
    proxies_dir = os.path.join(directory, "Proxies")
    os.makedirs(masters_dir, exist_ok=True)
    os.makedirs(proxies_dir, exist_ok=True)

    base_name = os.path.splitext(filename)[0]

    # Logic setup
    codec = str(row['Video Codec']).lower()
    field_order = str(row['Field Order']).lower()

    # 1. MASTER ARCHIVE
    archive_out = os.path.join(masters_dir, f"{base_name}_Master.mov")

    if not os.path.exists(archive_out):
        # Build Archive Command
        if 'prores' in codec:
            # Copy Mode
            cmd_archive = [
                "ffmpeg", "-n", "-i", input_path,
                "-c", "copy", "-map", "0", "-map_metadata", "0",
                archive_out
            ]
        else:
            # Transcode Mode
            cmd_archive = [
                "ffmpeg", "-n", "-i", input_path,
                "-c:v", "prores_ks", "-profile:v", "2", "-vendor", "apl0",
                "-bits_per_mb", "8000", "-pix_fmt", "yuv422p10le",
                "-c:a", "pcm_s16le", "-ar", "48000",
                "-map", "0", "-map_metadata", "0",
                archive_out
            ]

        # Run Archive
        # stdout/stderr to DEVNULL prevents the console from becoming a mess of 4 overlapping text streams
        subprocess.run(cmd_archive, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  [DONE] Master: {filename}")
    else:
        print(f"  [SKIP] Master exists: {filename}")

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
            "-map", "0", "-map_metadata", "0",
            "-movflags", "+faststart",
            sharing_out
        ]

        # Run Proxy
        subprocess.run(cmd_sharing, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  [DONE] Proxy:  {filename}")
    else:
        print(f"  [SKIP] Proxy exists:  {filename}")

    return f"COMPLETED: {filename}"


def main():
    catalog_file = "Archive_Catalog_Khyentsé Rinpoché-AVI.csv"
    try:
        df = pd.read_csv(catalog_file)
    except FileNotFoundError:
        print(f"Error: Could not find {catalog_file}")
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