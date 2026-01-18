import os
import sys
import csv
import json
import subprocess
import math
from datetime import datetime

def format_size(size_bytes):
    """Converts bytes to human readable string (e.g., 1.2 GB)."""
    if not size_bytes or size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    try:
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"
    except ValueError:
        return "0 B"

def format_duration(seconds_str):
    """Converts seconds string to HH:MM:SS format."""
    try:
        total_seconds = float(seconds_str)
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except (ValueError, TypeError):
        return "" 

def clean_date_string(date_str):
    """Makes ISO metadata dates look like normal dates."""
    if not date_str:
        return ""
    return date_str.replace("T", " ").replace("Z", "").split(".")[0]

def get_file_info(filepath):
    """Runs ffprobe and stat to get all metadata safely."""
    
    # 1. Get File System Data
    try:
        stat_info = os.stat(filepath)
        file_size_bytes = stat_info.st_size
        readable_size = format_size(file_size_bytes)
        fs_modify_time = datetime.fromtimestamp(stat_info.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

    # 2. Get Video/Audio Data via JSON
    # I've put this on one line to prevent copy-paste errors
    cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', filepath]
    
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        data = json.loads(result.stdout)
    except:
        return {
            'Filename': os.path.basename(filepath),
            'Directory Path': os.path.dirname(filepath),
            'File Size': readable_size,
            'Status': 'Corrupt/Unreadable'
        }

    # Extract Data
    fmt = data.get('format', {})
    tags = fmt.get('tags', {})
    video = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), {})
    audio = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), {})

    # Date Logic
    meta_date = tags.get('creation_time', '')
    final_date = clean_date_string(meta_date) if meta_date else fs_modify_time

    # Build Dictionary
    return {
        'Directory Path': os.path.dirname(filepath),
        'Filename': os.path.basename(filepath),
        'Content Date': final_date,
        'File Size': readable_size,
        'Duration': format_duration(fmt.get('duration', '')),
        'Container Format': fmt.get('format_name', ''),
        'Total Bitrate': fmt.get('bit_rate', ''),
        
        'Video Codec': video.get('codec_name', ''),
        'Resolution': f"{video.get('width', '?')}x{video.get('height', '?')}",
        'Field Order': video.get('field_order', 'unknown'),
        'Color Space': video.get('color_primaries', 'unknown'),
        'Pixel Format': video.get('pix_fmt', ''),
        'Frame Rate': video.get('r_frame_rate', ''),
        
        'Audio Codec': audio.get('codec_name', 'none'),
        'Sample Rate': audio.get('sample_rate', ''),
        'Bit Depth': audio.get('bits_per_sample', ''),
        'Channels': audio.get('channels', '')
    }

def main():
    dir = "Khyentsé Rinpoché-AVI"
    search_dir = f"/media/drupchen/Khyentse Önang/NAS/Video Archives/{dir}"
    output_file = f"Archive_Catalog_{dir}.csv"
    
    columns = [
        'Directory Path', 'Filename', 'Content Date', 'File Size', 'Duration', 
        'Container Format', 'Total Bitrate', 
        'Video Codec', 'Resolution', 'Field Order', 'Color Space', 'Pixel Format', 'Frame Rate',
        'Audio Codec', 'Sample Rate', 'Bit Depth', 'Channels'
    ]

    print(f"Scanning directory: {search_dir}")
    print(f"Saving to: {output_file}")
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        
        count = 0
        for root, dirs, files in os.walk(search_dir):
            for file in files:
                if file.lower().endswith(('.avi', '.mov', '.mp4')):
                    filepath = os.path.join(root, file)
                    info = get_file_info(filepath)
                    if info:
                        writer.writerow(info)
                        print(f"Cataloged: {file}")
                        count += 1
                        
    print(f"\nDone! Processed {count} files.")

if __name__ == "__main__":
    main()
