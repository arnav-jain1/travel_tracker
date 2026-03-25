import json
import gzip
import csv
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import time

def convert_time(base_timestamp, trace_seconds):
    """Converts seconds-since-midnight into a readable UTC timestamp."""
    if base_timestamp == 0 or trace_seconds is None:
        return "UNKNOWN"
        
    base_dt = datetime.fromtimestamp(base_timestamp, tz=timezone.utc)
    midnight = base_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    actual_time = midnight + timedelta(seconds=trace_seconds)
    return actual_time.strftime('%Y-%m-%d %H:%M:%S')

def process_single_folder(folder_path):
    """Worker function: Processes a single folder of gzipped JSON files."""
    path = Path(folder_path)
    results = []
    missing_count = 0
    error_count = 0
    
    # Locate all .json files in this specific subfolder
    all_files = list(path.rglob("*.json"))
    
    for file_path in all_files:
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                
                trace_data = data.get("trace", [])
                if not trace_data:
                    continue 
                
                # 1. Find the callsign
                callsign = None
                for point in trace_data:
                    if len(point) > 8 and isinstance(point[8], dict):
                        raw_flight = point[8].get("flight")
                        if raw_flight and raw_flight.strip():
                            callsign = raw_flight.strip()
                            break
                
                # 2. Extract everything else if callsign exists
                if callsign:
                    icao = data.get("icao", "UNKNOWN")
                    tail_number = data.get("r", "UNKNOWN")
                    desc = data.get("desc", "UNKNOWN")
                    base_timestamp = data.get("timestamp", 0)
                    
                    first_time_raw, first_lat, first_lon = None, None, None
                    last_time_raw, last_lat, last_lon = None, None, None
                    
                    # Find FIRST valid location
                    for point in trace_data:
                        if len(point) >= 3 and point[1] is not None and point[2] is not None:
                            first_time_raw = point[0]
                            first_lat = point[1]
                            first_lon = point[2]
                            break
                            
                    # Find LAST valid location
                    for point in reversed(trace_data):
                        if len(point) >= 3 and point[1] is not None and point[2] is not None:
                            last_time_raw = point[0]
                            last_lat = point[1]
                            last_lon = point[2]
                            break

                    first_time_utc = convert_time(base_timestamp, first_time_raw)
                    last_time_utc = convert_time(base_timestamp, last_time_raw)

                    results.append({
                        "icao": icao,
                        "callsign": callsign,
                        "tail_number": tail_number,
                        "description": desc,
                        "first_time_utc": first_time_utc,
                        "first_lat": first_lat,
                        "first_lon": first_lon,
                        "last_time_utc": last_time_utc,
                        "last_lat": last_lat,
                        "last_lon": last_lon,
                        "file": file_path.name,
                        "folder": file_path.parent.name
                    })
                else:
                    missing_count += 1
                    
        except gzip.BadGzipFile:
            error_count += 1
        except Exception:
            error_count += 1
            
    return results, missing_count, error_count

def save_to_csv(results, output_filename="flight_metadata.csv"):
    if not results:
        print("No data to save.")
        return

    headers = results[0].keys()
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Successfully saved {len(results)} rows to {output_filename}")

# ==========================================
# Run it
# ==========================================
if __name__ == '__main__':
    start = time.time()
    folder_to_scan = r"C:\Users\Arnav\Desktop\programming\flight_tracker\raw_adsb\2026.02.22\traces"
    base_path = Path(folder_to_scan)
    
    print(f"Scanning base folder: {folder_to_scan}\n")
    
    # Gather all immediate subdirectories
    subfolders = [f for f in base_path.iterdir() if f.is_dir()]
    
    if not subfolders:
        print("No subfolders found to process.")
    else:
        all_results = []
        total_missing = 0
        total_errors = 0
        
        print(f"Distributing {len(subfolders)} folders across available CPU cores...")
        
        # ProcessPoolExecutor manages the core pooling safely
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # Submit all folder paths to the executor
            futures = {executor.submit(process_single_folder, folder): folder for folder in subfolders}
            
            # Use tqdm to track completed folders as they return to the main process
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(subfolders), desc="Folders Processed"):
                try:
                    res, missing, errors = future.result()
                    # Collate data safely in the main thread
                    all_results.extend(res)
                    total_missing += missing
                    total_errors += errors
                except Exception as exc:
                    folder = futures[future]
                    print(f"Folder {folder.name} generated an exception: {exc}")

        if total_errors > 0:
            print(f"\nEncountered errors on {total_errors} files (likely corrupted or not actually gzip).")
            
        print(f"\nFound {len(all_results)} aircraft with full metadata.")
        print(f"Skipped {total_missing} aircraft (no callsign found).")

        save_to_csv(all_results, "full_day_flight_metadata.csv")

        stop = time.time()

        print(f"Total time {stop-start}s")