import os
import json
import glob
import re
import gzip
import pandas as pd
import airportsdata
from tqdm import tqdm  # Progress bar

# 1. Load Airport Data
print("Loading airport database...")
airports = airportsdata.load('IATA')

def find_nearest_airport(lat, lon, max_dist_km=25):
    nearest_code = None
    min_dist = float('inf')
    # Simple bounding box filter for speed
    for code, data in airports.items():
        if abs(data['lat'] - lat) > 1 or abs(data['lon'] - lon) > 1:
            continue
        dist = (data['lat'] - lat)**2 + (data['lon'] - lon)**2
        if dist < min_dist:
            min_dist = dist
            nearest_code = code
    return nearest_code

carrier_pattern = re.compile(r"^[A-Z]{3}\d+$")
routes = []
data_dir = r"C:\Users\Arnav\Desktop\programming\flight_tracker\raw_adsb\2026.02.22\traces" 

# 2. Get File List
print("Scanning for files...")
files = glob.glob(f"{data_dir}/**/*.json", recursive=True)
print(f"Found {len(files)} files. Starting processing...")

# 3. Process with Progress Bar
# 'unit' tells tqdm to count 'files'
for filepath in tqdm(files, desc="Extracting Routes", unit="file"):
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
        
        # 1. Broaden callsign and data extraction
        callsign = (data.get('flight') or data.get('callsign') or "").strip()
        trace = data.get('trace', [])
        
        # 2. Extract only points that have valid Lat (index 1) and Lon (index 2)
        valid_points = [
            p for p in trace 
            if isinstance(p, list) and len(p) > 2 and p[1] is not None and p[2] is not None
        ]

        # 3. Process if we have a path
        if len(valid_points) >= 2:
            start, end = valid_points[0], valid_points[-1]
            origin = find_nearest_airport(start[1], start[2])
            dest = find_nearest_airport(end[1], end[2])
            
            if origin and dest and origin != dest:
                routes.append({
                    'Callsign': callsign,
                    'Origin': origin,
                    'Destination': dest,
                    'Aircraft': data.get('t', 'Unknown')
                })

    except Exception as e:
        print(f"Error: {e}")
        continue

# 4. Save and Finalize
if routes:
    df = pd.DataFrame(routes).drop_duplicates()
    df.to_csv("carrier_routes_found.csv", index=False)
    print(f"\nSuccess! Found {len(df)} unique routes. Saved to carrier_routes_found.csv")
else:
    print("\nNo valid routes found. Check if the files contain flight and trace data.")