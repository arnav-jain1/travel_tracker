import requests
import json

callsign = "DL1"
url = f"https://api.adsbdb.com/v0/callsign/{callsign}"

r = requests.get(url, timeout=10)
r.raise_for_status()

print(json.dumps(r.json(), indent=2))