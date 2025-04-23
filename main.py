import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
USERNAME = os.getenv("USERNAME")
URL = os.getenv("URL")

response = requests.get(os.getenv(URL))
data = response.json()

tracks = data.get("recenttracks", {}).get("track", [])

for track in tracks:
    print(
        {
            "user_id": USERNAME,
            "track_name": track.get("name"),
            "artist": track.get("artist", {}).get("#text"),
            "album": track.get("album", {}).get("#text"),
            "played_at": track.get("date", {}).get("#text"),
        }
    )
