import requests
import time
import json
import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.oauth2 import service_account

# Load .env variables
load_dotenv()

# Load environment config
LAST_FM_API_KEY = os.getenv("LAST_FM_API_KEY")
LAST_FM_USERNAME = os.getenv("LAST_FM_USERNAME")
LIMIT = os.getenv("LIMIT", "5")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = "scenic-foundry-457704-m9"
TOPIC_ID = "lasfm-trackflow-topic"

# Last.fm API URL
URL = (
    f"https://ws.audioscrobbler.com/2.0/"
    f"?method=user.getrecenttracks&user={LAST_FM_USERNAME}"
    f"&api_key={LAST_FM_API_KEY}&format=json&limit={LIMIT}"
)

print("✅ Config loaded")
print("🔑 Service account:", GOOGLE_CREDENTIALS_PATH)
print("🎯 Target topic:", f"{PROJECT_ID}/{TOPIC_ID}")


def get_recent_tracks(user):
    response = requests.get(
        f"https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user={user}&api_key={LAST_FM_API_KEY}&format=json&limit={LIMIT}"
    )
    if response.status_code == 200:
        return response.json().get("recenttracks", {}).get("track", [])
    else:
        print(f"❌ Error fetching tracks: {response.status_code}")
        return []


def publish_to_pubsub(track):
    data = {
        "user_id": LAST_FM_USERNAME,
        "track_name": track.get("name"),
        "artist": track.get("artist", {}).get("#text"),
        "album": track.get("album", {}).get("#text"),
        "played_at": track.get("date", {}).get("#text"),
    }

    if not data["played_at"]:
        return

    message = json.dumps(data).encode("utf-8")

    try:
        # Load credentials fresh every time to avoid token reuse/thread issues
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_PATH
        )
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

        future = publisher.publish(topic_path, message)
        print("✅ Published:", data)
        return future.result()
    except Exception as e:
        print("❌ Publish failed:", e)


if __name__ == "__main__":
    seen = set()
    while True:
        tracks = get_recent_tracks(LAST_FM_USERNAME)
        for track in tracks:
            unique_id = track.get("date", {}).get("uts")
            if unique_id and unique_id not in seen:
                seen.add(unique_id)
                publish_to_pubsub(track)
        time.sleep(30)
