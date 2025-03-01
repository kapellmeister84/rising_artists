import streamlit as st
import requests
import datetime
import json
import time
import pandas as pd
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")

# === Notion-Konfiguration ===
tracking_db_id = "1a9b6204cede80e29338ede2c76999f2"  # Weeks-Datenbank mit "Date", "Popularity Score" und Relation "Song"
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === Spotify-Konfiguration ===
def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("accessToken")

SPOTIFY_TOKEN = get_spotify_token()

# --- Hilfsfunktionen (z.B. parse_rollup_text etc.) ---
def parse_rollup_text(rollup):
    texts = []
    if rollup and "array" in rollup:
        for item in rollup["array"]:
            if item.get("type") == "rich_text":
                for sub in item.get("rich_text", []):
                    texts.append(sub.get("plain_text", ""))
            elif item.get("type") == "date":
                date_info = item.get("date", {})
                if date_info.get("start"):
                    texts.append(date_info["start"])
    return " ".join(texts).strip()

def create_new_measurement(song_id, popularity_score):
    """
    Legt einen neuen Eintrag in der Weeks-Datenbank an.
    Verwendet als Titel/Name einen eindeutigen Code: "Song-<song_id>-<YYYYMMDD-HHMMSS>"
    """
    now_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    unique_code = f"Song-{song_id}-{now_str}"
    # Du kannst die Property, in die du den Code schreibst, anpassen (z.B. "Name", "Week", o.ä.)
    # Hier heißt die Title-Property "Week"
    
    payload = {
        "parent": {"database_id": tracking_db_id},
        "properties": {
            "Week": {
                "title": [
                    {"text": {"content": unique_code}}
                ]
            },
            "Song": {
                "relation": [
                    {"id": song_id}
                ]
            },
            "Popularity Score": {
                "number": popularity_score
            },
            "Date": {
                "date": {
                    # Speichere das aktuelle Datum/Uhrzeit
                    "start": datetime.datetime.now().strftime("%Y/%m/%d %H:%M")
                }
            }
            # Falls du Growth o.ä. befüllen willst, kommt das hier rein
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, data=json.dumps(payload))
    if response.status_code != 200:
        st.error(f"Fehler beim Erstellen einer neuen Messung: {response.text}")
    else:
        st.success(f"Neuer Eintrag mit Code '{unique_code}' erstellt.")

# Deine bestehenden get_tracking_entries / get_metadata_from_tracking_db etc.
# kannst du beibehalten. Wir fokussieren uns hier auf das 'Update Popularity'.


def update_popularity():
    """
    Beispiel: Für jeden Song in der DB einen neuen Messwert anlegen,
    mit einem eindeutigen Code anstelle von 'Week of 2025-03-01'.
    """
    st.write("Füge neue Popularity-Messung hinzu...")
    # Hier könntest du z.B. erst alle Songs abfragen und ihre aktuellen Popularity Scores ermitteln
    # oder du nimmst an, du hast eine Liste von (song_id, popularity) ...
    
    # Im einfachsten Fall: Hardcode oder simuliere 2 Songs
    example_songs = [
        {"song_id": "abc123", "pop": 10},
        {"song_id": "def456", "pop": 30}
    ]
    
    for song in example_songs:
        create_new_measurement(song["song_id"], song["pop"])
        time.sleep(0.5)
    
    st.success("Popularity wurde aktualisiert und neue Einträge mit eindeutigem Code angelegt!")

# --- Streamlit App ---
st.title("Song Tracking Übersicht")

if st.button("Update Popularity"):
    update_popularity()
st.write("Hier würde dann dein restliches Script folgen ...")
