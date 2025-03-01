import streamlit as st
import schedule
import threading
import time
import requests
import json
import datetime

# ============================
# Script A: Sync Songs with Spotify
# ============================

# --- Notion Einstellungen für Script A ---
NOTION_TOKEN = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
SONG_DATABASE_ID = "b94c8042619d42a3be799c9795984150"
ARTIST_DATABASE_ID = "bdd9f40550c640f3bc305ff39ff9055d"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# 🔑 Spotify Access Token holen
def get_spotify_token():
    response = requests.get("https://open.spotify.com/get_access_token?reason=transport&productType=web_player")
    response.raise_for_status()
    return response.json().get("accessToken")

SPOTIFY_TOKEN = get_spotify_token()
SPOTIFY_HEADERS = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}

# 🎤 Lade alle Artists
def get_artists():
    url = f"https://api.notion.com/v1/databases/{ARTIST_DATABASE_ID}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    artists = {}
    for page in response.json().get("results", []):
        artist_id = page["id"]
        name = page["properties"]["NAME"]["title"][0]["plain_text"] if page["properties"]["NAME"]["title"] else "Unbekannt"
        artists[artist_id] = name
    st.write(f"🔄 {len(artists)} Artists geladen.")
    return artists

# 🎶 Lade Songs mit Artists
def get_songs_with_artists(artists):
    url = f"https://api.notion.com/v1/databases/{SONG_DATABASE_ID}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    songs = []
    for page in response.json().get("results", []):
        props = page["properties"]
        # Nur "released"-Songs
        if props.get("Status", {}).get("select", {}).get("name", "").lower() != "released":
            continue
        # Song Title
        song_title = props.get("Song Title", {}).get("title", [])
        song_title = song_title[0]["plain_text"].strip() if song_title else "Unbenannter Song"
        # Track ID
        track_id_prop = props.get("Track ID", {}).get("rich_text", [])
        track_id = track_id_prop[0]["plain_text"].strip() if track_id_prop else ""
        # Artists
        artist_ids = props.get("Artist", {}).get("relation", [])
        artists_names = [artists.get(a["id"], "Unbekannt") for a in artist_ids]
        songs.append({
            "page_id": page["id"],
            "song_title": song_title,
            "track_id": track_id,
            "artists": artists_names
        })
    st.write(f"🎵 {len(songs)} Songs gefunden.")
    return songs

# 🔍 Spotify Track Suche
def search_spotify_track(song_title, artists):
    if not song_title or not artists:
        st.write(f"🚫 Fehlende Daten für Suche: Song='{song_title}', Artists='{artists}'")
        return None
    query = f'track:"{song_title}" artist:"{artists[0]}"'
    params = {"q": query, "type": "track", "limit": 1}
    response = requests.get("https://api.spotify.com/v1/search", headers=SPOTIFY_HEADERS, params=params)
    response.raise_for_status()
    tracks = response.json().get("tracks", {}).get("items", [])
    return tracks[0]["id"] if tracks else None

# 📝 Spotify-Daten holen
def get_spotify_track_details(track_id):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    response = requests.get(url, headers=SPOTIFY_HEADERS)
    response.raise_for_status()
    track = response.json()
    cover_url = track["album"]["images"][0]["url"] if track["album"]["images"] else ""
    playcount = get_spotify_playcount(track_id)
    return playcount, cover_url

# 🔢 Playcount holen
def get_spotify_playcount(track_id):
    variables = json.dumps({"uri": f"spotify:track:{track_id}"})
    extensions = json.dumps({"persistedQuery": {"version": 1, "sha256Hash": "26cd58ab86ebba80196c41c3d48a4324c619e9a9d7df26ecca22417e0c50c6a4"}})
    params = {"operationName": "getTrack", "variables": variables, "extensions": extensions}
    response = requests.get("https://api-partner.spotify.com/pathfinder/v1/query", headers=SPOTIFY_HEADERS, params=params)
    response.raise_for_status()
    return int(response.json()["data"]["trackUnion"].get("playcount", 0))

# 🔄 Notion-Properties aktualisieren
def update_notion_properties(page_id, updates):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    data = {"properties": updates}
    response = requests.patch(url, headers=HEADERS, data=json.dumps(data))
    if response.status_code != 200:
        st.write(f"🚨 Fehler beim Aktualisieren (Properties): {response.status_code} | {response.json()}")
    response.raise_for_status()

# 🖼️ Cover & Icon separat setzen
def update_notion_icon_and_cover(page_id, cover_url):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    data = {
        "icon": {"external": {"url": cover_url}},
        "cover": {"external": {"url": cover_url}}
    }
    response = requests.patch(url, headers=HEADERS, data=json.dumps(data))
    if response.status_code != 200:
        st.write(f"🚨 Fehler beim Aktualisieren (Cover/Icon): {response.status_code} | {response.json()}")
    response.raise_for_status()

def sync_songs_with_spotify():
    artists = get_artists()
    songs = get_songs_with_artists(artists)
    for song in songs:
        page_id = song["page_id"]
        song_title = song["song_title"]
        track_id = song["track_id"]
        artist_names = song["artists"]
        st.write(f"\n🎶 {song_title} | 👤 Artists: {', '.join(artist_names)}")
        if not track_id:
            st.write("🔍 Suche Track-ID...")
            found_track_id = search_spotify_track(song_title, artist_names)
            if found_track_id:
                track_id = found_track_id
                update_notion_properties(page_id, {
                    "Track ID": {"rich_text": [{"text": {"content": track_id}}]},
                    "Spotify": {"url": f"https://open.spotify.com/track/{track_id}"}
                })
                st.write(f"✅ Track ID eingetragen: {track_id}")
            else:
                st.write("🚫 Kein Track gefunden.")
                continue
        playcount, cover_url = get_spotify_track_details(track_id)
        update_notion_properties(page_id, {"Streams": {"number": playcount}})
        st.write(f"🔄 Streams: {playcount}")
        if cover_url:
            update_notion_icon_and_cover(page_id, cover_url)
            st.write("🖼️ Cover & Icon synchronisiert.")
        else:
            st.write("⚠️ Kein Cover gefunden.")

def sync_songs_with_spotify_task():
    st.write("Starte Sync Songs with Spotify...")
    try:
        sync_songs_with_spotify()
        st.write("Sync Songs with Spotify abgeschlossen.")
    except Exception as e:
        st.write(f"Fehler beim Sync Songs with Spotify: {e}")

# ============================
# Script B: Create Week Entries
# ============================

# --- Notion Einstellungen für Script B ---
songs_database_id = "1a9b6204cede8006b67fd247dc660ba4"  # Songs-Datenbank
week_database_id = "1a9b6204cede80e29338ede2c76999f2"    # Week-Datenbank

def get_all_song_page_ids():
    url = f"{notion_query_endpoint}/{songs_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    song_pages = []
    for page in data.get("results", []):
        page_id = page["id"]
        popularity = 0
        if "Popularity" in page["properties"]:
            popularity_prop = page["properties"]["Popularity"]
            popularity = popularity_prop.get("number", 0)
        song_pages.append({"page_id": page_id, "popularity": popularity})
    return song_pages

def create_week_entry(song_page_id, popularity_score):
    now_iso = datetime.datetime.now().isoformat()
    payload = {
        "parent": { "database_id": week_database_id },
        "properties": {
            "Week": { 
                "title": [
                    { "text": { "content": f"Week of {now_iso[:10]}" } }
                ]
            },
            "Song": {
                "relation": [
                    { "id": song_page_id }
                ]
            },
            "Popularity Score": {
                "number": popularity_score
            },
            "Date": {
                "date": { "start": now_iso }
            }
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        st.write(f"Erfolgreich Week Entry für Song {song_page_id} erstellt.")
    else:
        st.write(f"Fehler beim Erstellen des Week Entry für Song {song_page_id}: {response.text}")

def create_week_entries():
    song_pages = get_all_song_page_ids()
    st.write(f"Gefundene Songs: {len(song_pages)}")
    for song in song_pages:
        create_week_entry(song["page_id"], song["popularity"])

def create_week_entries_task():
    st.write("Starte Week Entries...")
    try:
        create_week_entries()
        st.write("Week Entries abgeschlossen.")
    except Exception as e:
        st.write(f"Fehler beim Erstellen der Week Entries: {e}")

# ============================
# Scheduler Setup & Streamlit App
# ============================

def scheduler_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

# Plane die Tasks:
# Jeden Freitag um 00:01
schedule.every().friday.at("00:01").do(sync_songs_with_spotify_task)
# Täglich um 00:01 und 17:00
schedule.every().day.at("00:01").do(create_week_entries_task)
schedule.every().day.at("17:00").do(create_week_entries_task)

# Starte den Scheduler in einem Hintergrund-Thread
scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
scheduler_thread.start()

st.title("Notion Spotify Sync Scheduler")
st.write("Diese App startet jeden Freitag um 00:01 den 'New Music Friday Sync'-Task und täglich um 00:01 sowie 17:00 den 'Track Analytics'-Task automatisch im Hintergrund.")
st.write("Du kannst die Tasks auch manuell über die Buttons unten starten:")

if st.button("New Music Friday Sync"):
    sync_songs_with_spotify_task()

if st.button("Track Analytics"):
    create_week_entries_task()

st.write("Scheduler läuft. Warte auf die nächste geplante Ausführung...")
