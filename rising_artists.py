import streamlit as st
import requests
import datetime
import base64

# --- Notion Einstellungen aus secrets.toml ---
# Eindeutiger Name für die Song-Datenbank
song_database_id = st.secrets["notion"]["song-database"]
notion_secret = st.secrets["notion"]["secret"]
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_query_endpoint = f"https://api.notion.com/v1/databases/{song_database_id}/query"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- Spotify Einstellungen aus secrets.toml ---
playlist_ids = st.secrets["spotify"]["playlist_ids"]
spotify_client_id = st.secrets["spotify"]["client_id"]
spotify_client_secret = st.secrets["spotify"]["client_secret"]

def get_spotify_token():
    """
    Ruft einen Spotify Access Token über den Client Credentials Flow ab.
    """
    auth_str = f"{spotify_client_id}:{spotify_client_secret}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()
    headers = {
        "Authorization": f"Basic {b64_auth_str}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def get_playlist_songs(playlist_id, token):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    response = requests.get(url, headers=headers)
    data = response.json()
    songs = []
    for item in data.get("tracks", {}).get("items", []):
        track = item.get("track")
        if track:
            song_name = track.get("name")
            artists = track.get("artists", [])
            artist_names = [artist.get("name") for artist in artists]
            artist_name = ", ".join(artist_names)
            # Extrahiere die Artist ID des ersten Künstlers (falls vorhanden)
            artist_id = artists[0].get("id") if artists and artists[0].get("id") else ""
            track_id = track.get("id")
            songs.append({
                "song_name": song_name,
                "artist_name": artist_name,
                "artist_id": artist_id,
                "track_id": track_id,
            })
    return songs

def song_exists_in_notion(track_id):
    payload = {
        "filter": {
            "property": "Track ID",
            "rich_text": {
                "equals": track_id
            }
        }
    }
    response = requests.post(notion_query_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        results = response.json().get("results", [])
        return len(results) > 0
    else:
        st.error("Fehler beim Abfragen der Notion-Datenbank: " + response.text)
        return False

def create_notion_page(song_data):
    now_iso = datetime.datetime.now().isoformat()
    payload = {
        "parent": { "database_id": song_database_id },
        "properties": {
            "Track Name": {
                "title": [
                    { "text": { "content": song_data["song_name"] or "Unbekannt" } }
                ]
            },
            "Artist Name": {
                "rich_text": [
                    { "text": { "content": song_data["artist_name"] or "Unbekannt" } }
                ]
            },
            "Artist ID": {
                "rich_text": [
                    { "text": { "content": song_data["artist_id"] or "" } }
                ]
            },
            "Track ID": {
                "rich_text": [
                    { "text": { "content": song_data["track_id"] or "" } }
                ]
            },
            "Date created": {
                "date": { "start": now_iso }
            },
            "Last time edited": {
                "date": { "start": now_iso }
            }
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        return f"Erstellt: {song_data['song_name']}"
    else:
        return f"Fehler beim Erstellen von {song_data['song_name']}: {response.text}"

def get_new_music():
    spotify_token = get_spotify_token()
    st.write("Spotify Access Token: ", spotify_token)
    all_songs = []
    for pid in playlist_ids:
        songs = get_playlist_songs(pid, spotify_token)
        all_songs.extend(songs)
    st.write(f"Gesammelte Songs: {len(all_songs)}")
    results = []
    for song in all_songs:
        if song["track_id"]:
            if song_exists_in_notion(song["track_id"]):
                results.append(f"{song['song_name']} von {song['artist_name']} existiert bereits.")
            else:
                res = create_notion_page(song)
                results.append(res)
        else:
            results.append(f"{song['song_name']} hat keine Track ID und wird übersprungen.")
    return results

st.title("Spotify zu Notion Music Sync")

# Button in der Seitenleiste zum Triggern des Scripts
if st.sidebar.button("Get New Music"):
    st.write("Lade neue Musik...")
    messages = get_new_music()
    for msg in messages:
        st.write(msg)
