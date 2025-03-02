import streamlit as st
import requests
import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor

# Optional: Hintergrund und Dark Mode (aus utils, falls vorhanden)
from utils import set_background, set_dark_mode
st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

#########################
# Notion-Konfiguration  #
#########################
# Neue Datenbankversion: "song-database" für Songs
songs_database_id = st.secrets["notion"]["song-database"]
notion_secret = st.secrets["notion"]["secret"]

# Endpunkt und Header für Notion
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#########################
# Spotify-Konfiguration
#########################
playlist_ids = st.secrets["spotify"]["playlist_ids"]
spotify_client_id = st.secrets["spotify"]["client_id"]
spotify_client_secret = st.secrets["spotify"]["client_secret"]

def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("accessToken")

SPOTIFY_TOKEN = get_spotify_token()

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
            # Extrahiere Artist ID des ersten Künstlers (falls vorhanden)
            artist_id = artists[0].get("id") if artists and artists[0].get("id") else ""
            track_id = track.get("id")
            # Streams werden hier weiterhin als 0 gesetzt (später nicht abgerufen)
            streams = 0
            # Release Date und Country Code aus den Album-Daten
            release_date = track.get("album", {}).get("release_date", "")
            available_markets = track.get("album", {}).get("available_markets", [])
            country_code = available_markets[0] if available_markets else ""
            songs.append({
                "song_name": song_name,
                "artist_name": artist_name,
                "artist_id": artist_id,
                "track_id": track_id,
                "streams": streams,
                "release_date": release_date,
                "country_code": country_code
            })
    return songs

def song_exists_in_notion(track_id):
    payload = {
        "filter": {
            "property": "Track ID",
            "rich_text": {"equals": track_id}
        }
    }
    response = requests.post(f"{notion_query_endpoint}/{songs_database_id}/query", headers=notion_headers, json=payload)
    if response.status_code == 200:
        results = response.json().get("results", [])
        return len(results) > 0
    else:
        st.error("Fehler beim Abfragen der Notion-Datenbank: " + response.text)
        return False

def create_notion_page(song_data):
    now_iso = datetime.datetime.now().isoformat()
    payload = {
        "parent": {"database_id": songs_database_id},
        "properties": {
            "Track Name": {
                "title": [{ "text": { "content": song_data["song_name"] or "Unbekannt" } }]
            },
            "Artist Name": {
                "rich_text": [{ "text": { "content": song_data["artist_name"] or "Unbekannt" } }]
            },
            "Artist ID": {
                "rich_text": [{ "text": { "content": song_data["artist_id"] or "" } }]
            },
            "Track ID": {
                "rich_text": [{ "text": { "content": song_data["track_id"] or "" } }]
            },
            "Date created": {
                "date": { "start": now_iso }
            },
            "Last time edited": {
                "date": { "start": now_iso }
            },
            # Neue Felder: Release Date und Country Code
            "Release Date": {
                "date": { "start": song_data.get("release_date", "") }
            },
            "Country Code": {
                "rich_text": [{ "text": { "content": song_data.get("country_code", "") } }]
            }
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        st.write(f"Erstellt: {song_data['song_name']}")
    else:
        st.write(f"Fehler beim Erstellen von {song_data['song_name']}: {response.text}")

def run_get_new_music():
    spotify_token = get_spotify_token()
    st.write("Spotify Access Token:", spotify_token)
    all_songs = []
    for pid in playlist_ids:
        songs = get_playlist_songs(pid, spotify_token)
        all_songs.extend(songs)
    st.write(f"Gesammelte Songs: {len(all_songs)}")
    for song in all_songs:
        if song["track_id"]:
            if song_exists_in_notion(song["track_id"]):
                st.write(f"{song['song_name']} von {song['artist_name']} existiert bereits.")
            else:
                st.write(f"{song['song_name']} von {song['artist_name']} (Artist ID: {song['artist_id']}, Release Date: {song.get('release_date','')}, Country Code: {song.get('country_code','')}) wird erstellt.")
                create_notion_page(song)
        else:
            st.write(f"{song['song_name']} hat keine Track ID und wird übersprungen.")

#########################
# Songs-Metadaten laden (Cache)
#########################
@st.cache_data(show_spinner=False)
def get_songs_metadata():
    url = f"{notion_query_endpoint}/{songs_database_id}/query"
    payload = {"page_size": 100}
    pages = []
    has_more = True
    start_cursor = None
    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=notion_headers, json=payload)
        response.raise_for_status()
        data = response.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
    metadata = {}
    for page in pages:
        props = page.get("properties", {})
        track_name = ""
        if "Track Name" in props and props["Track Name"].get("title"):
            track_name = "".join([t.get("plain_text", "") for t in props["Track Name"]["title"]]).strip()
        artist_name = ""
        if "Artist Name" in props and props["Artist Name"].get("rich_text"):
            artist_name = "".join([t.get("plain_text", "") for t in props["Artist Name"]["rich_text"]]).strip()
        artist_id = ""
        if "Artist ID" in props and props["Artist ID"].get("rich_text"):
            artist_id = "".join([t.get("plain_text", "") for t in props["Artist ID"]["rich_text"]]).strip()
        track_id = ""
        if "Track ID" in props and props["Track ID"].get("rich_text"):
            track_id = "".join([t.get("plain_text", "") for t in props["Track ID"]["rich_text"]]).strip()
        release_date = ""
        if "Release Date" in props and props["Release Date"].get("date"):
            release_date = props["Release Date"]["date"].get("start", "")
        country_code = ""
        if "Country Code" in props and props["Country Code"].get("rich_text"):
            country_code = "".join([t.get("plain_text", "") for t in props["Country Code"]["rich_text"]]).strip()
        key = track_id if track_id else page.get("id")
        metadata[key] = {
            "page_id": page.get("id"),
            "track_name": track_name,
            "artist_name": artist_name,
            "artist_id": artist_id,
            "track_id": track_id,
            "release_date": release_date,
            "country_code": country_code
        }
    return metadata

# Lade die aktuellen Songs-Metadaten aus der Notion-Datenbank
songs_metadata = get_songs_metadata()

#########################
# Sidebar Buttons
#########################
st.sidebar.title("Songs Cache")
if st.sidebar.button("Get New Music"):
    run_get_new_music()
    # Cache löschen: damit beim nächsten Aufruf die neuen Daten geladen werden
    get_songs_metadata.clear()
    st.info("Neue Musik wurde verarbeitet. Bitte Seite neu laden, um die aktualisierten Daten zu sehen.")

if st.sidebar.button("Get Data"):
    # Button zum Auffüllen der Song-Details (Song Pop, Artist Pop, Country Code, Artist Followers, Streams)
    def update_song_data(song, token):
        url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            available_markets = data.get("album", {}).get("available_markets", [])
            country_code = available_markets[0] if available_markets else ""
            song_pop = get_spotify_popularity(song["track_id"], token)
            artists = data.get("artists", [])
            artist_id = artists[0].get("id") if artists and artists[0].get("id") else ""
            artist_pop = 0
            artist_followers = 0
            if artist_id:
                artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
                artist_resp = requests.get(artist_url, headers={"Authorization": f"Bearer {token}"})
                if artist_resp.status_code == 200:
                    artist_data = artist_resp.json()
                    artist_pop = artist_data.get("popularity", 0)
                    artist_followers = artist_data.get("followers", {}).get("total", 0)
            streams = get_spotify_playcount(song["track_id"], token)
            return {
                "song_pop": song_pop,
                "artist_pop": artist_pop,
                "country_code": country_code,
                "artist_followers": artist_followers,
                "streams": streams
            }
        else:
            st.error(f"Error fetching data for track {song['track_id']}: {response.text}")
            return {}

    def get_spotify_popularity(track_id, token):
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("popularity", 0)

    def update_song_details():
        spotify_token = get_spotify_token()
        messages = []
        for key, song in songs_metadata.items():
            if song.get("track_id"):
                details = update_song_data(song, spotify_token)
                page_id = song.get("page_id")
                if not page_id:
                    continue
                payload = {
                    "properties": {
                        "Song Pop": {"number": details.get("song_pop", 0)},
                        "Artist Pop": {"number": details.get("artist_pop", 0)},
                        "Artist Followers": {"number": details.get("artist_followers", 0)},
                        "Country Code": {"rich_text": [{"text": {"content": details.get("country_code", "")}}]},
                        "Streams": {"number": details.get("streams", 0)}
                    }
                }
                url = f"{notion_page_endpoint}/{page_id}"
                response = requests.patch(url, headers=notion_headers, json=payload)
                if response.status_code == 200:
                    messages.append(f"Updated details for {song.get('track_name')}")
                else:
                    messages.append(f"Error updating details for {song.get('track_name')}: {response.text}")
        return messages

    msgs = update_song_details()
    for m in msgs:
        st.write(m)

#########################
# Anzeige der geladenen Songs-Metadaten
#########################
st.title("Songs Metadata from Notion (with Measurements)")
st.write("Loaded songs metadata:")
st.write(songs_metadata)
