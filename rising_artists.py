import streamlit as st
import requests
import datetime
import json
import time

# --- Notion Einstellungen ---
notion_database_id = "1a9b6204cede8006b67fd247dc660ba4"
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_query_endpoint = f"https://api.notion.com/v1/databases/{notion_database_id}/query"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- Spotify Einstellungen ---
def get_spotify_token():
    response = requests.get("https://open.spotify.com/get_access_token?reason=transport&productType=web_player").json()
    return response["accessToken"]

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
            # Hier werden Streams weiterhin als 0 gesetzt
            streams = 0
            # Ergänze hier Release Date und Country Code aus den Album-Daten:
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
        print("Fehler beim Abfragen der Notion-Datenbank:", response.text)
        return False

def create_notion_page(song_data):
    now_iso = datetime.datetime.now().isoformat()
    payload = {
        "parent": { "database_id": notion_database_id },
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
            },
            # Neue Felder:
            "Release Date": {
                "date": { "start": song_data.get("release_date", "") }
            },
            "Country Code": {
                "rich_text": [
                    { "text": { "content": song_data.get("country_code", "") } }
                ]
            }
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        print(f"Erstellt: {song_data['song_name']}")
    else:
        print(f"Fehler beim Erstellen von {song_data['song_name']}: {response.text}")

# Playlist-IDs für die drei Playlists
playlist_ids = [
    "37i9dQZF1DWUW2bvSkjcJ6",
    "7jLtJrdChX6rXZ39SLVMKD",
    "37i9dQZF1DX3crNbt46mRU"
]

spotify_token = get_spotify_token()
print("Spotify Access Token:", spotify_token)

all_songs = []
for pid in playlist_ids:
    songs = get_playlist_songs(pid, spotify_token)
    all_songs.extend(songs)

print(f"Gesammelte Songs: {len(all_songs)}")
for song in all_songs:
    if song["track_id"]:
        if song_exists_in_notion(song["track_id"]):
            print(f"{song['song_name']} von {song['artist_name']} existiert bereits.")
        else:
            print(f"{song['song_name']} von {song['artist_name']} (Artist ID: {song['artist_id']}, Release Date: {song.get('release_date','')}, Country Code: {song.get('country_code','')}) wird erstellt.")
            create_notion_page(song)
    else:
        print(f"{song['song_name']} hat keine Track ID und wird übersprungen.")
