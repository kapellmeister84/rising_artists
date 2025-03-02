import streamlit as st
import requests
import datetime
import json
from concurrent.futures import ThreadPoolExecutor

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

notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#########################
# Caching Notion-Daten: Songs-Metadaten inkl. Measurements (wenn vorhanden)
#########################
@st.cache_data(show_spinner=False)
def get_measurement_details(measurement_id):
    """
    Ruft für eine Measurement-Seite (ID) die Details ab.
    Erwartete Properties in der Measurements-Datenbank:
      - Song Pop (number)
      - Artist Pop (number)
      - Streams (number)
      - Monthly Listeners (number)
      - Artist Followers (number)
    """
    url = f"{notion_page_endpoint}/{measurement_id}"
    response = requests.get(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    props = data.get("properties", {})
    return {
        "song_pop": props.get("Song Pop", {}).get("number"),
        "artist_pop": props.get("Artist Pop", {}).get("number"),
        "streams": props.get("Streams", {}).get("number"),
        "monthly_listeners": props.get("Monthly Listeners", {}).get("number"),
        "artist_followers": props.get("Artist Followers", {}).get("number")
    }

@st.cache_data(show_spinner=False)
def get_songs_metadata():
    """
    Ruft alle Einträge aus der Songs-Datenbank ab und extrahiert folgende Properties:
      - Track Name (title)
      - Artist Name (rich_text)
      - Artist ID (rich_text)
      - Track ID (rich_text)
      - Release Date (date)
      - Country Code (rich_text)
      - Measurements (relation) – Falls vorhanden, werden die verknüpften Measurement-Details parallel abgefragt.
    """
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
    with ThreadPoolExecutor() as executor:
        measurement_futures = {}
        for page in pages:
            props = page.get("properties", {})

            # Track Name (title)
            track_name = ""
            if "Track Name" in props and props["Track Name"].get("title"):
                track_name = "".join([t.get("plain_text", "") for t in props["Track Name"]["title"]]).strip()

            # Artist Name (rich_text)
            artist_name = ""
            if "Artist Name" in props and props["Artist Name"].get("rich_text"):
                artist_name = "".join([t.get("plain_text", "") for t in props["Artist Name"]["rich_text"]]).strip()

            # Artist ID (rich_text)
            artist_id = ""
            if "Artist ID" in props and props["Artist ID"].get("rich_text"):
                artist_id = "".join([t.get("plain_text", "") for t in props["Artist ID"]["rich_text"]]).strip()

            # Track ID (rich_text)
            track_id = ""
            if "Track ID" in props and props["Track ID"].get("rich_text"):
                track_id = "".join([t.get("plain_text", "") for t in props["Track ID"]["rich_text"]]).strip()

            # Release Date (date)
            release_date = ""
            if "Release Date" in props and props["Release Date"].get("date"):
                release_date = props["Release Date"]["date"].get("start", "")

            # Country Code (rich_text)
            country_code = ""
            if "Country Code" in props and props["Country Code"].get("rich_text"):
                country_code = "".join([t.get("plain_text", "") for t in props["Country Code"]["rich_text"]]).strip()

            # Measurements: Relation Property
            measurements_ids = []
            if "Measurements" in props and props["Measurements"].get("relation"):
                for rel in props["Measurements"]["relation"]:
                    m_id = rel.get("id")
                    if m_id:
                        measurements_ids.append(m_id)
                        measurement_futures[m_id] = executor.submit(get_measurement_details, m_id)
            key = track_id if track_id else page.get("id")
            metadata[key] = {
                "page_id": page.get("id"),
                "track_name": track_name,
                "artist_name": artist_name,
                "artist_id": artist_id,
                "track_id": track_id,
                "release_date": release_date,
                "country_code": country_code,
                "measurements_ids": measurements_ids
            }
        # Ergänze Measurement-Details
        for measurement_id, future in measurement_futures.items():
            try:
                details = future.result()
            except Exception as e:
                details = {"song_pop": None, "artist_pop": None, "streams": None, "monthly_listeners": None, "artist_followers": None}
            for key, song_data in metadata.items():
                if measurement_id in song_data.get("measurements_ids", []):
                    if "measurements" not in song_data:
                        song_data["measurements"] = []
                    song_data["measurements"].append({"id": measurement_id, **details})
    return metadata

# Laden der Songs-Metadaten inkl. Measurements
songs_metadata = get_songs_metadata()

#########################
# Spotify-Konfiguration
#########################
SPOTIFY_CLIENT_ID = st.secrets["spotify"]["client_id"]
SPOTIFY_CLIENT_SECRET = st.secrets["spotify"]["client_secret"]

def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("accessToken")

SPOTIFY_TOKEN = get_spotify_token()

# Für Playcount: Verwende exakt die Technik aus deinem Scanner-Script
def get_spotify_playcount(track_id, token):
    variables = json.dumps({"uri": f"spotify:track:{track_id}"})
    extensions = json.dumps({
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "26cd58ab86ebba80196c41c3d48a4324c619e9a9d7df26ecca22417e0c50c6a4"
        }
    })
    params = {"operationName": "getTrack", "variables": variables, "extensions": extensions}
    url = "https://api-partner.spotify.com/pathfinder/v1/query"
    headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return int(data["data"]["trackUnion"].get("playcount", 0))

def get_spotify_popularity(track_id, token):
    """Holt den Song-Popularity-Wert (0-100) über die offizielle API."""
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("popularity", 0)

def update_song_data(song, token):
    """
    Ruft über die Spotify-API die aktuellen Song-Daten ab.
    Für:
      - Song Pop und Playcount: 
          - Song Pop holen wir über die offizielle API (get_spotify_popularity)
          - Playcount (Streams) über die persistierte Query (get_spotify_playcount)
      - Artist Pop und Follower sowie Country Code: direkt von der offiziellen Artist-API bzw. Album-Daten.
    """
    # Abruf der Track-Daten
    url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        # Country Code aus den Album-Daten (erste verfügbare Market)
        available_markets = data.get("album", {}).get("available_markets", [])
        country_code = available_markets[0] if available_markets else ""
        # Song Pop über offizielle API
        song_pop = get_spotify_popularity(song["track_id"], token)
        # Artist-Daten abrufen
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
        # Playcount (Streams) über persistierte Query
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

def update_song_details():
    """
    Iteriert über alle Songs im Cache (songs_metadata) und aktualisiert deren Details
    (Song Pop, Artist Pop, Country Code, Artist Followers, Streams) in der Notion-Songs-Datenbank.
    """
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

#########################
# Sidebar Buttons
#########################
st.sidebar.title("Songs Cache")
if st.sidebar.button("Get New Music"):
    get_songs_metadata.clear()
    st.experimental_rerun()

if st.sidebar.button("Get Data"):
    msgs = update_song_details()
    for m in msgs:
        st.write(m)

#########################
# Anzeige der geladenen Songs-Metadaten
#########################
st.title("Songs Metadata from Notion (with Measurements)")
st.write("Loaded songs metadata:")
st.write(songs_metadata)
