import streamlit as st
import requests
import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor

from utils import set_background, set_dark_mode

st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

#########################
# Notion-Konfiguration  #
#########################
# Songs-Datenbank (song-database) und Measurements-Datenbank (measurements-database)
songs_database_id = st.secrets["notion"]["song-database"]
measurements_db_id = st.secrets["notion"]["measurements-database"]
notion_secret = st.secrets["notion"]["secret"]

notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#########################
# Caching Notion-Daten: Songs-Metadaten inkl. Measurements
#########################
@st.cache_data(show_spinner=False)
def get_measurement_details(measurement_id):
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

            # Track Name
            track_name = ""
            if "Track Name" in props and props["Track Name"].get("title"):
                track_name = "".join([t.get("plain_text", "") for t in props["Track Name"]["title"]]).strip()

            # Artist Name
            artist_name = ""
            if "Artist Name" in props and props["Artist Name"].get("rich_text"):
                artist_name = "".join([t.get("plain_text", "") for t in props["Artist Name"]["rich_text"]]).strip()

            # Artist ID
            artist_id = ""
            if "Artist ID" in props and props["Artist ID"].get("rich_text"):
                artist_id = "".join([t.get("plain_text", "") for t in props["Artist ID"]["rich_text"]]).strip()

            # Track ID
            track_id = ""
            if "Track ID" in props and props["Track ID"].get("rich_text"):
                track_id = "".join([t.get("plain_text", "") for t in props["Track ID"]["rich_text"]]).strip()

            # Release Date
            release_date = ""
            if "Release Date" in props and props["Release Date"].get("date"):
                release_date = props["Release Date"]["date"].get("start", "")

            # Country Code
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

songs_metadata = get_songs_metadata()
st.title("Songs Metadata from Notion (with Measurements)")
st.write("Loaded songs metadata:")
st.write(songs_metadata)

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

# Für Playcount: Genau wie in deinem Scanner-Script
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
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("popularity", 0)

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

#########################
# Neue Funktion: Neue Measurement-Einträge anlegen
#########################
def create_measurement_entry(song, details):
    """
    Legt einen neuen Measurement-Eintrag in der Measurements-Datenbank an.
    Dieser Eintrag wird NICHT überschrieben, sondern ergänzt die History.
    Die Felder sind:
      - Name: z.B. aktueller Zeitstempel als Text
      - Song Pop, Artist Pop, Streams, Monthly Listeners, Artist Followers
    """
    now = datetime.datetime.now().isoformat()
    payload = {
        "parent": {"database_id": measurements_db_id},
        "properties": {
            "Name": {"title": [{"text": {"content": f"Measurement {now}"}}]},
            "Song Pop": {"number": details.get("song_pop", 0)},
            "Artist Pop": {"number": details.get("artist_pop", 0)},
            "Streams": {"number": details.get("streams", 0)},
            "Monthly Listeners": {"number": 0},  # Falls benötigt, hier evtl. noch ergänzen
            "Artist Followers": {"number": details.get("artist_followers", 0)}
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    response.raise_for_status()
    new_id = response.json().get("id")
    return new_id

def update_song_measurements_relation(page_id, new_measurement_id):
    """
    Liest die bestehende Relation "Measurements" der Song-Seite aus und fügt die neue Measurement-ID hinzu.
    """
    # Hole die aktuelle Seite
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    response.raise_for_status()
    page = response.json()
    props = page.get("properties", {})
    current_relations = []
    if "Measurements" in props and props["Measurements"].get("relation"):
        current_relations = props["Measurements"]["relation"]
    # Füge die neue Relation hinzu, wenn noch nicht vorhanden
    if not any(rel.get("id") == new_measurement_id for rel in current_relations):
        current_relations.append({"id": new_measurement_id})
    # Aktualisiere die Seite
    payload = {
        "properties": {
            "Measurements": {"relation": current_relations}
        }
    }
    patch_resp = requests.patch(url, headers=notion_headers, json=payload)
    patch_resp.raise_for_status()

#########################
# Neue Funktion: "Fill Song Details" – erstellt neue Measurement-Einträge
#########################
def fill_song_measurements():
    spotify_token = get_spotify_token()
    messages = []
    # Für jedes Lied aus dem Cache:
    for key, song in songs_metadata.items():
        if song.get("track_id"):
            details = update_song_data(song, spotify_token)
            # Lege neuen Measurement-Eintrag an:
            new_meas_id = create_measurement_entry(song, details)
            # Aktualisiere die Relation auf der Song-Seite (History erweitern)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            messages.append(f"Neue Measurement für {song.get('track_name')} erstellt (ID: {new_meas_id})")
    return messages

#########################
# Measurements-Datenbank-ID
#########################
measurements_db_id = st.secrets["notion"]["measurements-database"]

#########################
# Sidebar Buttons
#########################
st.sidebar.title("Songs Cache")
if st.sidebar.button("Get New Music"):
    # Führe den "Get New Music"-Prozess aus (wie bisher)
    # Hier verwenden wir deinen existierenden Code
    def run_get_new_music():
        spotify_token = get_spotify_token()
        st.write("Spotify Access Token:", spotify_token)
        all_songs = []
        for pid in st.secrets["spotify"]["playlist_ids"]:
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
    run_get_new_music()
    st.info("Get New Music abgeschlossen. Bitte Seite neu laden, um die aktualisierten Daten zu sehen.")

if st.sidebar.button("Get Data"):
    msgs = fill_song_measurements()
    for m in msgs:
        st.write(m)

#########################
# Anzeige der geladenen Songs-Metadaten
#########################
st.title("Songs Metadata from Notion (with Measurements)")
st.write("Loaded songs metadata:")
st.write(songs_metadata)
