import streamlit as st
import requests
import datetime
import json
import time
import re
from concurrent.futures import ThreadPoolExecutor

from utils import set_background, set_dark_mode

st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

######################################
# Notion-Konfiguration
######################################
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

######################################
# Caching Notion-Daten: Songs-Metadaten inkl. Measurements
######################################
@st.cache_data(show_spinner=False)
def get_measurement_details(measurement_id):
    url = f"{notion_page_endpoint}/{measurement_id}"
    response = requests.get(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    props = data.get("properties", {})
    return {
        "song_pop": int(props.get("Song Pop", {}).get("number") or 0),
        "artist_pop": int(props.get("Artist Pop", {}).get("number") or 0),
        "streams": int(props.get("Streams", {}).get("number") or 0),
        "monthly_listeners": int(props.get("Monthly Listeners", {}).get("number") or 0),
        "artist_followers": int(props.get("Artist Followers", {}).get("number") or 0)
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
                details = {"song_pop": 0, "artist_pop": 0, "streams": 0, "monthly_listeners": 0, "artist_followers": 0}
            for key, song_data in metadata.items():
                if measurement_id in song_data.get("measurements_ids", []):
                    if "measurements" not in song_data:
                        song_data["measurements"] = []
                    song_data["measurements"].append({"id": measurement_id, **details})
    return metadata

songs_metadata = get_songs_metadata()

######################################
# Sidebar: Log-Fenster und Fortschrittsbalken
######################################
if "log_messages" not in st.session_state:
    st.session_state.log_messages = []

def log(msg):
    st.session_state.log_messages.append(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")
    st.sidebar.text_area("Log", "\n".join(st.session_state.log_messages), height=200)

def show_progress(progress, info):
    pb = st.sidebar.progress(progress)
    st.sidebar.write(info)
    return pb

######################################
# Spotify-Konfiguration
######################################
SPOTIFY_CLIENT_ID = st.secrets["spotify"]["client_id"]
SPOTIFY_CLIENT_SECRET = st.secrets["spotify"]["client_secret"]

def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("accessToken")

SPOTIFY_TOKEN = get_spotify_token()

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
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return int(data["data"]["trackUnion"].get("playcount", 0))
    except requests.HTTPError as e:
        log(f"Error fetching playcount for track {track_id}: {e}")
        return 0

def get_spotify_popularity(track_id, token):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("popularity", 0)
    except requests.HTTPError as e:
        log(f"Error fetching popularity for track {track_id}: {e}")
        return 0

def get_monthly_listeners_from_html(artist_id):
    url = f"https://open.spotify.com/artist/{artist_id}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "de"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        html = r.text
        match = re.search(r'([\d\.,]+)\s*(?:Hörer monatlich|monatliche Hörer)', html, re.IGNORECASE)
        if match:
            value = match.group(1)
            value = value.replace('.', '').replace(',', '')
            try:
                return int(value)
            except Exception as e:
                log(f"Fehler bei der Konvertierung der monatlichen Hörer für {artist_id}: {e}")
        else:
            log(f"Kein passender Wert auf der Seite von Artist {artist_id} gefunden.")
    else:
        log(f"Fehler beim Abrufen der Artist-Seite {artist_id}: Status {r.status_code}")
    return None

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
        artist_image = ""
        if artist_id:
            artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
            artist_resp = requests.get(artist_url, headers={"Authorization": f"Bearer {token}"})
            if artist_resp.status_code == 200:
                artist_data = artist_resp.json()
                artist_pop = artist_data.get("popularity", 0)
                artist_followers = artist_data.get("followers", {}).get("total", 0)
                if artist_data.get("images") and len(artist_data["images"]) > 0:
                    artist_image = artist_data["images"][0]["url"]
        streams = get_spotify_playcount(song["track_id"], token)
        monthly_listeners = get_monthly_listeners_from_html(artist_id)
        if monthly_listeners is None:
            monthly_listeners = artist_followers
        return {
            "song_pop": song_pop,
            "artist_pop": artist_pop,
            "country_code": country_code,
            "artist_followers": artist_followers,
            "streams": streams,
            "monthly_listeners": monthly_listeners,
            "artist_image": artist_image
        }
    else:
        st.error(f"Error fetching data for track {song['track_id']}: {response.text}")
        return {}

######################################
# Neue Funktion: Neue Measurement-Einträge anlegen und Relation aktualisieren
######################################
def create_measurement_entry(song, details):
    now = datetime.datetime.now().isoformat()
    payload = {
        "parent": {"database_id": measurements_db_id},
        "properties": {
            "Name": {"title": [{"text": {"content": f"Measurement {now}"}}]},
            "Song": {"relation": [{"id": song["page_id"]}]},
            "Song Pop": {"number": int(details.get("song_pop") or 0)},
            "Artist Pop": {"number": int(details.get("artist_pop") or 0)},
            "Streams": {"number": int(details.get("streams") or 0)},
            "Monthly Listeners": {"number": int(details.get("monthly_listeners") or 0)},
            "Artist Followers": {"number": int(details.get("artist_followers") or 0)}
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    response.raise_for_status()
    new_id = response.json().get("id")
    return new_id

def update_song_measurements_relation(page_id, new_measurement_id, retries=3):
    for attempt in range(retries):
        url = f"{notion_page_endpoint}/{page_id}"
        response = requests.get(url, headers=notion_headers)
        response.raise_for_status()
        page = response.json()
        props = page.get("properties", {})
        current_relations = []
        if "Measurements" in props and props["Measurements"].get("relation"):
            current_relations = props["Measurements"]["relation"]
        if not any(rel.get("id") == new_measurement_id for rel in current_relations):
            current_relations.append({"id": new_measurement_id})
        payload = {
            "properties": {
                "Measurements": {"relation": current_relations}
            }
        }
        patch_resp = requests.patch(url, headers=notion_headers, json=payload)
        if patch_resp.status_code == 200:
            return
        elif patch_resp.status_code == 409:
            time.sleep(1)
            continue
        else:
            patch_resp.raise_for_status()
    st.warning(f"Konnte Measurements für Seite {page_id} nach {retries} Versuchen nicht aktualisieren.")

def fill_song_measurements():
    spotify_token = get_spotify_token()
    messages = []
    for key, song in songs_metadata.items():
        if song.get("track_id"):
            details = update_song_data(song, spotify_token)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            messages.append(f"Neue Measurement für {song.get('track_name')} erstellt (ID: {new_meas_id})")
    return messages

######################################
# Hilfsfunktion: song_exists_in_notion
######################################
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

######################################
# Neue Funktion: Gruppierung der Suchergebnisse nach Artist
######################################
def group_results_by_artist(results):
    grouped = {}
    for key, song in results.items():
        # Gruppiere nach artist_id, wenn vorhanden, ansonsten nach artist_name
        group_key = song.get("artist_id") or song.get("artist_name")
        if group_key not in grouped:
            grouped[group_key] = []
        grouped[group_key].append(song)
    return grouped

######################################
# Neue Funktion: Suchergebnisse anzeigen als Karteikarten
######################################
def display_search_results(results):
    st.title("Search Results")
    grouped = group_results_by_artist(results)
    for group_key, songs in grouped.items():
        # Nutze das erste Lied als Repräsentant für den Künstler
        representative = songs[0]
        artist_name = representative.get("artist_name")
        artist_image = representative.get("latest_measurement", {}).get("artist_image", "")
        # Baue die Artist-Karte
        artist_card = f"""
        <div style="
            border: 2px solid #1DB954;
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
            background-color: #f9f9f9;
            ">
            <div style="display: flex; align-items: center;">
                <img src="{artist_image}" alt="Artist" style="width: 80px; height: 80px; border-radius: 50%; margin-right: 16px;">
                <h2 style="margin: 0;">{artist_name}</h2>
            </div>
        </div>
        """
        st.markdown(artist_card, unsafe_allow_html=True)
        # Liste die Songs des Künstlers als Karten auf
        st.markdown("<div style='display: flex; flex-wrap: wrap;'>", unsafe_allow_html=True)
        for song in songs:
            # Hole Cover und Spotify-Link
            cover_url = ""
            track_url = ""
            try:
                url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
                headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                if data.get("album") and data["album"].get("images"):
                    cover_url = data["album"]["images"][0].get("url", "")
                track_url = data.get("external_urls", {}).get("spotify", "")
            except Exception as e:
                log(f"Fehler beim Abrufen des Covers für {song.get('track_name')}: {e}")
            card_html = f"""
            <div style="
                border: 1px solid #ccc;
                border-radius: 8px;
                padding: 16px;
                margin: 8px;
                width: 300px;
                box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
                background-color: #ffffff;
                ">
                <div style="text-align: center;">
                    <img src="{cover_url}" alt="Cover" style="width: 100%; border-radius: 4px;">
                </div>
                <h3 style="margin: 8px 0 4px 0;">{song.get("track_name")}</h3>
                <p style="margin: 4px 0;"><strong>Release Date:</strong> {song.get("release_date")}</p>
                <hr style="border: none; border-top: 1px solid #eee;">
                <p style="margin: 4px 0;"><strong>Song Pop:</strong> {song.get("latest_measurement", {}).get("song_pop", 0)}</p>
                <p style="margin: 4px 0;"><strong>Streams:</strong> {song.get("latest_measurement", {}).get("streams", 0)}</p>
                <p style="margin: 4px 0;"><strong>Monthly Listeners:</strong> {song.get("latest_measurement", {}).get("monthly_listeners", 0)}</p>
                <p style="margin: 4px 0;"><strong>Artist Followers:</strong> {song.get("latest_measurement", {}).get("artist_followers", 0)}</p>
                <div style="text-align: center; margin-top: 8px;">
                    <a href="{track_url}" target="_blank" style="
                        text-decoration: none;
                        color: #1DB954;
                        font-weight: bold;">Listen on Spotify</a>
                </div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

######################################
# Sidebar Buttons
######################################
st.sidebar.title("Actions")
if st.sidebar.button("Get New Music"):
    def run_get_new_music():
        spotify_token = get_spotify_token()
        log(f"Spotify Access Token: {spotify_token}")
        all_songs = []
        for pid in st.secrets["spotify"]["playlist_ids"]:
            songs = get_playlist_songs(pid, spotify_token)
            all_songs.extend(songs)
        log(f"Gesammelte Songs: {len(all_songs)}")
        for song in all_songs:
            if song["track_id"]:
                if song_exists_in_notion(song["track_id"]):
                    log(f"{song['song_name']} von {song['artist_name']} existiert bereits.")
                else:
                    log(f"{song['song_name']} von {song['artist_name']} wird erstellt.")
                    create_notion_page(song)
            else:
                log(f"{song['song_name']} hat keine Track ID und wird übersprungen.")
    run_get_new_music()
    log("Get New Music abgeschlossen. Bitte Seite neu laden, um die aktualisierten Daten zu sehen.")

if st.sidebar.button("Get Data"):
    msgs = fill_song_measurements()
    for m in msgs:
        log(m)

######################################
# Hilfsfunktion: song_exists_in_notion
######################################
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

######################################
# Hauptbereich: Suchergebnisse anzeigen (nur Ergebnisse)
######################################
def search_songs(query):
    query_lower = query.lower()
    results = {}
    for key, song in songs_metadata.items():
        if query_lower in song.get("track_name", "").lower() or query_lower in song.get("artist_name", "").lower():
            details = update_song_data(song, SPOTIFY_TOKEN)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            song["latest_measurement"] = details
            results[key] = song
    return results

if search_query:
    results_found = search_songs(search_query)
    display_search_results(results_found)
else:
    st.title("Search Results")
    st.write("Bitte einen Suchbegriff in der Sidebar eingeben.")
