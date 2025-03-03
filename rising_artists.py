import streamlit as st
import requests
import datetime
import json
import time
import re
from concurrent.futures import ThreadPoolExecutor
import plotly.express as px
import pandas as pd
import math

from utils import set_background, set_dark_mode

st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

# CSS: Suchfeld heller gestalten
st.markdown(
    """
    <style>
    .stTextInput>div>div>input {
        background-color: #ffffff;
        color: #000000;
    }
    </style>
    """,
    unsafe_allow_html=True
)

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
    # created_time von Notion als Timestamp nutzen
    timestamp = data.get("created_time", "")
    return {
        "timestamp": timestamp,
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
                details = {"timestamp": "", "song_pop": 0, "artist_pop": 0, "streams": 0, "monthly_listeners": 0, "artist_followers": 0}
            for key, song_data in metadata.items():
                if measurement_id in song_data.get("measurements_ids", []):
                    if "measurements" not in song_data:
                        song_data["measurements"] = []
                    song_data["measurements"].append({"id": measurement_id, **details})
    return metadata

songs_metadata = get_songs_metadata()

######################################
# Hype Score Berechnung (basierend auf der neuesten Messung)
######################################
def compute_song_hype(song):
    m = song.get("latest_measurement", {})
    P = m.get("song_pop", 0)
    streams = m.get("streams", 0)
    S_scaled = min(math.log10(streams + 1) / 4 * 100, 100)
    return 0.4 * P + 0.6 * S_scaled

def compute_artist_hype(song):
    m = song.get("latest_measurement", {})
    A = m.get("artist_pop", 0)
    followers = m.get("artist_followers", 0)
    monthly = m.get("monthly_listeners", 0)
    F_scaled = min(math.log10(followers + 1) / 6 * 100, 100)
    M_scaled = min(math.log10(monthly + 1) / 6 * 100, 100)
    return 0.4 * A + 0.3 * F_scaled + 0.3 * M_scaled

def update_hype_score_in_measurement(measurement_id, hype_score, retries=3):
    url = f"{notion_page_endpoint}/{measurement_id}"
    payload = {
         "properties": {
              "Hype Score": {"number": hype_score}
         }
    }
    for attempt in range(retries):
        try:
            response = requests.patch(url, headers=notion_headers, json=payload)
            response.raise_for_status()
            return
        except requests.HTTPError as e:
            if response.status_code == 409:
                time.sleep(1)  # Kurze Pause vor erneutem Versuch
                continue
            else:
                raise e
    st.warning(f"Konnte Hype Score für {measurement_id} nach {retries} Versuchen nicht aktualisieren.")

######################################
# Sidebar: Suchfeld, Filter, Log & Buttons
######################################
st.sidebar.title("Search")
search_query = st.sidebar.text_input("Search by artist or song:")

# Start Search Button direkt unter dem Suchfeld
start_search = st.sidebar.button("Start Search", key="start_search_button")

st.sidebar.markdown("## Filters")
pop_range = st.sidebar.slider("Popularity Range", 0, 100, (0, 100))
stream_range = st.sidebar.slider("Stream Count Range", 0, 20000000, (0, 20000000), step=100000)
hype_range = st.sidebar.slider("Hype Score Range", 0, 100, (0, 100))
sort_option = st.sidebar.selectbox("Sort by", ["Hype Score", "Popularity", "Streams", "Release Date"])

# Confirm Filters Button (um auch ohne Suchbegriff zu suchen)
confirm_filters = st.sidebar.button("Confirm Filters", key="confirm_filters_button")

st.sidebar.markdown("---")
if "log_messages" not in st.session_state:
    st.session_state.log_messages = []

def log(msg):
    st.session_state.log_messages.append(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")
    st.sidebar.text_area("Log", "\n".join(st.session_state.log_messages), height=200)

def show_progress(current, total, info=""):
    progress = current / total
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
            # "date created" wird automatisch von Notion gesetzt
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
    total = len(songs_metadata)
    progress_bar = st.sidebar.progress(0)
    i = 0
    for key, song in songs_metadata.items():
        if song.get("track_id"):
            details = update_song_data(song, spotify_token)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            hype = compute_song_hype({"latest_measurement": details})
            update_hype_score_in_measurement(new_meas_id, hype)
            song["latest_measurement"] = details
            msg = f"'{song.get('track_name')}' aktualisiert (Hype: {hype:.1f})"
            messages.append(msg)
            st.sidebar.write(msg)
        i += 1
        progress_bar.progress(i / total)
    return messages

def update_hype_score_in_measurement(measurement_id, hype_score):
    url = f"{notion_page_endpoint}/{measurement_id}"
    payload = {
         "properties": {
              "Hype Score": {"number": hype_score}
         }
    }
    response = requests.patch(url, headers=notion_headers, json=payload)
    response.raise_for_status()

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
# Gruppierung der Suchergebnisse nach Artist
######################################
def group_results_by_artist(results):
    grouped = {}
    for key, song in results.items():
        group_key = song.get("artist_id") or song.get("artist_name")
        if group_key not in grouped:
            grouped[group_key] = []
        grouped[group_key].append(song)
    return grouped

######################################
# Graphen anzeigen
######################################
def display_artist_history(measurements):
    if not measurements:
        st.write("Keine historischen Daten vorhanden.")
        return
    df = pd.DataFrame(measurements)
    if "timestamp" in df.columns and not df["timestamp"].isnull().all():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        fig = px.line(df, x="timestamp", y=["artist_pop", "monthly_listeners", "artist_followers"],
                      labels={"timestamp": "Date", "value": "Value", "variable": "Metric"},
                      title="Artist History")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Keine Datum-Informationen in den Messdaten vorhanden.")

def display_song_history(measurements):
    if not measurements:
        st.write("Keine historischen Daten vorhanden.")
        return
    df = pd.DataFrame(measurements)
    if "timestamp" in df.columns and not df["timestamp"].isnull().all():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        fig = px.line(df, x="timestamp", y=["song_pop", "streams"],
                      labels={"timestamp": "Date", "value": "Value", "variable": "Metric"},
                      title="Song History")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Keine Datum-Informationen in den Messdaten vorhanden.")

######################################
# Suchergebnisse anzeigen als Karteikarten (gruppiert nach Artist)
######################################
def display_search_results(results):
    st.title("Search Results")
    grouped = group_results_by_artist(results)
    for group_key, songs in grouped.items():
        representative = songs[0]
        artist_name = representative.get("artist_name")
        artist_id = representative.get("artist_id")
        artist_image = representative.get("latest_measurement", {}).get("artist_image", "")
        artist_link = f"https://open.spotify.com/artist/{artist_id}" if artist_id else ""
        hype_artist = compute_artist_hype({"latest_measurement": representative.get("latest_measurement", {})})
        artist_pop = representative.get("latest_measurement", {}).get("artist_pop", 0)
        monthly_listeners = representative.get("latest_measurement", {}).get("monthly_listeners", 0)
        artist_followers = representative.get("latest_measurement", {}).get("artist_followers", 0)
        artist_card = f"""
        <div style="
            border: 2px solid #1DB954;
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
            background-color: #444444;
            color: #ffffff;
            display: flex;
            justify-content: space-between;
            align-items: center;
            ">
            <div style="display: flex; align-items: center;">
                <a href="{artist_link}" target="_blank">
                  <img src="{artist_image}" alt="Artist" style="width: 80px; height: 80px; border-radius: 50%; margin-right: 16px;">
                </a>
                <div>
                  <h2 style="margin: 0;">{artist_name}</h2>
                  <p style="margin: 4px 0;">Popularity: {artist_pop}</p>
                  <p style="margin: 4px 0;">Monthly Listeners: {monthly_listeners}</p>
                  <p style="margin: 4px 0;">Followers: {artist_followers}</p>
                </div>
            </div>
            <div style="font-size: 2rem; font-weight: bold; color: #FFD700;">
                {hype_artist:.1f}
            </div>
        </div>
        """
        st.markdown(artist_card, unsafe_allow_html=True)
        with st.expander("Show Artist History"):
            all_artist_measurements = []
            for s in songs:
                if "measurements" in s:
                    all_artist_measurements.extend(s["measurements"])
            display_artist_history(all_artist_measurements)
        st.markdown("<div style='display: flex; flex-wrap: wrap;'>", unsafe_allow_html=True)
        for song in songs:
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
            hype_song = compute_song_hype(song)
            song_card = f"""
            <div style="
                border: 1px solid #ccc;
                border-radius: 8px;
                padding: 16px;
                margin: 8px;
                width: 300px;
                box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
                background-color: #444444;
                color: #ffffff;
                ">
                <div style="text-align: center;">
                    <a href="{track_url}" target="_blank">
                      <img src="{cover_url}" alt="Cover" style="width: 100%; border-radius: 4px;">
                    </a>
                </div>
                <h3 style="margin: 8px 0 4px 0;">{song.get("track_name")}</h3>
                <p style="margin: 4px 0;"><strong>Release Date:</strong> {song.get("release_date")}</p>
                <hr style="border: none; border-top: 1px solid #888;">
                <p style="margin: 4px 0;"><strong>Song Pop:</strong> {song.get("latest_measurement", {}).get("song_pop", 0)}</p>
                <p style="margin: 4px 0;"><strong>Streams:</strong> {song.get("latest_measurement", {}).get("streams", 0)}</p>
                <p style="margin: 4px 0;"><strong>Hype Score:</strong> {hype_song:.1f}</p>
            </div>
            """
            st.markdown(song_card, unsafe_allow_html=True)
            with st.expander("Show Song History"):
                display_song_history(song.get("measurements", []))
        st.markdown("</div>", unsafe_allow_html=True)

######################################
# Sidebar Buttons: Get New Music und Get Data
######################################
st.sidebar.title("Actions")
if st.sidebar.button("Get New Music", key="get_new_music_button"):
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

if st.sidebar.button("Get Data", key="get_data_button"):
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
# Hauptbereich: Suchergebnisse anzeigen (Filter & Sortierung)
######################################
def search_songs(query):
    query_lower = query.lower()
    results = {}
    for key, song in songs_metadata.items():
        if query_lower in song.get("track_name", "").lower() or query_lower in song.get("artist_name", "").lower():
            details = update_song_data(song, SPOTIFY_TOKEN)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            hype = compute_song_hype({"latest_measurement": details})
            update_hype_score_in_measurement(new_meas_id, hype)
            song["latest_measurement"] = details
            results[key] = song
    return results

def apply_filters_and_sort(results):
    filtered = {}
    for key, song in results.items():
        lm = song.get("latest_measurement", {})
        pop = lm.get("song_pop", 0)
        streams = lm.get("streams", 0)
        hype = compute_song_hype({"latest_measurement": lm})
        if pop < pop_range[0] or pop > pop_range[1]:
            continue
        if streams < stream_range[0] or streams > stream_range[1]:
            continue
        if hype < hype_range[0] or hype > hype_range[1]:
            continue
        filtered[key] = song
    sorted_results = list(filtered.values())
    if sort_option == "Hype Score":
        sorted_results.sort(key=lambda s: compute_song_hype({"latest_measurement": s.get("latest_measurement", {})}), reverse=True)
    elif sort_option == "Popularity":
        sorted_results.sort(key=lambda s: s.get("latest_measurement", {}).get("song_pop", 0), reverse=True)
    elif sort_option == "Streams":
        sorted_results.sort(key=lambda s: s.get("latest_measurement", {}).get("streams", 0), reverse=True)
    elif sort_option == "Release Date":
        sorted_results.sort(key=lambda s: s.get("release_date", ""), reverse=True)
    final = {s.get("track_id") or s.get("page_id"): s for s in sorted_results}
    return final

# Suchergebnisse auslösen: Entweder "Start Search" oder "Confirm Filters"
if start_search or confirm_filters:
    if search_query:
        results_found = search_songs(search_query)
    else:
        results_found = songs_metadata
    results_filtered_sorted = apply_filters_and_sort(results_found)
    display_search_results(results_filtered_sorted)
else:
    st.title("Search Results")
    st.write("Bitte einen Suchbegriff eingeben oder Filter bestätigen.")
