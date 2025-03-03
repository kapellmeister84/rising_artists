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
set_dark_mode()   # Setzt den dunklen Modus via utils.py
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

# CSS-Anpassungen: Suchfeld und Buttons in der Sidebar schwarz; Links in Weiß ohne Unterstreichung; Artist-Karte mit weißem Rahmen
st.markdown(
    """
    <style>
    .stTextInput>div>div>input {
        background-color: #ffffff;
        color: #000000;
    }
    .stButton button {
        color: #000000 !important;
    }
    a {
        color: #ffffff;
        text-decoration: none;
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
            last_edited = page.get("last_edited_time", "")
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
                "measurements_ids": measurements_ids,
                "last_edited": last_edited
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
# Neue Hype Score Berechnung (unter Einbeziehung der Playlist-Follower)
######################################
# Wir definieren:
# Playlist Points (Song) = (Anzahl der Playlisten) + (Summe der Follower der Playlisten / SCALE)
# Playlist Points (Artist) = Durchschnitt über alle Songs des Künstlers
SCALE = 1000000  # Skalierungsfaktor für die Followerzahlen

def compute_song_hype(song):
    latest = song.get("latest_measurement", {})
    streams = latest.get("streams", 0)
    song_pop = latest.get("song_pop", 0)
    # Falls Playlist-Daten vorhanden: Anzahl + (Summe der Follower / SCALE)
    if song.get("playlists"):
        playlist_points = len(song["playlists"]) + (sum(int(pl.get("followers", 0)) for pl in song["playlists"]) / SCALE)
    else:
        playlist_points = 0
    base = (streams * 14.8) + (song_pop * 8.75) + (playlist_points * 0.92)
    measurements = song.get("measurements", [])
    if len(measurements) >= 2:
        sorted_ms = sorted(measurements, key=lambda m: m.get("timestamp"))
        previous = sorted_ms[-2]
        prev_base = (previous.get("streams", 0) * 14.8) + (previous.get("song_pop", 0) * 8.75)
        growth = base - prev_base
        raw = base + growth
        K = 100
    else:
        raw = base
        K = 1000  # Höherer K-Wert reduziert den initialen Score
    hype = 100 * raw / (raw + K) if raw >= 0 else 0
    return max(0, min(hype, 100))

def compute_artist_hype(song):
    latest = song.get("latest_measurement", {})
    streams = latest.get("streams", 0)
    artist_pop = latest.get("artist_pop", 0)
    artist_songs = [s for s in songs_metadata.values() if s.get("artist_id") == song.get("artist_id")]
    if artist_songs:
        avg_playlist_points = sum(len(s.get("playlists", [])) + (sum(int(pl.get("followers", 0)) for pl in s.get("playlists", [])) / SCALE)
                                 for s in artist_songs) / len(artist_songs)
    else:
        avg_playlist_points = 0
    base = (streams * 14.8) + (artist_pop * 8.75) + (avg_playlist_points * 0.92)
    measurements = song.get("measurements", [])
    if len(measurements) >= 2:
        sorted_ms = sorted(measurements, key=lambda m: m.get("timestamp"))
        previous = sorted_ms[-2]
        prev_base = (previous.get("streams", 0) * 14.8) + (previous.get("artist_pop", 0) * 8.75)
        growth = base - prev_base
        raw = base + growth
        K = 100
    else:
        raw = base
        K = 1000
    hype = 100 * raw / (raw + K) if raw >= 0 else 0
    return max(0, min(hype, 100))

def update_hype_score_in_measurement(measurement_id, hype_score, retries=5):
    url = f"{notion_page_endpoint}/{measurement_id}"
    payload = {
         "properties": {
              "Hype Score": {"number": hype_score}
         }
    }
    backoff = 1
    for attempt in range(retries):
        try:
            response = requests.patch(url, headers=notion_headers, json=payload)
            response.raise_for_status()
            return True
        except requests.HTTPError as e:
            if response.status_code == 409:
                st.warning(f"Conflict beim Update {measurement_id}, Versuch {attempt+1}/{retries}. Warte {backoff} Sekunde(n).")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                raise e
    st.error(f"Update des Hype Scores für {measurement_id} nach {retries} Versuchen fehlgeschlagen. Song wird übersprungen.")
    return False

######################################
# Graphen generieren
######################################
def get_artist_history_figure(measurements, height=80):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" in df.columns and not df["timestamp"].isnull().all():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        fig = px.line(df, x="timestamp", y=["artist_pop", "monthly_listeners", "artist_followers"],
                      labels={"timestamp": "Date", "value": "Value", "variable": "Metric"},
                      title="")
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig
    return None

def get_song_history_figure(measurements, height=150):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" in df.columns and not df["timestamp"].isnull().all():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        fig = px.line(df, x="timestamp", y=["song_pop", "streams"],
                      labels={"timestamp": "Date", "value": "Value", "variable": "Metric"},
                      title="")
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig
    return None

######################################
# Log- und Fortschrittsanzeige im Hauptbereich
######################################
log_container = st.empty()
progress_container = st.empty()

def log(msg):
    if "log_messages" not in st.session_state:
        st.session_state.log_messages = []
    st.session_state.log_messages.append(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")
    log_content = "\n".join(st.session_state.log_messages)
    log_container.markdown(
        f"""
        <div style="height:200px; overflow-y: scroll; border:1px solid #ccc; padding: 5px; background-color:#f9f9f9;">
            <pre style="white-space: pre-wrap; margin:0;">{log_content}</pre>
        </div>
        """,
        unsafe_allow_html=True
    )

def show_progress(current, total, info=""):
    progress = current / total
    progress_container.progress(progress)
    progress_container.write(info)

######################################
# Sidebar: Suchfeld, Filter, Buttons (Schrift in Sidebar = schwarz)
######################################
st.sidebar.title("Search")
search_query = st.sidebar.text_input("Search by artist or song:")
start_search = st.sidebar.button("Start Search", key="start_search_button")

st.sidebar.markdown("## Filters")
pop_range = st.sidebar.slider("Popularity Range", 0, 100, (0, 100))
stream_range = st.sidebar.slider("Stream Count Range", 0, 20000000, (0, 20000000), step=100000)
hype_range = st.sidebar.slider("Hype Score Range", 0, 100, (0, 100))
sort_option = st.sidebar.selectbox("Sort by", ["Hype Score", "Popularity", "Streams", "Release Date"])
confirm_filters = st.sidebar.button("Confirm Filters", key="confirm_filters_button")

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
        preferred_markets = {"DE", "AT", "CH"}
        available_markets = data.get("album", {}).get("available_markets", [])
        country_code = ""
        for market in available_markets:
            if market in preferred_markets:
                country_code = market
                break
        if not country_code and available_markets:
            country_code = available_markets[0]
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
            "Artist Followers": {"number": int(details.get("artist_followers") or 0)},
            "Artist Hype Score": {"number": float(compute_artist_hype({"latest_measurement": details}))}
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

######################################
# Fill Song Measurements: Nur Songs updaten, die nicht in den letzten 2 Stunden editiert wurden
######################################
def fill_song_measurements():
    spotify_token = get_spotify_token()
    messages = []
    total = len(songs_metadata)
    progress_container.empty()
    i = 0
    now = datetime.datetime.now(datetime.timezone.utc)
    for key, song in songs_metadata.items():
        update_needed = True
        if song.get("last_edited"):
            try:
                last_edit = datetime.datetime.fromisoformat(song["last_edited"].replace("Z", "+00:00"))
                if (now - last_edit).total_seconds() < 2 * 3600:
                    update_needed = False
                    log(f"Überspringe '{song.get('track_name')}' – zuletzt editiert vor weniger als 2 Stunden.")
            except Exception as e:
                log(f"Fehler bei der Zeitkonvertierung für '{song.get('track_name')}': {e}")
        if update_needed and song.get("track_id"):
            details = update_song_data(song, spotify_token)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            # Berechne den Song Hype Score – falls Vergleichsdaten vorliegen, wird der Zuwachs berücksichtigt,
            # sonst wird nur der Basiswert genutzt.
            hype = compute_song_hype({"latest_measurement": details, "playlists": song.get("playlists", [])})
            if not update_hype_score_in_measurement(new_meas_id, hype):
                log(f"Song '{song.get('track_name')}' wird übersprungen aufgrund von Hype-Score-Update-Fehler.")
                i += 1
                continue
            song["latest_measurement"] = details
            msg = f"'{song.get('track_name')}' aktualisiert (Hype: {hype:.1f})"
            messages.append(msg)
            log(msg)
        i += 1
        show_progress(i, total, f"Aktualisiere {i}/{total} Songs")
    progress_container.empty()
    return messages

######################################
# Hilfsfunktion: song_exists_in_notio (sollte song_exists_in_notion heißen)
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
# Graphen generieren
######################################
def get_artist_history_figure(measurements, height=80):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" in df.columns and not df["timestamp"].isnull().all():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        fig = px.line(df, x="timestamp", y=["artist_pop", "monthly_listeners", "artist_followers"],
                      labels={"timestamp": "Date", "value": "Value", "variable": "Metric"},
                      title="")
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig
    return None

def get_song_history_figure(measurements, height=150):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" in df.columns and not df["timestamp"].isnull().all():
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        fig = px.line(df, x="timestamp", y=["song_pop", "streams"],
                      labels={"timestamp": "Date", "value": "Value", "variable": "Metric"},
                      title="")
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig
    return None

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
        cols_artist = st.columns([1, 2, 2])
        with cols_artist[0]:
            st.markdown(f'<a href="{artist_link}" target="_blank"><img src="{artist_image}" alt="Artist" style="width:80px; height:80px; border-radius:50%;"></a>', unsafe_allow_html=True)
        with cols_artist[1]:
            st.markdown(f"<h2><a href='{artist_link}' target='_blank'>{artist_name}</a></h2>", unsafe_allow_html=True)
            st.markdown(f"<p>Popularity: {artist_pop}</p>", unsafe_allow_html=True)
            st.markdown(f"<p>Monthly Listeners: {monthly_listeners}</p>", unsafe_allow_html=True)
            st.markdown(f"<p>Followers: {artist_followers}</p>", unsafe_allow_html=True)
            st.markdown(f"<p>Artist Hype Score: {hype_artist:.1f}</p>", unsafe_allow_html=True)
        with cols_artist[2]:
            artist_fig = get_artist_history_figure(representative.get("measurements", []), height=80)
            if artist_fig:
                st.plotly_chart(artist_fig, use_container_width=True)
            else:
                st.write("No Data")
        st.markdown("---")
        st.markdown("<div style='display: flex; flex-direction: column;'>", unsafe_allow_html=True)
        for song in songs:
            cols_song = st.columns([1, 2, 2])
            try:
                url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
                headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                cover_url = ""
                if data.get("album") and data["album"].get("images"):
                    cover_url = data["album"]["images"][0].get("url", "")
                track_url = data.get("external_urls", {}).get("spotify", "")
            except Exception as e:
                log(f"Fehler beim Abrufen des Covers für {song.get('track_name')}: {e}")
                cover_url = ""
                track_url = ""
            hype_song = compute_song_hype(song)
            song_title_link = f"<h2><a href='{track_url}' target='_blank'>{song.get('track_name')}</a></h2>"
            with cols_song[0]:
                st.markdown(f'<a href="{track_url}" target="_blank"><img src="{cover_url}" alt="Cover" style="width:100%; border-radius:4px;"></a>', unsafe_allow_html=True)
            with cols_song[1]:
                st.markdown(song_title_link, unsafe_allow_html=True)
                st.markdown(f"<p><strong>Release Date:</strong> {song.get('release_date')}</p>", unsafe_allow_html=True)
                st.markdown(f"<p><strong>Song Pop:</strong> {song.get('latest_measurement', {}).get('song_pop', 0)}</p>", unsafe_allow_html=True)
                st.markdown(f"<p><strong>Streams:</strong> {song.get('latest_measurement', {}).get('streams', 0)}</p>", unsafe_allow_html=True)
                st.markdown(f"<p><strong>Hype Score:</strong> {hype_song:.1f}</p>", unsafe_allow_html=True)
            with cols_song[2]:
                song_fig = get_song_history_figure(song.get("measurements", []), height=150)
                if song_fig:
                    st.plotly_chart(song_fig, use_container_width=True)
                else:
                    st.write("No Data")
            st.markdown("<hr>", unsafe_allow_html=True)
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
            songs = get_playlist_data(pid, spotify_token).get("tracks", {}).get("items", [])
            for item in songs:
                track = item.get("track")
                if track:
                    all_songs.append(track)
        log(f"Gesammelte Songs: {len(all_songs)}")
        for song in all_songs:
            if song.get("id"):
                if song_exists_in_notion(song["id"]):
                    log(f"{song.get('name')} existiert bereits.")
                else:
                    log(f"{song.get('name')} wird erstellt.")
                    # Hier sollte deine Funktion zum Erstellen in Notion aufgerufen werden
            else:
                log(f"{song.get('name')} hat keine Track ID und wird übersprungen.")
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
            hype = compute_song_hype({"latest_measurement": details, "playlists": song.get("playlists", [])})
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
        hype = compute_song_hype({"latest_measurement": lm, "playlists": song.get("playlists", [])})
        if pop < pop_range[0] or pop > pop_range[1]:
            continue
        if streams < stream_range[0] or streams > stream_range[1]:
            continue
        if hype < hype_range[0] or hype > hype_range[1]:
            continue
        filtered[key] = song
    sorted_results = list(filtered.values())
    if sort_option == "Hype Score":
        sorted_results.sort(key=lambda s: compute_song_hype({"latest_measurement": s.get("latest_measurement", {}), "playlists": s.get("playlists", [])}), reverse=True)
    elif sort_option == "Popularity":
        sorted_results.sort(key=lambda s: s.get("latest_measurement", {}).get("song_pop", 0), reverse=True)
    elif sort_option == "Streams":
        sorted_results.sort(key=lambda s: s.get("latest_measurement", {}).get("streams", 0), reverse=True)
    elif sort_option == "Release Date":
        sorted_results.sort(key=lambda s: s.get("release_date", ""), reverse=True)
    final = {s.get("track_id") or s.get("page_id"): s for s in sorted_results}
    return final

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

# Falls keine Logmeldungen mehr vorhanden sind, Log- und Fortschrittscontainer ausblenden
if not st.session_state.get("log_messages"):
    log_container.empty()
    progress_container.empty()
