import streamlit as st
import requests
import datetime
import json
import time
import re
import math
import plotly.express as px
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from utils import set_background, set_dark_mode

# --- Page Configuration & Dark Mode ---
st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

# --- Globales CSS ---
st.markdown("""
<style>
/* Alle Links in Weiß ohne Unterstreichung */
a {
    color: #ffffff !important;
    text-decoration: none !important;
}
a:hover {
    color: #f0f0f0 !important;
}
/* Input & Button Style in der Sidebar: schwarze Schrift */
.stTextInput>div>div>input {
    background-color: #ffffff;
    color: #000000;
}
.stButton button {
    color: #000000 !important;
}
/* Container Styles für Artist- und Song-Karteikarten */
.artist-card {
    border: 2px solid #ffffff;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
    background-color: #444444;
    color: #ffffff;
}
.song-card {
    border: 1px solid #ccc;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
    background-color: #444444;
    color: #ffffff;
}
</style>
""", unsafe_allow_html=True)

#############################
# Notion-Konfiguration
#############################
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

#############################
# Logging & Fortschritt (Hauptbereich)
#############################
log_container = st.empty()
progress_container = st.empty()

def log(msg):
    if "log_messages" not in st.session_state:
        st.session_state.log_messages = []
    st.session_state.log_messages.append(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")
    log_content = "\n".join(st.session_state.log_messages)
    log_container.markdown(
        f"""
        <div style="height:200px; overflow-y: auto; border:1px solid #ccc; padding: 5px; background-color:#f9f9f9;">
            <pre style="white-space: pre-wrap; margin:0;">{log_content}</pre>
        </div>
        """,
        unsafe_allow_html=True
    )

def show_progress(current, total, info=""):
    progress = current / total
    progress_container.progress(progress)
    progress_container.write(info)

#############################
# Notion-Daten: Songs-Metadaten & Measurements (inkl. Favourite)
#############################
@st.cache_data(show_spinner=False)
def get_measurement_details(measurement_id):
    url = f"{notion_page_endpoint}/{measurement_id}"
    resp = requests.get(url, headers=notion_headers)
    resp.raise_for_status()
    data = resp.json()
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
        resp = requests.post(url, headers=notion_headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
    metadata = {}
    measurement_futures = {}
    with ThreadPoolExecutor() as executor:
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
            # Neues: Favourite-Status auslesen
            favourite = False
            if "Favourite" in props:
                favourite = props["Favourite"].get("checkbox", False)
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
                "last_edited": last_edited,
                "favourite": favourite,
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

#############################
# Safe Timestamp (alle Timestamps ohne Zeitzone)
#############################
def safe_timestamp(m):
    t = m.get("timestamp")
    if not t:
        return datetime.datetime.min
    try:
        dt = datetime.datetime.fromisoformat(t)
        return dt.replace(tzinfo=None)
    except:
        return datetime.datetime.min

#############################
# Hype Score Berechnung (ohne Playlisten)
#############################
def compute_song_hype(song):
    measurements = song.get("measurements", [])
    if len(measurements) < 2:
        return 0
    sorted_ms = sorted(measurements, key=lambda m: safe_timestamp(m))
    latest = sorted_ms[-1]
    previous = sorted_ms[-2]
    growth_streams = latest.get("streams", 0) - previous.get("streams", 0)
    growth_pop = latest.get("song_pop", 0) - previous.get("song_pop", 0)
    EPSILON = 5
    if abs(growth_streams) < EPSILON and abs(growth_pop) < EPSILON:
        return 0
    log_streams = math.log10(latest.get("streams", 0) + 1)
    base_current = (log_streams * 14.8) + (latest.get("song_pop", 0) * 8.75)
    growth_val = (growth_streams * 14.8) + (growth_pop * 8.75)
    raw = base_current + growth_val
    K = 100
    hype = 100 * raw / (raw + K) if raw >= 0 else 0
    return max(0, min(hype, 100))

def compute_artist_hype(song):
    measurements = song.get("measurements", [])
    if len(measurements) < 2:
        return 0
    sorted_ms = sorted(measurements, key=lambda m: safe_timestamp(m))
    latest = sorted_ms[-1]
    previous = sorted_ms[-2]
    growth_streams = latest.get("streams", 0) - previous.get("streams", 0)
    growth_pop = latest.get("artist_pop", 0) - previous.get("artist_pop", 0)
    EPSILON = 5
    if abs(growth_streams) < EPSILON and abs(growth_pop) < EPSILON:
        return 0
    log_streams = math.log10(latest.get("streams", 0) + 1)
    base_current = (log_streams * 14.8) + (latest.get("artist_pop", 0) * 8.75)
    growth_val = (growth_streams * 14.8) + (growth_pop * 8.75)
    raw = base_current + growth_val
    K = 100
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
            r = requests.patch(url, headers=notion_headers, json=payload)
            r.raise_for_status()
            return True
        except requests.HTTPError as e:
            if r.status_code == 409:
                st.warning(f"Conflict beim Update {measurement_id}, Versuch {attempt+1}/{retries}. Warte {backoff} Sekunde(n).")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                raise e
    st.error(f"Update des Hype Scores für {measurement_id} nach {retries} Versuchen fehlgeschlagen.")
    return False

#############################
# Spotify API Funktionen
#############################
def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    r = requests.get(url)
    r.raise_for_status()
    return r.json().get("accessToken")

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
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        return int(data["data"]["trackUnion"].get("playcount", 0))
    except requests.HTTPError as e:
        log(f"Error fetching playcount for track {track_id}: {e}")
        return 0

def get_spotify_popularity(track_id, token):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
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
            val = match.group(1).replace('.', '').replace(',', '')
            try:
                return int(val)
            except Exception as e:
                log(f"Fehler bei der Konvertierung der monatlichen Hörer für {artist_id}: {e}")
        else:
            log(f"Kein passender Wert auf der Seite von Artist {artist_id} gefunden.")
    else:
        log(f"Fehler beim Abrufen der Artist-Seite {artist_id}: Status {r.status_code}")
    return None

def update_song_data(song, token):
    if not song.get("track_id"):
        return {}
    url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        data = r.json()
        preferred_markets = {"DE", "AT", "CH"}
        available_markets = data.get("album", {}).get("available_markets", [])
        country_code = ""
        for m in available_markets:
            if m in preferred_markets:
                country_code = m
                break
        if not country_code and available_markets:
            country_code = available_markets[0]
        song_pop = get_spotify_popularity(song["track_id"], token)
        artists = data.get("artists", [])
        artist_id = artists[0].get("id") if (artists and artists[0].get("id")) else ""
        artist_pop = 0
        artist_followers = 0
        artist_image = ""
        if artist_id:
            artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
            ar = requests.get(artist_url, headers={"Authorization": f"Bearer {token}"})
            if ar.status_code == 200:
                adata = ar.json()
                artist_pop = adata.get("popularity", 0)
                artist_followers = adata.get("followers", {}).get("total", 0)
                if adata.get("images") and len(adata["images"]) > 0:
                    artist_image = adata["images"][0].get("url", "")
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
        st.error(f"Error fetching data for track {song['track_name']}: {r.text}")
        return {}

#############################
# Favourites-Funktionalität
#############################
def update_favourite_property(page_id, new_state):
    url = f"{notion_page_endpoint}/{page_id}"
    payload = {
        "properties": {
            "Favourite": {"checkbox": new_state}
        }
    }
    st.write(f"Updating page {page_id} with payload: {payload}")  # Debug-Ausgabe
    r = requests.patch(url, headers=notion_headers, json=payload)
    try:
        r.raise_for_status()
        st.write(f"Update successful for page {page_id}: {r.json()}")
    except Exception as e:
        st.error(f"Update failed for page {page_id}: {r.text}")
        raise e

def is_song_favourite(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    r = requests.get(url, headers=notion_headers)
    r.raise_for_status()
    return r.json().get("properties", {}).get("Favourite", {}).get("checkbox", False)

def is_artist_favourite(artist_id):
    for song in songs_metadata.values():
        if song.get("artist_id") == artist_id and is_song_favourite(song["page_id"]):
            return True
    return False

def toggle_favourite_for_artist(artist_id, new_state=True):
    for song in songs_metadata.values():
        if song.get("artist_id") == artist_id:
            update_favourite_property(song["page_id"], new_state)
            

#############################
# Measurement-Einträge & Hype Score Update
#############################
def create_measurement_entry(song, details):
    now = datetime.datetime.now().isoformat()
    payload = {
        "parent": {"database_id": measurements_db_id},
        "properties": {
            "Name": {"title": [{"text": {"content": f"Measurement {now}"}}]},
            "Song": {"relation": [{"id": song["page_id"]}]},
            "Song Pop": {"number": details.get("song_pop", 0)},
            "Artist Pop": {"number": details.get("artist_pop", 0)},
            "Streams": {"number": details.get("streams", 0)},
            "Monthly Listeners": {"number": details.get("monthly_listeners", 0)},
            "Artist Followers": {"number": details.get("artist_followers", 0)},
            "Artist Hype Score": {"number": float(compute_artist_hype(song))}
        }
    }
    r = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    r.raise_for_status()
    return r.json().get("id")

def update_song_measurements_relation(page_id, new_measurement_id, retries=3):
    for attempt in range(retries):
        url = f"{notion_page_endpoint}/{page_id}"
        r = requests.get(url, headers=notion_headers)
        r.raise_for_status()
        page_data = r.json()
        props = page_data.get("properties", {})
        current_rels = []
        if "Measurements" in props and props["Measurements"].get("relation"):
            current_rels = props["Measurements"]["relation"]
        if not any(rel.get("id") == new_measurement_id for rel in current_rels):
            current_rels.append({"id": new_measurement_id})
        payload = {
            "properties": {
                "Measurements": {"relation": current_rels}
            }
        }
        patch_resp = requests.patch(url, headers=notion_headers, json=payload)
        if patch_resp.status_code == 200:
            return
        elif patch_resp.status_code == 409:
            st.warning(f"Conflict beim Update von Seite {page_id}, Versuch {attempt+1}/3. Warte 1 Sekunde.")
            time.sleep(1)
            continue
        else:
            patch_resp.raise_for_status()
    st.warning(f"Konnte Measurements für Seite {page_id} nach {retries} Versuchen nicht aktualisieren.")

#############################
# Fill Song Measurements ("Get Data")
#############################
def fill_song_measurements():
    token = get_spotify_token()
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
                    log(f"Überspringe '{song.get('track_name')}' – Zuletzt editiert vor < 2h.")
                    update_needed = False
            except Exception as e:
                log(f"Zeitkonvertierungsfehler bei '{song.get('track_name')}': {e}")
        if update_needed and song.get("track_id"):
            details = update_song_data(song, token)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            # Hype Score Berechnung: nur, wenn Vergleichsdaten vorhanden und signifikant, sonst 0
            measurements = song.get("measurements", [])
            EPSILON = 5
            if len(measurements) >= 2:
                sorted_ms = sorted(measurements, key=lambda m: safe_timestamp(m))
                previous = sorted_ms[-2]
                prev_streams = previous.get("streams", 0)
                prev_pop = previous.get("song_pop", 0)
                prev_base = (prev_streams * 14.8) + (prev_pop * 8.75)
                current_streams = details.get("streams", 0)
                current_pop = details.get("song_pop", 0)
                current_base = (current_streams * 14.8) + (current_pop * 8.75)
                growth = current_base - prev_base
                if abs(growth) < EPSILON:
                    raw = 0
                else:
                    raw = current_base + growth
                K = 100
            else:
                raw = 0
                K = 1000
            hype = 100 * raw / (raw + K) if raw >= 0 else 0
            if not update_hype_score_in_measurement(new_meas_id, hype):
                log(f"Hype Score Update fehlgeschlagen für {song.get('track_name')}.")
                i += 1
                continue
            song["latest_measurement"] = details
            msg = f"'{song.get('track_name')}' aktualisiert. Hype Score: {hype:.1f}"
            messages.append(msg)
            log(msg)
        i += 1
        show_progress(i, total, f"Aktualisiere {i}/{total} Songs")
    progress_container.empty()
    return messages

#############################
# Song Exists
#############################
def song_exists_in_notion(track_id):
    payload = {
        "filter": {
            "property": "Track ID",
            "rich_text": {"equals": track_id}
        }
    }
    r = requests.post(f"{notion_query_endpoint}/{songs_database_id}/query", headers=notion_headers, json=payload)
    if r.status_code == 200:
        return len(r.json().get("results", [])) > 0
    else:
        st.error("Notion-Query Fehler: " + r.text)
        return False

#############################
# Graph-Funktionen
#############################
def get_artist_pop_monthly_figure(measurements, height=200):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" not in df.columns or df["timestamp"].isnull().all():
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    fig = px.line(df, x="timestamp", y=["artist_pop", "monthly_listeners"],
                  labels={"timestamp": "Date", "value": "Value", "variable": "Metric"}, title="")
    fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
    return fig

def get_artist_followers_figure(measurements, height=200):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" not in df.columns or df["timestamp"].isnull().all():
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    fig = px.line(df, x="timestamp", y=["artist_followers"],
                  labels={"timestamp": "Date", "value": "Followers", "variable": "Metric"}, title="")
    fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
    return fig

def get_song_pop_figure(measurements, height=200):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" not in df.columns or df["timestamp"].isnull().all():
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    fig = px.line(df, x="timestamp", y=["song_pop"],
                  labels={"timestamp": "Date", "value": "Popularity", "variable": "Metric"}, title="")
    fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
    return fig

def get_song_streams_figure(measurements, height=200):
    if not measurements:
        return None
    df = pd.DataFrame(measurements)
    if "timestamp" not in df.columns or df["timestamp"].isnull().all():
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    fig = px.line(df, x="timestamp", y=["streams"],
                  labels={"timestamp": "Date", "value": "Streams", "variable": "Metric"}, title="")
    fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
    return fig

#############################
# Gruppierung der Suchergebnisse nach Artist
#############################
def group_results_by_artist(results):
    grouped = {}
    for key, song in results.items():
        group_key = song.get("artist_id") or song.get("artist_name", "Unknown Artist")
        if group_key not in grouped:
            grouped[group_key] = []
        grouped[group_key].append(song)
    return grouped

#############################
# Favourites-Funktionalität
#############################
def is_song_favourite(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    r = requests.get(url, headers=notion_headers)
    r.raise_for_status()
    return r.json().get("properties", {}).get("Favourite", {}).get("checkbox", False)

def is_artist_favourite(artist_id):
    for song in songs_metadata.values():
        if song.get("artist_id") == artist_id and is_song_favourite(song["page_id"]):
            return True
    return False

def update_favourite_property(page_id, new_state):
    url = f"{notion_page_endpoint}/{page_id}"
    payload = {
        "properties": {
            "Favourite": {"checkbox": new_state}
        }
    }
    r = requests.patch(url, headers=notion_headers, json=payload)
    r.raise_for_status()

def toggle_favourite_for_artist(artist_id, new_state=True):
    for song in songs_metadata.values():
        if song.get("artist_id") == artist_id:
            update_favourite_property(song["page_id"], new_state)
st.title("ARTIST SCOUT 1.0")
#############################
# Anzeige der Suchergebnisse (Artist- & Song-Karten)
#############################
def display_search_results(results):
    st.title("Search Results")
    grouped = group_results_by_artist(results)
    for group_key, songs in grouped.items():
        rep = songs[0]
        artist_name = rep.get("artist_name", "Unknown Artist")
        artist_id = rep.get("artist_id", "")
        artist_link = f"https://open.spotify.com/artist/{artist_id}" if artist_id else ""
        hype_artist = compute_artist_hype(rep)
        artist_pop = rep.get("latest_measurement", {}).get("artist_pop", 0)
        monthly_listeners = rep.get("latest_measurement", {}).get("monthly_listeners", 0)
        artist_followers = rep.get("latest_measurement", {}).get("artist_followers", 0)
        artist_img = rep.get("latest_measurement", {}).get("artist_image", "")
        fav_state = rep.get("favourite", False)
        star_icon = "★" if fav_state else "☆"
        
        # Artist-Karte
        with st.container():
            cols_artist = st.columns([1, 3, 2, 1])
            with cols_artist[0]:
                st.markdown(f'<a href="{artist_link}" target="_blank"><img src="{artist_img}" alt="Artist" style="width:120px; height:120px; border-radius:50%; object-fit:cover;"></a>', unsafe_allow_html=True)
            with cols_artist[1]:
                st.markdown(f"<h1><a href='{artist_link}' target='_blank' style='color:#ffffff;'>{artist_name}</a></h1>", unsafe_allow_html=True)
                st.markdown(f"<p>Popularity: {artist_pop}</p>", unsafe_allow_html=True)
                st.markdown(f"<p>Monthly Listeners: {monthly_listeners}</p>", unsafe_allow_html=True)
                st.markdown(f"<p>Followers: {artist_followers}</p>", unsafe_allow_html=True)
                st.markdown(f"<p style='font-size:1.3rem;'><strong>Artist Hype Score: <span style='font-size:1.6rem; color:#FFD700;'>{hype_artist:.1f}</span></strong></p>", unsafe_allow_html=True)
            with cols_artist[2]:
                with st.expander("Show Artist Charts", expanded=False):
                    fig_pop = get_artist_pop_monthly_figure(rep.get("measurements", []))
                    if fig_pop:
                        st.plotly_chart(fig_pop, use_container_width=True)
                    fig_follow = get_artist_followers_figure(rep.get("measurements", []))
                    if fig_follow:
                        st.plotly_chart(fig_follow, use_container_width=True)
            with cols_artist[3]:
                # Verwende einen Button mit unique key, ohne st.experimental_rerun() – 
                # stattdessen den lokalen Status in st.session_state updaten
                if st.button(f"{star_icon}", key=f"fav_{artist_id}"):
                    toggle_favourite_for_artist(artist_id, not fav_state)
                    st.session_state.fav_updated = True
            st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("---")
        
        # Song-Karten des Artists
        for song in songs:
            with st.container():
                cols_song = st.columns([1, 3, 2])
                try:
                    track_url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
                    r = requests.get(track_url, headers={"Authorization": f"Bearer {SPOTIFY_TOKEN}"})
                    r.raise_for_status()
                    data = r.json()
                    cover_url = ""
                    if data.get("album") and data["album"].get("images"):
                        cover_url = data["album"]["images"][0].get("url", "")
                    song_link = data.get("external_urls", {}).get("spotify", "")
                except Exception as e:
                    log(f"Fehler beim Abrufen des Covers für {song.get('track_name')}: {e}")
                    cover_url = ""
                    song_link = ""
                hype_song = compute_song_hype(song)
                song_title = song.get("track_name", "Unknown Song")
                with cols_song[0]:
                    st.markdown(f'<a href="{song_link}" target="_blank"><img src="{cover_url}" alt="Cover" style="width:200px; border-radius:8px; object-fit:cover;"></a>', unsafe_allow_html=True)
                with cols_song[1]:
                    st.markdown(f"<h2 style='margin:0 0 10px 0;'><a href='{song_link}' target='_blank' style='color:#ffffff;'>{song_title}</a></h2>", unsafe_allow_html=True)
                    st.markdown(f"<p><strong>Release Date:</strong> {song.get('release_date')}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p><strong>Song Pop:</strong> {song.get('latest_measurement', {}).get('song_pop', 0)}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p><strong>Streams:</strong> {song.get('latest_measurement', {}).get('streams', 0)}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size:1.2rem;'><strong>Hype Score: <span style='font-size:1.6rem; color:#FFD700;'>{hype_song:.1f}</span></strong></p>", unsafe_allow_html=True)
                with cols_song[2]:
                    with st.expander("Show Song Streams", expanded=False):
                        fig_streams = get_song_streams_figure(song.get("measurements", []))
                        if fig_streams:
                            st.plotly_chart(fig_streams, use_container_width=True)
                        else:
                            st.write("No Streams Data")
                    with st.expander("Show Song Popularity", expanded=False):
                        fig_pop = get_song_pop_figure(song.get("measurements", []))
                        if fig_pop:
                            st.plotly_chart(fig_pop, use_container_width=True)
                        else:
                            st.write("No Popularity Data")
            st.markdown("<hr>", unsafe_allow_html=True)

#############################
# Sidebar: Suchfeld, Filter, Buttons
#############################
st.sidebar.title("Search")
search_query = st.sidebar.text_input("Search by artist or song:", "")
start_search = st.sidebar.button("Start Search", key="start_search_button")

st.sidebar.markdown("## Filters")
pop_range = st.sidebar.slider("Popularity Range", 0, 100, (0, 100))
stream_range = st.sidebar.slider("Stream Count Range", 0, 20000000, (0, 20000000), step=100000)
hype_range = st.sidebar.slider("Hype Score Range", 0, 100, (0, 100))
sort_option = st.sidebar.selectbox("Sort by", ["Hype Score", "Popularity", "Streams", "Release Date"])
confirm_filters = st.sidebar.button("Confirm Filters", key="confirm_filters_button")

st.sidebar.title("Actions")
if st.sidebar.button("Get New Music", key="get_new_music_button"):
    def run_get_new_music():
        token = get_spotify_token()
        log(f"Spotify Access Token: {token}")
        all_songs = []
        for pid in st.secrets["spotify"]["playlist_ids"]:
            r = requests.get(f"https://api.spotify.com/v1/playlists/{pid}", headers={"Authorization": f"Bearer {token}"})
            data = r.json()
            items = data.get("tracks", {}).get("items", [])
            for item in items:
                track = item.get("track")
                if track:
                    all_songs.append(track)
        log(f"Gesammelte Songs: {len(all_songs)}")
        for s in all_songs:
            if s.get("id"):
                if song_exists_in_notion(s["id"]):
                    log(f"{s.get('name')} existiert bereits.")
                else:
                    log(f"{s.get('name')} wird erstellt.")
                    # Hier Funktion zum Erstellen in Notion aufrufen
            else:
                log(f"{s.get('name')} hat keine Track ID und wird übersprungen.")
    run_get_new_music()
    log("Get New Music abgeschlossen. Bitte Seite neu laden.")
    
if st.sidebar.button("Get Data", key="get_data_button"):
    msgs = fill_song_measurements()
    for msg in msgs:
        log(msg)

#############################
# Suchergebnisse: Filtern & Sortieren
#############################
def search_songs(query):
    query_lower = query.lower()
    results = {}
    for key, song in songs_metadata.items():
        if query_lower in song.get("track_name", "").lower() or query_lower in song.get("artist_name", "").lower():
            details = update_song_data(song, SPOTIFY_TOKEN)
            new_meas_id = create_measurement_entry(song, details)
            update_song_measurements_relation(song["page_id"], new_meas_id)
            hype = compute_song_hype({**song, "measurements": song.get("measurements", []) + [details]})
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
        hype = compute_song_hype(song)
        if pop < pop_range[0] or pop > pop_range[1]:
            continue
        if streams < stream_range[0] or streams > stream_range[1]:
            continue
        if hype < hype_range[0] or hype > hype_range[1]:
            continue
        filtered[key] = song
    sorted_list = list(filtered.values())
    if sort_option == "Hype Score":
        sorted_list.sort(key=lambda s: compute_song_hype(s), reverse=True)
    elif sort_option == "Popularity":
        sorted_list.sort(key=lambda s: s.get("latest_measurement", {}).get("song_pop", 0), reverse=True)
    elif sort_option == "Streams":
        sorted_list.sort(key=lambda s: s.get("latest_measurement", {}).get("streams", 0), reverse=True)
    elif sort_option == "Release Date":
        sorted_list.sort(key=lambda s: s.get("release_date", ""), reverse=True)
    final = {s.get("track_id") or s.get("page_id"): s for s in sorted_list}
    return final

if start_search or confirm_filters:
    if search_query:
        found = search_songs(search_query)
    else:
        found = songs_metadata
    final_results = apply_filters_and_sort(found)
    display_search_results(final_results)
else:
    st.title("Search Results")
    st.write("Bitte einen Suchbegriff eingeben oder Filter bestätigen.")

# Log- und Fortschrittscontainer ausblenden, falls keine Logmeldungen mehr vorhanden
if not st.session_state.get("log_messages"):
    log_container.empty()
    progress_container.empty()

#############################
# Top 10 Artists To Watch
#############################
def get_top_10_artists_to_watch(songs_metadata):
    artist_map = {}
    for key, song in songs_metadata.items():
        aid = song.get("artist_id") or song.get("artist_name")
        if aid not in artist_map:
            artist_map[aid] = song
    def get_artist_score(s):
        meas = s.get("measurements", [])
        if len(meas) < 2:
            return 0
        sorted_ms = sorted(meas, key=lambda m: safe_timestamp(m))
        return compute_artist_hype({"measurements": sorted_ms})
    all_artists = list(artist_map.values())
    all_artists.sort(key=lambda s: get_artist_score(s), reverse=True)
    top_artists = [a for a in all_artists if get_artist_score(a) > 0]
    return top_artists[:10]

st.header("Top 10 Artists To Watch")
top10 = get_top_10_artists_to_watch(songs_metadata)
if not top10:
    st.write("Aktuell keine Artists mit signifikantem Wachstum.")
else:
    for art in top10:
        st.markdown(f"• {art.get('artist_name')} – Hype Score: {compute_artist_hype(art):.1f}")

#############################
# Favourites-Sektion
#############################
def get_favourite_artists(songs_metadata):
    artist_map = {}
    for s in songs_metadata.values():
        aid = s.get("artist_id")
        if aid not in artist_map:
            artist_map[aid] = []
        if is_song_favourite(s["page_id"]):
            artist_map[aid].append(s)
    return {aid: songs for aid, songs in artist_map.items() if songs}

fav_artists = get_favourite_artists(songs_metadata)
if fav_artists:
    st.header("Favourites")
    # Vertreter der jeweiligen Artist-Gruppe auswählen
    fav_reps = [songs[0] for songs in fav_artists.values()]
    # Galerie in 5 Spalten
    cols = st.columns(5)
    for idx, artist in enumerate(fav_reps):
        with cols[idx % 5]:
            artist_img = artist.get("latest_measurement", {}).get("artist_image", "")
            if artist_img:
                st.image(artist_img, use_column_width=True)
            else:
                st.markdown('<div style="width:100%; height:150px; background-color:#444; border-radius:8px; display:flex; align-items:center; justify-content:center; color:#fff;">No Image</div>', unsafe_allow_html=True)
            artist_name = artist.get("artist_name", "Unbekannt")
            # Button, der die Suche simuliert
            if st.button(artist_name, key=f"fav_{artist.get('artist_id', artist_name)}"):
                st.session_state.search_query = artist_name
                st.experimental_rerun()
            # Kurze Infos zum Artist
            st.markdown(f"<p style='color:#ffffff;'>Popularity: {artist.get('latest_measurement', {}).get('artist_pop', 0)}</p>", unsafe_allow_html=True)
            st.markdown(f"<p style='color:#ffffff;'>Monthly Listeners: {artist.get('latest_measurement', {}).get('monthly_listeners', 0)}</p>", unsafe_allow_html=True)
