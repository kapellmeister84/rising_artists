import streamlit as st
import requests
import datetime
import json
import time
import re
import math
import plotly.express as px
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from utils import set_background, set_dark_mode

# --- Page Configuration & Dark Mode ---
st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

# --- Globales CSS ---
st.markdown("""
<style>
a {
    color: #ffffff !important;
    text-decoration: none !important;
}
a:hover {
    color: #f0f0f0 !important;
}
.stTextInput>div>div>input {
    background-color: #ffffff;
    color: #000000;
}
.stButton button {
    color: #000000 !important;
}
.artist-card, .song-card {
    border: 2px solid #ffffff;
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
# Logging & Fortschritt
#############################
log_container = st.empty()
progress_container = st.empty()

def log(msg):
    if "log_messages" not in st.session_state:
        st.session_state.log_messages = []
    st.session_state.log_messages.append(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")
    log_content = "\n".join(st.session_state.log_messages)
    log_container.markdown(
        f"""<div style="height:200px; overflow-y: auto; border:1px solid #ccc; padding: 5px; background-color:#f9f9f9;">
            <pre style="white-space: pre-wrap; margin:0;">{log_content}</pre>
        </div>""",
        unsafe_allow_html=True
    )

def show_progress(current, total, info=""):
    progress = current / total
    progress_container.progress(progress)
    progress_container.write(info)

#############################
# Notion-Daten: Songs & Measurements
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
            track_name = "".join([t.get("plain_text", "") for t in props.get("Track Name", {}).get("title", [])]).strip()
            artist_name = "".join([t.get("plain_text", "") for t in props.get("Artist Name", {}).get("rich_text", [])]).strip()
            artist_id = "".join([t.get("plain_text", "") for t in props.get("Artist ID", {}).get("rich_text", [])]).strip()
            track_id = "".join([t.get("plain_text", "") for t in props.get("Track ID", {}).get("rich_text", [])]).strip()
            release_date = props.get("Release Date", {}).get("date", {}).get("start", "")
            country_code = "".join([t.get("plain_text", "") for t in props.get("Country Code", {}).get("rich_text", [])]).strip()
            last_edited = page.get("last_edited_time", "")
            favourite = props.get("Favourite", {}).get("checkbox", False)
            measurements_ids = []
            if props.get("Measurements", {}).get("relation"):
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
# Query-Parameter auslesen
#############################
query_params = st.query_params
if "search_query" in query_params:
    st.session_state.search_query = query_params["search_query"][0]

#############################
# Safe Timestamp
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
# Hype Score Berechnung
#############################
def compute_song_hype(song):
    measurements = song.get("measurements", [])
    if len(measurements) < 2:
        return 0
    sorted_ms = sorted(measurements, key=lambda m: safe_timestamp(m))
    latest, previous = sorted_ms[-1], sorted_ms[-2]
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
    latest, previous = sorted_ms[-1], sorted_ms[-2]
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
    payload = {"properties": {"Hype Score": {"number": hype_score}}}
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
        "persistedQuery": {"version": 1, "sha256Hash": "26cd58ab86ebba80196c41c3d48a4324c619e9a9d7df26ecca22417e0c50c6a4"}
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
    payload = {"properties": {"Favourite": {"checkbox": new_state}}}
    st.write(f"Updating page {page_id} with payload: {payload}")
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
        if props.get("Measurements", {}).get("relation"):
            current_rels = props["Measurements"]["relation"]
        if not any(rel.get("id") == new_measurement_id for rel in current_rels):
            current_rels.append({"id": new_measurement_id})
        payload = {"properties": {"Measurements": {"relation": current_rels}}}
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
# Anzeige der Suchergebnisse
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
                st.markdown(f'<a href="{artist_link}" target="_self"><img src="{artist_img}" alt="Artist" style="width:120px; height:120px; border-radius:50%; object-fit:cover;"></a>', unsafe_allow_html=True)
            with cols_artist[1]:
                st.markdown(f"<h1><a href='{artist_link}' target='_self' style='color:#ffffff;'>{artist_name}</a></h1>", unsafe_allow_html=True)
                st.markdown(f"<p style='color:#ffffff;'>Popularity: {artist_pop}</p>", unsafe_allow_html=True)
                st.markdown(f"<p style='color:#ffffff;'>Monthly Listeners: {monthly_listeners}</p>", unsafe_allow_html=True)
                st.markdown(f"<p style='color:#ffffff;'>Followers: {artist_followers}</p>", unsafe_allow_html=True)
                st.markdown(f"<p style='font-size:1.3rem; color:#ffffff;'><strong>Artist Hype Score: <span style='font-size:1.6rem; color:#FFD700;'>{hype_artist:.1f}</span></strong></p>", unsafe_allow_html=True)
            with cols_artist[2]:
                with st.expander("Show Artist Charts", expanded=False):
                    fig_pop = get_artist_pop_monthly_figure(rep.get("measurements", []))
                    if fig_pop:
                        st.plotly_chart(fig_pop, use_container_width=True)
                    fig_follow = get_artist_followers_figure(rep.get("measurements", []))
                    if fig_follow:
                        st.plotly_chart(fig_follow, use_container_width=True)
            with cols_artist[3]:
                if st.button(f"{star_icon}", key=f"fav_{artist_id}"):
                    toggle_favourite_for_artist(artist_id, not fav_state)
                    st.session_state.fav_updated = True
            st.markdown("---", unsafe_allow_html=True)
        
        # Song-Karten
        for song in songs:
            with st.container():
                cols_song = st.columns([1, 2])
                with cols_song[0]:
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
                    st.markdown(f'<a href="{song_link}" target="_self"><img src="{cover_url}" alt="Cover" style="width:100%; border-radius:8px; object-fit:cover;"></a>', unsafe_allow_html=True)
                    song_title = song.get("track_name", "Unknown Song")
                    st.markdown(f"<h2 style='margin: 10px 0 5px 0; color:#ffffff;'><a href='{song_link}' target='_self' style='color:#ffffff;'>{song_title}</a></h2>", unsafe_allow_html=True)
                    st.markdown(f"<p style='color:#ffffff;'><strong>Release Date:</strong> {song.get('release_date')}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='color:#ffffff;'><strong>Song Pop:</strong> {song.get('latest_measurement', {}).get('song_pop', 0)}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size:1.2rem; color:#ffffff;'><strong>Hype Score: <span style='font-size:1.6rem; color:#FFD700;'>{compute_song_hype(song):.1f}</span></strong></p>", unsafe_allow_html=True)
                with cols_song[1]:
                    st.markdown("<p style='color:#ffffff;'>Streams</p>", unsafe_allow_html=True)
                    fig_streams = get_song_streams_figure(song.get("measurements", []))
                    if fig_streams:
                        st.plotly_chart(fig_streams, use_container_width=True)
                    else:
                        st.write("No Streams Data")
                    st.markdown("<p style='color:#ffffff;'>Popularity</p>", unsafe_allow_html=True)
                    fig_pop = get_song_pop_figure(song.get("measurements", []))
                    if fig_pop:
                        st.plotly_chart(fig_pop, use_container_width=True)
                    else:
                        st.write("No Popularity Data")
            st.markdown("<hr>", unsafe_allow_html=True)

#############################
# Sidebar: Suchfeld, Filter, Buttons
#############################
# Übernehme den Suchbegriff aus dem Session-State als Default
default_search = st.session_state.get("search_query", "")
search_query = st.sidebar.text_input("Search by artist or song:", default_search)
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
            else:
                log(f"{s.get('name')} hat keine Track ID und wird übersprungen.")
    run_get_new_music()
    log("Get New Music abgeschlossen. Bitte Seite neu laden.")
    
if st.sidebar.button("Get Data", key="get_data_button"):
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
                    raw = 0 if abs(growth) < EPSILON else current_base + growth
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
    msgs = fill_song_measurements()
    for msg in msgs:
        log(msg)

def song_exists_in_notion(track_id):
    payload = {"filter": {"property": "Track ID", "rich_text": {"equals": track_id}}}
    r = requests.post(f"{notion_query_endpoint}/{songs_database_id}/query", headers=notion_headers, json=payload)
    if r.status_code == 200:
        return len(r.json().get("results", [])) > 0
    else:
        st.error("Notion-Query Fehler: " + r.text)
        return False

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

# Wenn ein Suchbegriff (auch aus Query) vorhanden ist, starte die Suche
if start_search or confirm_filters or st.session_state.get("search_query"):
    if search_query:
        found = search_songs(search_query)
    else:
        found = songs_metadata
    final_results = apply_filters_and_sort(found)
    display_search_results(final_results)
else:
    st.title("Search Results")
    st.write("Bitte einen Suchbegriff eingeben oder Filter bestätigen.")

#############################
# Persistente Speicherung "Zuletzt angesehen"
#############################
RECENT_SEARCHES_FILE = "recent_searches.json"

def load_recent_searches():
    if os.path.exists(RECENT_SEARCHES_FILE):
        with open(RECENT_SEARCHES_FILE, "r") as f:
            return json.load(f)
    return []

def save_recent_searches(data):
    with open(RECENT_SEARCHES_FILE, "w") as f:
        json.dump(data, f)

if "recent_searches" not in st.session_state:
    st.session_state.recent_searches = load_recent_searches()

if st.session_state.recent_searches:
    for tile in st.session_state.recent_searches:
        for song in songs_metadata.values():
            if song.get("artist_name") == tile["artist_name"]:
                new_meas = song.get("latest_measurement", {})
                tile["artist_img"] = new_meas.get("artist_image", tile.get("artist_img"))
                tile["artist_pop"] = new_meas.get("artist_pop", tile.get("artist_pop"))
                tile["monthly_listeners"] = new_meas.get("monthly_listeners", tile.get("monthly_listeners"))
                tile["artist_hype"] = compute_artist_hype(song)
                break
    save_recent_searches(st.session_state.recent_searches)

if start_search or confirm_filters:
    if final_results:
        grouped = group_results_by_artist(final_results)
        recent_tiles = []
        for group_key, songs in grouped.items():
            rep = songs[0]
            tile = {
                "artist_img": rep.get("latest_measurement", {}).get("artist_image", ""),
                "artist_name": rep.get("artist_name", "Unbekannt"),
                "artist_pop": rep.get("latest_measurement", {}).get("artist_pop", 0),
                "monthly_listeners": rep.get("latest_measurement", {}).get("monthly_listeners", 0),
                "artist_hype": compute_artist_hype(rep)
            }
            recent_tiles.append(tile)
        for tile in recent_tiles:
            if tile not in st.session_state.recent_searches:
                st.session_state.recent_searches.insert(0, tile)
        st.session_state.recent_searches = st.session_state.recent_searches[:5]
        save_recent_searches(st.session_state.recent_searches)

if st.session_state.recent_searches:
    st.header("Zuletzt angesehen")
    cols = st.columns(5)
    for idx, tile in enumerate(st.session_state.recent_searches):
        with cols[idx % 5]:
            link = f"?search_query={tile['artist_name']}"
            st.markdown(f"""
            <a href="{link}" target="_self" style="text-decoration: none; color: inherit;">
                <div style="border: 2px solid #ffffff; border-radius: 8px; padding: 10px; background-color: #444444; text-align: center; cursor: pointer;">
                    <img src="{tile['artist_img']}" alt="{tile['artist_name']}" style="width:120px; height:120px; border-radius:50%; object-fit:cover;">
                    <h3 style="margin: 10px 0 5px 0;">{tile['artist_name']}</h3>
                    <p style="margin: 0; color:#ffffff;">Popularity: {tile['artist_pop']}</p>
                    <p style="margin: 0; color:#ffffff;">Monthly Listeners: {tile['monthly_listeners']}</p>
                    <p style="margin: 0; color:#ffffff;">Hype Score: {tile.get('artist_hype', 0):.1f}</p>
                </div>
            </a>
            """, unsafe_allow_html=True)

if not st.session_state.get("log_messages"):
    log_container.empty()
    progress_container.empty()
