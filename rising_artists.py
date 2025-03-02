import streamlit as st
import requests
import datetime
import json
import time
import pandas as pd
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor
import uuid
import math
from math import isnan
from utils import set_background, set_dark_mode

st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

# Session-State initialisieren
if "log_messages" not in st.session_state:
    st.session_state["log_messages"] = []

# Log in der Sidebar in einem scrollbaren Expander
with st.sidebar.expander("Log-Details", expanded=True):
    log_placeholder = st.empty()

def log(msg):
    timestamp = datetime.datetime.now().strftime('%H:%M:%S')
    st.session_state["log_messages"].append(f"{timestamp} - {msg}")
    log_placeholder.text("\n".join(st.session_state["log_messages"]))

# === Notion-Konfiguration ===
tracking_db_id = st.secrets["notion"]["tracking_db_id"]      # Tracking-Datenbank für Songs
songs_database_id = st.secrets["notion"]["songs_db_id"]        # Songs-Datenbank
notion_secret = st.secrets["notion"]["token"]
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === Spotify-Konfiguration ===
SPOTIFY_CLIENT_ID = st.secrets["spotify"]["client_id"]
SPOTIFY_CLIENT_SECRET = st.secrets["spotify"]["client_secret"]

def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    log("Hole Spotify Token...")
    retry_count = 0
    while retry_count < 3:
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            token = response.json().get("accessToken")
            if token:
                log("Spotify Token erfolgreich abgerufen.")
                return token
        except requests.exceptions.RequestException as e:
            log(f"Fehler beim Abrufen des Spotify Tokens: {e}")
            time.sleep(2)
            retry_count += 1
    raise Exception("Spotify Token konnte nicht abgerufen werden (504 Gateway Timeout).")

SPOTIFY_TOKEN = get_spotify_token()
global SPOTIFY_HEADERS
SPOTIFY_HEADERS = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}

def get_spotify_popularity(track_id, token):
    log(f"Rufe Spotify Popularity für Track {track_id} ab...")
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("popularity", 0)

def get_spotify_playcount(track_id, token):
    log(f"Rufe Spotify Playcount für Track {track_id} ab...")
    variables = json.dumps({"uri": f"spotify:track:{track_id}"})
    extensions = json.dumps({
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "26cd58ab86ebba80196c41c3d48a4324c619e9a9d7df26ecca22417e0c50c6a4"
        }
    })
    params = {"operationName": "getTrack", "variables": variables, "extensions": extensions}
    token_value = SPOTIFY_HEADERS['Authorization'].split()[1]
    headers = {"Authorization": f"Bearer {token_value}"}
    url = "https://api-partner.spotify.com/pathfinder/v1/query"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return int(data["data"]["trackUnion"].get("playcount", 0))

def parse_rollup_text(rollup):
    texts = []
    if rollup and "array" in rollup:
        for item in rollup["array"]:
            if item.get("type") == "rich_text":
                for sub in item.get("rich_text", []):
                    texts.append(sub.get("plain_text", ""))
            elif item.get("type") == "date":
                date_info = item.get("date", {})
                if date_info.get("start"):
                    texts.append(date_info["start"])
    return " ".join(texts).strip()

def get_track_name_from_page(page_id):
    log(f"Lade Track Name für Seite {page_id}...")
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track Name" in page["properties"]:
            title_prop = page["properties"]["Track Name"].get("title", [])
            return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

def get_track_id_from_page(page_id):
    log(f"Lade Track ID für Seite {page_id}...")
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track ID" in page["properties"]:
            text_prop = page["properties"]["Track ID"].get("rich_text", [])
            return "".join([t.get("plain_text", "") for t in text_prop]).strip()
    return ""

def update_hype_for_measurement(entry_id, hype_score):
    log(f"Aktualisiere Hype Score ({hype_score}) für Eintrag {entry_id}...")
    url = f"{notion_page_endpoint}/{entry_id}"
    payload = {"properties": {"Hype Score": {"number": hype_score}}}
    response = requests.patch(url, headers=notion_headers, json=payload)
    response.raise_for_status()

def update_streams_for_measurement(entry_id, streams):
    log(f"Aktualisiere Streams ({streams}) für Eintrag {entry_id}...")
    url = f"{notion_page_endpoint}/{entry_id}"
    payload = {"properties": {"Streams": {"number": streams}}}
    response = requests.patch(url, headers=notion_headers, json=payload)
    response.raise_for_status()

def update_artist_info_for_measurement(entry_id, artist_data):
    log(f"Aktualisiere Artist Info für Eintrag {entry_id} (Artist ID: {artist_data.get('id')})...")
    url = f"{notion_page_endpoint}/{entry_id}"
    payload = {"properties": {
        "Artist ID": {"rich_text": [{"text": {"content": artist_data.get("id", "")}}]},
        "Artist Popularity": {"number": artist_data.get("popularity", 0)}
    }}
    response = requests.patch(url, headers=notion_headers, json=payload)
    response.raise_for_status()

def get_spotify_data(spotify_track_id):
    log(f"Lade Spotify-Daten (Cover, Link) für Track {spotify_track_id} ab...")
    url = f"https://api.spotify.com/v1/tracks/{spotify_track_id}"
    response = requests.get(url, headers={"Authorization": f"Bearer {SPOTIFY_TOKEN}"})
    if response.status_code == 200:
        data = response.json()
        cover_url = ""
        if data.get("album") and data["album"].get("images"):
            cover_url = data["album"]["images"][0].get("url", "")
        spotify_link = data.get("external_urls", {}).get("spotify", "")
        return cover_url, spotify_link
    return "", ""

@st.cache_data(show_spinner=False)
def get_metadata_from_tracking_db():
    log("Lade Notion-Metadaten...")
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    payload = {"page_size": 100}
    pages = []
    has_more = True
    start_cursor = None
    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        try:
            response = requests.post(url, headers=notion_headers, json=payload)
            if response.status_code == 429:
                log("Notion Rate Limit erreicht, warte 5 Sekunden...")
                time.sleep(5)
                continue
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            log(f"HTTPError beim Laden von Notion-Metadaten: {e}")
            raise
        data = response.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
    metadata = {}
    with ThreadPoolExecutor() as executor:
        futures = {}
        for page in pages:
            props = page.get("properties", {})
            song_relations = props.get("Song", {}).get("relation", [])
            if song_relations:
                related_page_id = song_relations[0].get("id")
                futures[related_page_id] = executor.submit(get_track_name_from_page, related_page_id)
        track_names = {key: future.result() for key, future in futures.items()}
    for page in pages:
        props = page.get("properties", {})
        song_relations = props.get("Song", {}).get("relation", [])
        if song_relations:
            related_page_id = song_relations[0].get("id")
            track_name = track_names.get(related_page_id, "Unbekannter Track")
            spotify_track_id = get_track_id_from_page(related_page_id)
            key = related_page_id
        else:
            track_name = "Unbekannter Track"
            spotify_track_id = ""
            key = page.get("id")
        artist_rollup = props.get("Artist", {}).get("rollup", {})
        artist = parse_rollup_text(artist_rollup)
        release_rollup = props.get("Release Date", {}).get("rollup", {})
        release_date = parse_rollup_text(release_rollup)
        metadata[key] = {
            "track_name": track_name,
            "artist": artist,
            "release_date": release_date,
            "spotify_track_id": spotify_track_id
        }
    log("Notion-Metadaten geladen.")
    return metadata

def get_tracking_entries():
    log("Lade Tracking-Daten von Notion...")
    if "tracking_entries" in st.session_state:
        return st.session_state.tracking_entries
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    payload = {"page_size": 100}
    pages = []
    has_more = True
    start_cursor = None
    retry_delay = 1
    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        try:
            response = requests.post(url, headers=notion_headers, json=payload)
            if response.status_code == 429:
                log("Notion Rate Limit beim Laden Tracking-Daten, warte 5 Sekunden...")
                time.sleep(5)
                continue
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            log(f"HTTPError beim Laden Tracking-Daten: {e}")
            raise
        data = response.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        retry_delay = 1
    entries = []
    for page in pages:
        entry_id = page.get("id")
        props = page.get("properties", {})
        pop = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        streams_val = props.get("Streams", {}).get("number")
        hype = props.get("Hype Score", {}).get("number")
        song_relations = props.get("Song", {}).get("relation", [])
        for relation in song_relations:
            song_id = relation.get("id")
            entries.append({
                "entry_id": entry_id,
                "song_id": song_id,
                "date": date_str,
                "popularity": pop,
                "Streams": streams_val,
                "hype_score": hype
            })
    st.session_state.tracking_entries = entries
    log("Tracking-Daten geladen.")
    return entries

def get_all_song_page_ids():
    log("Lade alle Song-Seiten aus Notion...")
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
    song_pages = []
    for page in pages:
        page_id = page["id"]
        popularity = 0
        if "Popularity" in page["properties"]:
            popularity = page["properties"]["Popularity"].get("number", 0)
        song_pages.append({"page_id": page_id, "popularity": popularity})
    log("Alle Song-Seiten geladen.")
    return song_pages

def get_tracking_entries_for_song(song_id):
    log(f"Lade Tracking-Daten für Song {song_id} aus Notion...")
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    payload = {
       "filter": {
           "property": "Song",
           "relation": {
               "contains": song_id
           }
       },
       "page_size": 100
    }
    response = requests.post(url, headers=notion_headers, json=payload)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
         entry_id = page.get("id")
         props = page.get("properties", {})
         pop = props.get("Popularity Score", {}).get("number")
         date_str = props.get("Date", {}).get("date", {}).get("start")
         streams_val = props.get("Streams", {}).get("number")
         hype = props.get("Hype Score", {}).get("number")
         entries.append({
              "entry_id": entry_id,
              "song_id": song_id,
              "date": date_str,
              "popularity": pop,
              "Streams": streams_val,
              "hype_score": hype
         })
    log(f"Tracking-Daten für Song {song_id} geladen.")
    return entries

def create_week_entry(song_page_id, popularity_score, track_id):
    log(f"Erstelle neuen Wochen-Eintrag für Song {song_page_id}...")
    now = datetime.datetime.now()
    now_with_offset = now + datetime.timedelta(seconds=1)
    now_iso = now_with_offset.isoformat()
    payload = {
        "parent": { "database_id": tracking_db_id },
        "properties": {
            "Name": {
                "title": [
                    { "text": { "content": f"Week of {now_iso[:10]}" } }
                ]
            },
            "Song": {
                "relation": [
                    { "id": song_page_id }
                ]
            },
            "Popularity Score": {
                "number": popularity_score
            },
            "Date": {
                "date": { "start": now_iso }
            },
            "Notion Track ID": {
                "rich_text": [
                    { "text": { "content": track_id } }
                ]
            }
        }
    }
    requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    log(f"Wochen-Eintrag für Song {song_page_id} erstellt.")

def update_popularity():
    log("Starte Aktualisierung: Popularity, Streams, Hype Score und Artist Info …")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    spotify_token = get_spotify_token()
    song_pages = get_all_song_page_ids()
    total = len(song_pages)
    
    # Neuen Wochen-Eintrag für jeden Song anlegen (mit aktueller Popularity)
    for idx, song in enumerate(song_pages):
        page_id = song["page_id"]
        status_text.text(f"Verarbeite Song: {page_id}")
        track_id = get_track_id_from_page(page_id)
        if not track_id:
            track_id = str(uuid.uuid4())
        try:
            spotify_pop = get_spotify_popularity(track_id, spotify_token)
        except Exception as e:
            log(f"Fehler bei Track ID {track_id}: {e}")
            spotify_pop = 0
        create_week_entry(page_id, spotify_pop, track_id)
        progress_bar.progress(int((idx + 1) / total * 100))
    
    status_text.text("Popularity aktualisiert. Jetzt Streams, Hype Score und Artist Info …")
    
    # Für jeden Song Streams, Hype Score und Artist Info updaten
    metadata = get_metadata_from_tracking_db()
    song_ids = list(metadata.keys())
    total_songs = len(song_ids)
    for idx, song_id in enumerate(song_ids):
        fresh_entries = get_tracking_entries_for_song(song_id)
        if not fresh_entries:
            continue
        df_song = pd.DataFrame(fresh_entries)
        df_song["date"] = pd.to_datetime(df_song["date"], errors="coerce")
        df_song = df_song.dropna(subset=["date"]).sort_values("date")
        latest_entry_id = df_song.iloc[-1]["entry_id"]
        song_meta = metadata.get(song_id, {})
        spotify_track_id = song_meta.get("spotify_track_id", "")
        streams = 0
        if spotify_track_id:
            try:
                streams = get_spotify_playcount(spotify_track_id, spotify_token)
                if streams == 0:
                    time.sleep(1)
                    streams = get_spotify_playcount(spotify_track_id, spotify_token)
            except Exception as e:
                log(f"Fehler beim Abrufen der Streams für {spotify_track_id}: {e}")
                streams = 0
        update_streams_for_measurement(latest_entry_id, streams)
        # Hype Score & Artist Info (live)
        artist_name = song_meta.get("artist", "")
        if artist_name:
            hype_score, artist_data = update_artist_on_demand(artist_name)
            update_hype_for_measurement(latest_entry_id, hype_score)
            update_artist_info_for_measurement(latest_entry_id, artist_data)
        else:
            update_hype_for_measurement(latest_entry_id, 0)
        progress_bar.progress((idx + 1) / total_songs)
    
    st.session_state.tracking_entries = get_tracking_entries()
    status_text.text("Alle Daten aktualisiert!")
    log("Update abgeschlossen!")

def get_artist_data(artist_name, token):
    log(f"Abrufen von Artist-Daten für {artist_name} …")
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(search_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    artists = data.get("artists", {}).get("items", [])
    if not artists:
        log(f"Keine Daten für Artist {artist_name} gefunden.")
        return None
    artist = artists[0]
    return {
        "id": artist.get("id"),
        "name": artist.get("name"),
        "popularity": artist.get("popularity", 0),
        "followers": artist.get("followers", {}).get("total", 0)
    }

def calculate_hype_score(artist_data):
    popularity = artist_data.get("popularity", 0)
    followers = artist_data.get("followers", 0)
    followers_score = math.log10(followers) * 10 if followers > 0 else 0
    hype = popularity + followers_score
    return round(min(hype, 100), 1)

def update_artist_on_demand(artist_name):
    token = get_spotify_token()
    artist_data = get_artist_data(artist_name, token)
    if artist_data:
        hype_score = calculate_hype_score(artist_data)
        return hype_score, artist_data
    return None, None

# ─────────────────────────────────────────
# UI / Darstellung
st.title("ARTIST SCOUT 1.0b")
st.header("Top 10 Songs to Watch")

with st.sidebar:
    st.markdown("## Automatische Updates")
    if st.button("Update Popularity / Streams / Hype Score"):
        update_popularity()
    if st.button("Refresh Daten"):
        if "tracking_entries" in st.session_state:
            del st.session_state["tracking_entries"]
        get_metadata_from_tracking_db.clear()
        st.experimental_rerun()
    st.markdown("---")
    with st.form("filter_form"):
        search_query = st.text_input("Song/Artist Suche", "")
        filter_pop_range = st.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1, key="filter_pop")
        submitted = st.form_submit_button("Filter anwenden")

if "tracking_entries" in st.session_state:
    tracking_entries = st.session_state.tracking_entries
else:
    tracking_entries = []

metadata = get_metadata_from_tracking_db()
df = pd.DataFrame(tracking_entries)

if df.empty:
    st.write("Tracking-Daten noch nicht aktualisiert. Bitte klicke auf 'Update Popularity / Streams / Hype Score'.")
    df_all = pd.DataFrame(columns=["entry_id", "song_id", "date", "popularity", "Streams", "hype_score"])
else:
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    df["track_name"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("track_name", "Unbekannter Track"))
    df["artist"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("artist", "Unbekannt"))
    df["release_date"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("release_date", ""))
    df["spotify_track_id"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("spotify_track_id", ""))
    df_all = df[df["date"].notnull()]

cumulative = []
for song_id, group in df_all.groupby("song_id"):
    group = group.sort_values("date")
    if group.empty:
        continue
    last_pop = group.iloc[-1]["popularity"]
    last_streams = group.iloc[-1]["Streams"]
    last_hype = group.iloc[-1].get("hype_score", 0)
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    cumulative.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "last_popularity": last_pop,
        "last_streams": last_streams,
        "last_hype": last_hype
    })

cum_df = pd.DataFrame(cumulative)
if cum_df.empty:
    st.write("Keine Daten für die Top 10 verfügbar.")
    top10 = pd.DataFrame()
else:
    top10 = cum_df.sort_values("last_popularity", ascending=False).head(10)

num_columns = 5
rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
for row_df in rows:
    cols = st.columns(num_columns)
    for idx, (_, row) in enumerate(row_df.iterrows()):
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with cols[idx]:
            st.markdown(f"**{row['track_name']}**\nArtist: {row['artist']}\nHype Score: {row['last_hype']}")
            if cover_url and spotify_link:
                st.markdown(f'<a href="{spotify_link}" target="_blank"><img src="{cover_url}" style="width:100%;" /></a>', unsafe_allow_html=True)
            elif cover_url:
                st.image(cover_url, use_container_width=True)
            else:
                st.write("Kein Cover")
            st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f} | Streams: {row['last_streams']}</div>", unsafe_allow_html=True)
            show_graph = st.checkbox("Graph anzeigen / ausblenden", key=f"toggle_{row['song_id']}")
            if show_graph:
                with st.spinner("Graph wird geladen…"):
                    fresh_entries = get_tracking_entries_for_song(row["song_id"])
                    if fresh_entries:
                        df_new = pd.DataFrame(fresh_entries)
                        df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce").dt.tz_localize(None)
                        song_history = df_new.sort_values("date")
                        fig = px.line(song_history, x="date", y=["popularity", "Streams", "hype_score"],
                                      title=f"{row['track_name']} – {row['artist']}",
                                      labels={"value": "Wert", "variable": "Metrik", "date": "Datum"},
                                      markers=True)
                        fig.update_yaxes(range=[0, 100])
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}_{time.time()}")
                    else:
                        st.write("Keine Tracking-Daten verfügbar.")

st.header("Songs filtern")
if submitted and not df_all.empty:
    last_data = []
    song_groups = list(df_all.groupby("song_id"))
    filter_progress = st.progress(0)
    total_groups = len(song_groups)
    for idx, (song_id, group) in enumerate(song_groups):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        last_streams = group.iloc[-1]["Streams"]
        last_hype = group.iloc[-1].get("hype_score", 0)
        meta = metadata.get(song_id, {
            "track_name": "Unbekannter Track",
            "artist": "Unbekannt",
            "release_date": "",
            "spotify_track_id": ""
        })
        last_data.append({
            "song_id": song_id,
            "track_name": meta.get("track_name", "Unbekannter Track"),
            "artist": meta.get("artist", "Unbekannt"),
            "release_date": meta.get("release_date", ""),
            "spotify_track_id": meta.get("spotify_track_id", ""),
            "last_popularity": last_pop,
            "last_streams": last_streams,
            "last_hype": last_hype
        })
        filter_progress.progress((idx + 1) / total_groups)
    last_df = pd.DataFrame(last_data)
    
    filtered_df = last_df[
        (last_df["last_popularity"] >= filter_pop_range[0]) &
        (last_df["last_popularity"] <= filter_pop_range[1])
    ]
    if search_query:
        sq = search_query.lower()
        filtered_df = filtered_df[
            filtered_df["track_name"].str.lower().str.contains(sq) |
            filtered_df["artist"].str.lower().str.contains(sq)
        ]
    filtered_df = filtered_df.sort_values("last_popularity", ascending=False)
    
    st.write("Gefilterte Songs:")
    for idx, row in filtered_df.iterrows():
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with st.container():
            st.markdown(f"""**{row['track_name']}** – {row['artist']}  
Release Date: {row['release_date']}  
Popularity: {row['last_popularity']:.1f} | Streams: {row['last_streams']} | Hype Score: {row['last_hype']}""")
            if cover_url:
                st.image(cover_url, width=100)
            if spotify_link:
                st.markdown(f"[{row['track_name']}]({spotify_link})", unsafe_allow_html=True)
            with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                song_history = df_all[df_all["song_id"] == row["song_id"]].sort_values("date")
                if len(song_history) == 1:
                    fig = px.scatter(song_history, x="date", y="popularity",
                                     title=f"{row['track_name']} - {row['artist']}",
                                     labels={"date": "Datum", "popularity": "Popularity"})
                else:
                    fig = px.line(song_history, x="date", y=["popularity", "Streams", "hype_score"],
                                  title=f"{row['track_name']} - {row['artist']}",
                                  labels={"value": "Wert", "variable": "Metrik", "date": "Datum"},
                                  markers=True)
                st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
else:
    if not df.empty:
        st.write("Bitte benutze das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")
