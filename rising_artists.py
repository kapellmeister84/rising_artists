import streamlit as st
import requests
import datetime
import json
import time
import pandas as pd
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor
import uuid
from utils import set_background, set_dark_mode

st.set_page_config(layout="wide")
set_dark_mode()
set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

# === Notion-Konfiguration ===
tracking_db_id = st.secrets["notion"]["tracking_db_id"]      # Weeks-/Tracking-Datenbank
songs_database_id = st.secrets["notion"]["songs_db_id"]          # Songs-Datenbank
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
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("accessToken")
SPOTIFY_TOKEN = get_spotify_token()

# --- Hilfsfunktionen ---
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
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track Name" in page["properties"]:
            title_prop = page["properties"]["Track Name"].get("title", [])
            return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

def get_track_id_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track ID" in page["properties"]:
            text_prop = page["properties"]["Track ID"].get("rich_text", [])
            return "".join([t.get("plain_text", "") for t in text_prop]).strip()
    return ""

def update_growth_for_measurement(entry_id, growth):
    url = f"{notion_page_endpoint}/{entry_id}"
    payload = {"properties": {"Growth": {"number": growth}}}
    response = requests.patch(url, headers=notion_headers, json=payload)
    response.raise_for_status()

def update_streams_for_measurement(entry_id, streams):
    url = f"{notion_page_endpoint}/{entry_id}"
    payload = {"properties": {"Streams": {"number": streams}}}
    response = requests.patch(url, headers=notion_headers, json=payload)
    response.raise_for_status()

# Diese Funktionen müssen vor ihrer Verwendung definiert sein:
@st.cache_data(ttl=300, show_spinner=False)
def get_all_tracking_pages():
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    payload = {"page_size": 100}
    pages = []
    has_more = True
    start_cursor = None
    retry_delay = 1
    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=notion_headers, json=payload)
        if response.status_code == 429:
            time.sleep(retry_delay)
            retry_delay *= 2
            continue
        response.raise_for_status()
        data = response.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        retry_delay = 1
    return pages

@st.cache_data(ttl=300, show_spinner=False)
def get_tracking_entries():
    pages = get_all_tracking_pages()
    entries = []
    for page in pages:
        entry_id = page.get("id")
        props = page.get("properties", {})
        pop = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        growth = props.get("Growth", {}).get("number")
        song_relations = props.get("Song", {}).get("relation", [])
        for relation in song_relations:
            song_id = relation.get("id")
            entries.append({
                "entry_id": entry_id,
                "song_id": song_id,
                "date": date_str,
                "popularity": pop,
                "growth": growth,
            })
    return entries

@st.cache_data(ttl=300, show_spinner=False)
def get_spotify_data(spotify_track_id):
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
    pages = get_all_tracking_pages()
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
            "spotify_track_id": spotify_track_id,
        }
    return metadata

# --- Sidebar: Refresh Button und Filterformular ---
with st.sidebar:
    st.markdown("## Automatische Updates")
    if st.button("Refresh Daten"):
        get_all_tracking_pages.clear()
        get_tracking_entries.clear()
        st.experimental_rerun()
    st.markdown("---")
    with st.form("filter_form"):
        search_query = st.text_input("Song/Artist Suche", "")
        filter_pop_range = st.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1, key="filter_pop")
        filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
        filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
        filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
        filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "

st.title("ARTIST SCOUT 1.0b")
st.header("Top 10 songs to watch")

# Lade Tracking-Daten und Meta-Daten nur, wenn sie gebraucht werden
tracking_entries = get_tracking_entries()
metadata = get_metadata_from_tracking_db()

# Wenn keine Daten vorhanden sind, stoppe das Skript
df = pd.DataFrame(tracking_entries)
if df.empty:
    st.write("Keine Tracking-Daten gefunden.")
    st.stop()

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
        last_growth = group.iloc[-1].get("growth")
        if last_growth is None:
            first_pop = group.iloc[0]["popularity"]
            cumulative_growth = ((last_pop - first_pop) / first_pop) * 100 if first_pop and first_pop != 0 else 0
        else:
            cumulative_growth = last_growth
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        cumulative.append({
            "song_id": song_id,
            "track_name": meta["track_name"],
            "artist": meta["artist"],
            "release_date": meta["release_date"],
            "spotify_track_id": meta["spotify_track_id"],
            "last_popularity": last_pop,
            "cumulative_growth": cumulative_growth
        })

    cum_df = pd.DataFrame(cumulative)
    if cum_df.empty:
        st.write("Keine Daten für die Top 10 verfügbar.")
        top10 = pd.DataFrame()
    else:
        top10 = cum_df[cum_df["cumulative_growth"] > 0].sort_values("cumulative_growth", ascending=False).head(10)

    num_columns = 5
    rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
    for row_df in rows:
        cols = st.columns(num_columns)
        for idx, (_, row) in enumerate(row_df.iterrows()):
            cover_url, spotify_link = ("", "")
            if row["spotify_track_id"]:
                cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
            with cols[idx]:
                st.markdown(f"{row['track_name']}", unsafe_allow_html=True)
                if cover_url and spotify_link:
                    st.markdown(f'<a href="{spotify_link}" target="_blank"><img src="{cover_url}" style="width:100%;" /></a>', unsafe_allow_html=True)
                elif cover_url:
                    st.image(cover_url, use_container_width=True)
                else:
                    st.write("Kein Cover")
                st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center; font-weight: bold;'>Growth: {row['cumulative_growth']:.1f}%</div>", unsafe_allow_html=True)
                
                show_graph = st.checkbox("Graph anzeigen / ausblenden", key=f"toggle_{row['song_id']}")
                if show_graph:
                    with st.spinner("Graph wird geladen..."):
                        fresh_entries = get_tracking_entries_for_song(row["song_id"])
                        if fresh_entries:
                            df_new = pd.DataFrame(fresh_entries)
                            df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce").dt.tz_localize(None)
                            song_history = df_new.sort_values("date")
                            if len(song_history) == 1:
                                fig = px.scatter(song_history, x="date", y="popularity",
                                                 title=f"{row['track_name']} - {row['artist']}",
                                                 labels={"date": "Datum", "popularity": "Popularity Score"})
                            else:
                                fig = px.line(song_history, x="date", y="popularity",
                                              title=f"{row['track_name']} - {row['artist']}",
                                              labels={"date": "Datum", "popularity": "Popularity Score"},
                                              markers=True)
                            fig.update_yaxes(range=[0, 100])
                            st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}_{time.time()}")
                            
                            show_streams_graph = st.checkbox("Stream Wachstum anzeigen", key=f"toggle_stream_{row['song_id']}")
                            if show_streams_graph:
                                if "Streams" in song_history.columns and not song_history["Streams"].isnull().all():
                                    if len(song_history) == 1:
                                        fig_stream = px.scatter(song_history, x="date", y="Streams",
                                                                title=f"{row['track_name']} - Stream Wachstum",
                                                                labels={"date": "Datum", "Streams": "Streams"})
                                    else:
                                        fig_stream = px.line(song_history, x="date", y="Streams",
                                                            title=f"{row['track_name']} - Stream Wachstum",
                                                            labels={"date": "Datum", "Streams": "Streams"},
                                                            markers=True)
                                    st.plotly_chart(fig_stream, use_container_width=True, key=f"chart_stream_{row['song_id']}_{time.time()}")
                                else:
                                    st.write("Keine Stream-Daten verfügbar")
                        else:
                            st.write("Keine aktuellen Tracking-Daten verfügbar")
                    
st.header("Songs filtern")
if submitted:
    last_data = []
    song_groups = list(df_all.groupby("song_id"))
    filter_progress = st.progress(0)
    total_groups = len(song_groups)
    for idx, (song_id, group) in enumerate(song_groups):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            growth_val = ((last_pop - prev_pop) / prev_pop) * 100 if prev_pop and prev_pop != 0 else 0
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
            "growth": growth_val
        })
        filter_progress.progress((idx + 1) / total_groups)
    last_df = pd.DataFrame(last_data)
    
    filtered_df = last_df[
        (last_df["last_popularity"] >= filter_pop_range[0]) &
        (last_df["last_popularity"] <= filter_pop_range[1]) &
        (last_df["growth"] >= filter_growth_threshold)
    ]
    if search_query:
        sq = search_query.lower()
        filtered_df = filtered_df[
            filtered_df["track_name"].str.lower().str.contains(sq) |
            filtered_df["artist"].str.lower().str.contains(sq)
        ]
    if filter_sort_option == "Popularity":
        filtered_df = filtered_df.sort_values("last_popularity", ascending=False)
    elif filter_sort_option == "Release Date":
        filtered_df["release_date_dt"] = pd.to_datetime(filtered_df["release_date"], errors="coerce")
        filtered_df = filtered_df.sort_values("release_date_dt", ascending=True)
    
    st.write("Gefilterte Songs:")
    for idx, row in filtered_df.iterrows():
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with st.container():
            st.markdown(f"""**{row['track_name']}** – {row['artist']}  
Release Date: {row['release_date']}  
Popularity: {row['last_popularity']:.1f} | Growth: {row['growth']:.1f}%""")
            if cover_url:
                st.image(cover_url, width=100)
            if spotify_link:
                st.markdown(f"[{row['track_name']}]({spotify_link})", unsafe_allow_html=True)
            with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                song_history = df_all[df_all["song_id"] == row["song_id"]].sort_values("date")
                if len(song_history) == 1:
                    fig = px.scatter(song_history, x="date", y="popularity",
                                     title=f"{row['track_name']} - {row['artist']}",
                                     labels={"date": "Datum", "popularity": "Popularity Score"})
                else:
                    fig = px.line(song_history, x="date", y="popularity",
                                  title=f"{row['track_name']} - {row['artist']}",
                                  labels={"date": "Datum", "popularity": "Popularity Score"},
                                  markers=True)
                st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
else:
    st.write("Bitte verwenden Sie das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")
