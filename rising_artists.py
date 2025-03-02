import streamlit as st
import requests
import datetime
import json
import time
import pandas as pd
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor
import uuid

st.set_page_config(layout="wide")

# === Notion-Konfiguration ===
tracking_db_id = "1a9b6204cede80e29338ede2c76999f2"  # Tracking-Datenbank (enthält Rollups für "Artist" und "Release Date", Relation "Song")
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === Spotify-Konfiguration ===
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

@st.cache_data(show_spinner=False)
def get_all_tracking_pages():
    """Lädt per Pagination alle Seiten aus der Tracking-Datenbank (cached)."""
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    payload = {}
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
    return pages

@st.cache_data(show_spinner=False)
def get_tracking_entries():
    """
    Extrahiert aus allen Seiten der Tracking-Datenbank (cached):
      - Popularity Score,
      - Zeitstempel aus dem Property "Date",
      - die Song-ID aus der Relation "Song",
      - Growth (sofern vorhanden).
    """
    pages = get_all_tracking_pages()
    entries = []
    for page in pages:
        entry_id = page.get("id")
        props = page.get("properties", {})
        pop = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        growth = props.get("Growth", {}).get("number")  # kann None sein
        song_relations = props.get("Song", {}).get("relation", [])
        for relation in song_relations:
            song_id = relation.get("id")
            entries.append({
                "entry_id": entry_id,
                "song_id": song_id,
                "date": date_str,
                "popularity": pop,
                "growth": growth
            })
    return entries

@st.cache_data(show_spinner=False)
def get_spotify_data(spotify_track_id):
    """Liefert Cover und Spotify-Link (gecacht)."""
    url = f"https://api.spotify.com/v1/tracks/{spotify_track_id}"
    response = requests.get(url, headers={"Authorization": f"Bearer {SPOTIFY_TOKEN}"})
    if response.status_code == 200:
        data = response.json()
        cover_url = ""
        if data.get("album") and data["album"].get("images"):
            cover_url = data["album"]["images"][0].get("url", "")
        spotify_link = data["external_urls"].get("spotify", "")
        return cover_url, spotify_link
    return "", ""

@st.cache_data(show_spinner=False)
def get_metadata_from_tracking_db():
    """
    Extrahiert aus allen Seiten der Tracking-Datenbank (cached):
      - Den Trackname, Artist, Release Date,
      - die Spotify Track ID aus dem Property "Track ID".
    """
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
            "spotify_track_id": spotify_track_id
        }
    return metadata

# --- Platzhalterfunktionen für Buttons ---
def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    song_list = ["Song A", "Song B", "Song C", "Song D", "Song E"]
    for i, song in enumerate(song_list):
        status_text.text(f"Rufe {song} ab...")
        time.sleep(1)
        progress_bar.progress((i + 1) / len(song_list))
    st.success("Neue Musik wurde hinzugefügt!")
    st.session_state.get_new_music_week = datetime.datetime.now().isocalendar()[1]
    status_text.empty()

def update_popularity():
    st.write("Füge neue Popularity-Messung hinzu...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    songs_database_id = "1a9b6204cede8006b67fd247dc660ba4"
    week_database_id = "1a9b6204cede80e29338ede2c76999f2"

    def get_all_song_page_ids():
        url = f"{notion_query_endpoint}/{songs_database_id}/query"
        response = requests.post(url, headers=notion_headers)
        response.raise_for_status()
        data = response.json()
        song_pages = []
        for page in data.get("results", []):
            page_id = page["id"]
            popularity = 0
            if "Popularity" in page["properties"]:
                popularity = page["properties"]["Popularity"].get("number", 0)
            song_pages.append({"page_id": page_id, "popularity": popularity})
        return song_pages

    def get_song_name(page_id):
        url = f"{notion_page_endpoint}/{page_id}"
        response = requests.get(url, headers=notion_headers)
        response.raise_for_status()
        data = response.json()
        props = data.get("properties", {})
        if "Name" in props and "title" in props["Name"]:
            title_items = props["Name"].get("title", [])
            song_name = "".join(item.get("plain_text", "") for item in title_items).strip()
            if song_name:
                return song_name
        return page_id

    def create_week_entry(song_page_id, popularity_score, track_id):
        now = datetime.datetime.now()
        now_with_offset = now + datetime.timedelta(seconds=1)
        now_iso = now_with_offset.isoformat()
        payload = {
            "parent": { "database_id": week_database_id },
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

    song_pages = get_all_song_page_ids()
    total = len(song_pages)
    song_to_track = {}
    for idx, song in enumerate(song_pages):
        page_id = song["page_id"]
        song_name = get_song_name(page_id)
        if song_name == page_id:
            song_name = get_track_name_from_page(page_id)
        status_text.text(f"Verarbeite Song: {song_name}")
        if not song_name:
            continue
        if page_id not in song_to_track:
            track_id = get_track_id_from_page(page_id)
            if not track_id:
                track_id = str(uuid.uuid4())
            song_to_track[page_id] = track_id
        else:
            track_id = song_to_track[page_id]
        create_week_entry(page_id, song["popularity"], track_id)
        progress_bar.progress(int((idx + 1) / total * 100))
    status_text.text("Alle Songs verarbeitet.")
    st.success("Popularity wurde aktualisiert!")
    status_text.empty()
    
    # Growth-Berechnung: Für jeden Song werden die beiden neuesten Messwerte verglichen und der Growth-Wert
    # wird in den neuesten Eintrag geschrieben.
    st.write("Berechne Growth für jeden Song...")
    # Cache leeren, um die aktuellsten Daten zu erhalten:
    get_all_tracking_pages.clear()
    get_tracking_entries.clear()
    updated_entries = get_tracking_entries()
    df_update = pd.DataFrame(updated_entries)
    df_update["date"] = pd.to_datetime(df_update["date"], errors="coerce")
    df_update = df_update.dropna(subset=["date", "song_id"])
    # Für jeden Song: Nutze den Growth-Wert der neuesten Messung aus der DB
    for song_id, group in df_update.groupby("song_id"):
        group = group.sort_values("date")
        # Hier nehmen wir den Growth-Wert des letzten Eintrags – falls vorhanden
        latest_growth = group.iloc[-1].get("growth")
        # Nur wenn latest_growth vorhanden und > 0, sonst 0
        final_growth = latest_growth if latest_growth and latest_growth > 0 else 0
        latest_entry_id = group.iloc[-1]["entry_id"]
        st.write(f"Song {song_id}: Growth = {final_growth:.1f}%")
        update_growth_for_measurement(latest_entry_id, final_growth)

# --- Sidebar: Buttons und Filterformular ---
with st.sidebar:
    st.markdown("## Automatische Updates")
    if st.button("Get New Music"):
        get_new_music()
    if st.button("Update Popularity"):
        update_popularity()
    st.markdown("---")
    with st.form("filter_form"):
        search_query = st.text_input("Song/Artist Suche", "")
        filter_pop_range = st.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1, key="filter_pop")
        # Nur Tracks mit Growth > 0 werden berücksichtigt
        filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
        filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
        filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
        filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
        filter_days = filter_timeframe_days[filter_timeframe_option]
        submitted = st.form_submit_button("Filter anwenden")

st.title("Song Tracking Übersicht")

# 1. Oben: Top 10 Songs – Wachstum über alle Messungen
st.header("Top 10 Songs – Wachstum über alle Messungen")

tracking_entries = get_tracking_entries()
metadata = get_metadata_from_tracking_db()
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
    # Verwende den Growth-Wert des letzten Eintrags (aus der DB) – falls vorhanden, sonst berechne kumulativ
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
    # Nur Songs mit Growth > 0 berücksichtigen und die 10 mit den größten Growth-Werten auswählen
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
                    fig.update_yaxes(range=[0, 100])
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}_{time.time()}")
                    
# 2. Unterhalb: Filterergebnisse
st.header("Songs filtern")
if submitted:
    last_data = []
    for song_id, group in df_all.groupby("song_id"):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            growth_val = ((last_pop - prev_pop) / prev_pop) * 100 if prev_pop and prev_pop != 0 else 0
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        last_data.append({
            "song_id": song_id,
            "track_name": meta.get("track_name", "Unbekannter Track"),
            "artist": meta.get("artist", "Unbekannter"),
            "release_date": meta.get("release_date", ""),
            "spotify_track_id": meta.get("spotify_track_id", ""),
            "last_popularity": last_pop,
            "growth": growth_val
        })
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
