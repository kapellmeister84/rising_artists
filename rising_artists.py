import streamlit as st
import requests
import datetime
import json
import time
import random
import re
import pandas as pd
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")

# === Notion-Konfiguration ===
songs_database_id = "b94c8042619d42a3be799c9795984150"  # Songs-Datenbank
weeks_db_id = "1a9b6204cede80e29338ede2c76999f2"         # Weeks-Datenbank
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === Spotify-Konfiguration (falls benötigt) ===
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
    response = requests.post(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track Name" in page["properties"]:
            title_prop = page["properties"]["Track Name"].get("title", [])
            return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

def get_track_id_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.post(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track ID" in page["properties"]:
            text_prop = page["properties"]["Track ID"].get("rich_text", [])
            return "".join([t.get("plain_text", "") for t in text_prop]).strip()
    return ""

# Hole alle Song-Seiten aus der Songs-Datenbank und lese auch Artist und Release Date
def get_all_song_page_ids():
    url = f"{notion_query_endpoint}/{songs_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    song_pages = []
    for page in data.get("results", []):
        page_id = page["id"]
        # Popularity aus Songs-Datenbank (Property "Popularity")
        popularity = 0
        if "Popularity" in page["properties"]:
            popularity = page["properties"]["Popularity"].get("number", 0)
        # Artist (angenommen als Rich Text in Property "Artist")
        artist = ""
        if "Artist" in page["properties"]:
            artist_prop = page["properties"]["Artist"].get("rich_text", [])
            if artist_prop:
                artist = artist_prop[0].get("plain_text", "")
        # Release Date (angenommen als Date-Property "Release Date")
        release_date = ""
        if "Release Date" in page["properties"]:
            date_prop = page["properties"]["Release Date"].get("date", {})
            release_date = date_prop.get("start", "")
        song_pages.append({
            "page_id": page_id,
            "popularity": popularity,
            "artist": artist,
            "release_date": release_date
        })
    return song_pages

# --- Funktion zum Erstellen eines neuen Week-Eintrags (Messung) ---
def create_week_entry(song_id, popularity_score, code, artist, release_date):
    now_iso = datetime.datetime.now().isoformat()
    payload = {
        "parent": {"database_id": weeks_db_id},
        "properties": {
            "Track Code": {
                "title": [
                    {"text": {"content": code}}
                ]
            },
            "Song": {
                "relation": [{"id": song_id}]
            },
            "Artist": {
                "rich_text": [{"text": {"content": artist}}]
            },
            "Release Date": {
                "date": {"start": release_date} if release_date else None
            },
            "Popularity Score": {"number": popularity_score},
            "Date": {
                "date": {"start": now_iso}
            }
        }
    }
    # Entferne ggf. das Release Date-Feld, falls kein Datum vorhanden ist
    if not release_date:
        payload["properties"].pop("Release Date")
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        print(f"Erfolgreich Week Entry für Song {song_id} mit Code '{code}' erstellt.")
    else:
        print(f"Fehler beim Erstellen des Week Entry für Song {song_id}: {response.text}")

# --- Update Popularity: Für jeden Song aus der Songs-Datenbank wird ein neuer Messwert angelegt ---
def update_popularity():
    print("Füge neue Popularity-Messung hinzu...")
    run_time = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    song_pages = get_all_song_page_ids()
    print(f"Gefundene Songs: {len(song_pages)}")
    codes = {}
    for song in song_pages:
        artist = song.get("artist", "Unknown")
        sanitized_artist = re.sub(r'\W+', '', artist)
        if sanitized_artist not in codes:
            codes[sanitized_artist] = "TrackCode-" + sanitized_artist + "-" + run_time
        code = codes[sanitized_artist]
        create_week_entry(
            song_id=song["page_id"],
            popularity_score=song["popularity"],
            code=code,
            artist=artist,
            release_date=song.get("release_date", "")
        )
        time.sleep(0.3)
    print("Popularity wurde aktualisiert. Neue Einträge mit folgenden Codes wurden angelegt:")
    print(", ".join(codes.values()))

# --- Funktionen für die Streamlit-Oberfläche (Anzeige, Filter, Graphen) ---
@st.cache_data(show_spinner=False)
def get_tracking_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
        entry_id = page.get("id")
        props = page.get("properties", {})
        pop = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        song_relations = props.get("Song", {}).get("relation", [])
        for relation in song_relations:
            song_id = relation.get("id")
            entries.append({"entry_id": entry_id, "song_id": song_id, "date": date_str, "popularity": pop})
    return entries

@st.cache_data(show_spinner=False)
def get_spotify_data(spotify_track_id):
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
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    metadata = {}
    with ThreadPoolExecutor() as executor:
        futures = {}
        for page in data.get("results", []):
            props = page.get("properties", {})
            song_rel = props.get("Song", {}).get("relation", [])
            if song_rel:
                related_page_id = song_rel[0].get("id")
                futures[related_page_id] = executor.submit(get_track_name_from_page, related_page_id)
        track_names = {key: future.result() for key, future in futures.items()}
    for page in data.get("results", []):
        props = page.get("properties", {})
        song_rel = props.get("Song", {}).get("relation", [])
        if song_rel:
            related_page_id = song_rel[0].get("id")
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

@st.cache_data(show_spinner=False)
def get_weeks_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    results = []
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        track_code_prop = props.get("Track Code", {}).get("title", [])
        code_str = track_code_prop[0]["plain_text"] if track_code_prop else ""
        song_rel = props.get("Song", {}).get("relation", [])
        song_id = song_rel[0]["id"] if song_rel else None
        pop_score = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        results.append({
            "page_id": page_id,
            "track_code": code_str,
            "song_id": song_id,
            "popularity": pop_score,
            "date_str": date_str
        })
    return results

def build_code_graph(code):
    data = get_weeks_entries()
    df_code = [r for r in data if r["track_code"] == code]
    if not df_code:
        return None
    df = pd.DataFrame(df_code)
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
    df = df.sort_values("date")
    if df["date"].nunique() <= 1:
        fig = px.scatter(df, x="date", y="popularity",
                         title=f"Messungen für Code {code}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(df, x="date", y="popularity",
                      title=f"Messungen für Code {code}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    return fig

def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    # Hier deinen Code einfügen...
    st.success("Neue Musik wurde hinzugefügt!")

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
now = pd.Timestamp.now(tz='UTC')
start_2days = now - pd.Timedelta(days=2)
df_2days = df[df["date"] >= start_2days]
cumulative = []
for song_id, group in df_2days.groupby("song_id"):
    group = group.sort_values("date")
    if group.empty:
        continue
    first_pop = group.iloc[0]["popularity"]
    last_pop = group.iloc[-1]["popularity"]
    growth = ((last_pop - first_pop) / first_pop) * 100 if first_pop and first_pop != 0 else 0
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    cumulative.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "last_popularity": last_pop,
        "cumulative_growth": growth
    })
cum_df = pd.DataFrame(cumulative)
if cum_df.empty:
    st.write("Keine Daten für die Top 10 verfügbar.")
    top10 = pd.DataFrame()
else:
    top10 = cum_df.sort_values("cumulative_growth", ascending=False).head(10)
num_columns = 5
rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
for row_df in rows:
    cols = st.columns(num_columns)
    for idx, (_, row) in enumerate(row_df.iterrows()):
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with cols[idx]:
            if cover_url:
                st.image(cover_url, use_container_width=True)
            else:
                st.write("Kein Cover")
            short_title = row["track_name"][:40] + "..." if len(row["track_name"]) > 40 else row["track_name"]
            short_artist = row["artist"][:40] + "..." if len(row["artist"]) > 40 else row["artist"]
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>{short_title}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-style: italic;'>{short_artist}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f}</div>", unsafe_allow_html=True)
            if spotify_link:
                st.markdown(f"<div style='text-align: center;'><a href='{spotify_link}' target='_blank'>Spotify Link</a></div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>Growth: {row['cumulative_growth']:.1f}%</div>", unsafe_allow_html=True)

# 2. Filterergebnisse: Im Expander wird der Graph für einen bestimmten Code angezeigt
st.header("Songs filtern")
if submitted:
    last_data = []
    for song_id, group in df.groupby("song_id"):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0.0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            if prev_pop and prev_pop != 0:
                growth_val = ((last_pop - prev_pop) / prev_pop) * 100
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        last_data.append({
            "song_id": song_id,
            "track_name": meta.get("track_name", "Unbekannter Track"),
            "artist": meta.get("artist", "Unbekannt"),
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
                st.markdown(f"[Spotify Link]({spotify_link})")
            with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                code_input = st.text_input("Code für Messungen eingeben", key=f"code_{row['song_id']}")
                if code_input:
                    fig = build_code_graph(code_input)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
                    else:
                        st.write("Keine Messungen für diesen Code gefunden.")
else:
    st.write("Bitte verwenden Sie das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")

# Neue Funktion: Baut einen Graphen, der alle Einträge mit einem bestimmten Code (im Title-Property "Track Code") anzeigt
def build_code_graph(code):
    data = get_weeks_entries()
    df_code = [r for r in data if r["track_code"] == code]
    if not df_code:
        return None
    df = pd.DataFrame(df_code)
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
    df = df.sort_values("date")
    if df["date"].nunique() <= 1:
        fig = px.scatter(df, x="date", y="popularity",
                         title=f"Messungen für Code {code}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(df, x="date", y="popularity",
                      title=f"Messungen für Code {code}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    return fig

@st.cache_data(show_spinner=False)
def get_weeks_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    results = []
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        track_code_prop = props.get("Track Code", {}).get("title", [])
        code_str = track_code_prop[0]["plain_text"] if track_code_prop else ""
        song_rel = props.get("Song", {}).get("relation", [])
        song_id = song_rel[0]["id"] if song_rel else None
        pop_score = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        results.append({
            "page_id": page_id,
            "track_code": code_str,
            "song_id": song_id,
            "popularity": pop_score,
            "date_str": date_str
        })
    return results

# Optional: Funktion für Get New Music
def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    # Hier deinen Code einfügen...
    st.success("Neue Musik wurde hinzugefügt!")

# --- Hauptprogramm: Sidebar und Anzeige ---
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
        filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
        filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
        filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
        filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
        filter_days = filter_timeframe_days[filter_timeframe_option]
        submitted = st.form_submit_button("Filter anwenden")

st.title("Song Tracking Übersicht")

# Anzeige der Top 10 (kumulatives Wachstum)
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
now = pd.Timestamp.now(tz='UTC')
start_2days = now - pd.Timedelta(days=2)
df_2days = df[df["date"] >= start_2days]
cumulative = []
for song_id, group in df_2days.groupby("song_id"):
    group = group.sort_values("date")
    if group.empty:
        continue
    first_pop = group.iloc[0]["popularity"]
    last_pop = group.iloc[-1]["popularity"]
    growth = ((last_pop - first_pop) / first_pop) * 100 if first_pop and first_pop != 0 else 0
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    cumulative.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "last_popularity": last_pop,
        "cumulative_growth": growth
    })
cum_df = pd.DataFrame(cumulative)
if cum_df.empty:
    st.write("Keine Daten für die Top 10 verfügbar.")
    top10 = pd.DataFrame()
else:
    top10 = cum_df.sort_values("cumulative_growth", ascending=False).head(10)
num_columns = 5
rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
for row_df in rows:
    cols = st.columns(num_columns)
    for idx, (_, row) in enumerate(row_df.iterrows()):
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with cols[idx]:
            if cover_url:
                st.image(cover_url, use_container_width=True)
            else:
                st.write("Kein Cover")
            short_title = row["track_name"][:40] + "..." if len(row["track_name"]) > 40 else row["track_name"]
            short_artist = row["artist"][:40] + "..." if len(row["artist"]) > 40 else row["artist"]
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>{short_title}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-style: italic;'>{short_artist}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f}</div>", unsafe_allow_html=True)
            if spotify_link:
                st.markdown(f"<div style='text-align: center;'><a href='{spotify_link}' target='_blank'>Spotify Link</a></div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>Growth: {row['cumulative_growth']:.1f}%</div>", unsafe_allow_html=True)

# Filterergebnisse: Im Expander wird per eingegebenem Code der Graph der Messungen angezeigt.
st.header("Songs filtern")
if submitted:
    last_data = []
    for song_id, group in df.groupby("song_id"):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0.0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            if prev_pop and prev_pop != 0:
                growth_val = ((last_pop - prev_pop) / prev_pop) * 100
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        last_data.append({
            "song_id": song_id,
            "track_name": meta.get("track_name", "Unbekannter Track"),
            "artist": meta.get("artist", "Unbekannt"),
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
                st.markdown(f"[Spotify Link]({spotify_link})")
            with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                code_input = st.text_input("Code für Messungen eingeben", key=f"code_{row['song_id']}")
                if code_input:
                    fig = build_code_graph(code_input)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
                    else:
                        st.write("Keine Messungen für diesen Code gefunden.")
else:
    st.write("Bitte verwenden Sie das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")

# Neue Funktion: Baut einen Graphen, der alle Einträge mit einem bestimmten Code (im Title-Property "Track Code") anzeigt
def build_code_graph(code):
    data = get_weeks_entries()
    df_code = [r for r in data if r["track_code"] == code]
    if not df_code:
        return None
    df = pd.DataFrame(df_code)
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
    df = df.sort_values("date")
    if df["date"].nunique() <= 1:
        fig = px.scatter(df, x="date", y="popularity",
                         title=f"Messungen für Code {code}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(df, x="date", y="popularity",
                      title=f"Messungen für Code {code}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    return fig

@st.cache_data(show_spinner=False)
def get_weeks_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    results = []
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        track_code_prop = props.get("Track Code", {}).get("title", [])
        code_str = track_code_prop[0]["plain_text"] if track_code_prop else ""
        song_rel = props.get("Song", {}).get("relation", [])
        song_id = song_rel[0]["id"] if song_rel else None
        pop_score = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        results.append({
            "page_id": page_id,
            "track_code": code_str,
            "song_id": song_id,
            "popularity": pop_score,
            "date_str": date_str
        })
    return results

# Optional: Funktion für Get New Music
def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    # Hier deinen Code einfügen...
    st.success("Neue Musik wurde hinzugefügt!")

# --- Hauptprogramm ---
st.title("Song Tracking Übersicht")

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
        filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
        filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
        filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
        filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
        filter_days = filter_timeframe_days[filter_timeframe_option]
        submitted = st.form_submit_button("Filter anwenden")

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
now = pd.Timestamp.now(tz='UTC')
start_2days = now - pd.Timedelta(days=2)
df_2days = df[df["date"] >= start_2days]
cumulative = []
for song_id, group in df_2days.groupby("song_id"):
    group = group.sort_values("date")
    if group.empty:
        continue
    first_pop = group.iloc[0]["popularity"]
    last_pop = group.iloc[-1]["popularity"]
    growth = ((last_pop - first_pop) / first_pop) * 100 if first_pop and first_pop != 0 else 0
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    cumulative.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "last_popularity": last_pop,
        "cumulative_growth": growth
    })
cum_df = pd.DataFrame(cumulative)
if cum_df.empty:
    st.write("Keine Daten für die Top 10 verfügbar.")
    top10 = pd.DataFrame()
else:
    top10 = cum_df.sort_values("cumulative_growth", ascending=False).head(10)
num_columns = 5
rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
for row_df in rows:
    cols = st.columns(num_columns)
    for idx, (_, row) in enumerate(row_df.iterrows()):
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with cols[idx]:
            if cover_url:
                st.image(cover_url, use_container_width=True)
            else:
                st.write("Kein Cover")
            short_title = row["track_name"][:40] + "..." if len(row["track_name"]) > 40 else row["track_name"]
            short_artist = row["artist"][:40] + "..." if len(row["artist"]) > 40 else row["artist"]
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>{short_title}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-style: italic;'>{short_artist}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f}</div>", unsafe_allow_html=True)
            if spotify_link:
                st.markdown(f"<div style='text-align: center;'><a href='{spotify_link}' target='_blank'>Spotify Link</a></div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>Growth: {row['cumulative_growth']:.1f}%</div>", unsafe_allow_html=True)

st.header("Songs filtern")
if submitted:
    last_data = []
    for song_id, group in df.groupby("song_id"):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0.0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            if prev_pop and prev_pop != 0:
                growth_val = ((last_pop - prev_pop) / prev_pop) * 100
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        last_data.append({
            "song_id": song_id,
            "track_name": meta.get("track_name", "Unbekannter Track"),
            "artist": meta.get("artist", "Unbekannt"),
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
                st.markdown(f"[Spotify Link]({spotify_link})")
            with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                code_input = st.text_input("Code für Messungen eingeben", key=f"code_{row['song_id']}")
                if code_input:
                    fig = build_code_graph(code_input)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
                    else:
                        st.write("Keine Messungen für diesen Code gefunden.")
else:
    st.write("Bitte verwenden Sie das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")

def build_code_graph(code):
    data = get_weeks_entries()
    df_code = [r for r in data if r["track_code"] == code]
    if not df_code:
        return None
    df = pd.DataFrame(df_code)
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
    df = df.sort_values("date")
    if df["date"].nunique() <= 1:
        fig = px.scatter(df, x="date", y="popularity",
                         title=f"Messungen für Code {code}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(df, x="date", y="popularity",
                      title=f"Messungen für Code {code}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    return fig

@st.cache_data(show_spinner=False)
def get_weeks_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    results = []
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        track_code_prop = props.get("Track Code", {}).get("title", [])
        code_str = track_code_prop[0]["plain_text"] if track_code_prop else ""
        song_rel = props.get("Song", {}).get("relation", [])
        song_id = song_rel[0]["id"] if song_rel else None
        pop_score = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        results.append({
            "page_id": page_id,
            "track_code": code_str,
            "song_id": song_id,
            "popularity": pop_score,
            "date_str": date_str
        })
    return results

# Optional: Get New Music
def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    # Hier deinen Code einfügen...
    st.success("Neue Musik wurde hinzugefügt!")

# --- Hauptprogramm ---
st.title("Song Tracking Übersicht")
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
        filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
        filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
        filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
        filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
        filter_days = filter_timeframe_days[filter_timeframe_option]
        submitted = st.form_submit_button("Filter anwenden")

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
now = pd.Timestamp.now(tz='UTC')
start_2days = now - pd.Timedelta(days=2)
df_2days = df[df["date"] >= start_2days]
cumulative = []
for song_id, group in df_2days.groupby("song_id"):
    group = group.sort_values("date")
    if group.empty:
        continue
    first_pop = group.iloc[0]["popularity"]
    last_pop = group.iloc[-1]["popularity"]
    growth = ((last_pop - first_pop) / first_pop) * 100 if first_pop and first_pop != 0 else 0
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    cumulative.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "last_popularity": last_pop,
        "cumulative_growth": growth
    })
cum_df = pd.DataFrame(cumulative)
if cum_df.empty:
    st.write("Keine Daten für die Top 10 verfügbar.")
    top10 = pd.DataFrame()
else:
    top10 = cum_df.sort_values("cumulative_growth", ascending=False).head(10)
num_columns = 5
rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
for row_df in rows:
    cols = st.columns(num_columns)
    for idx, (_, row) in enumerate(row_df.iterrows()):
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        with cols[idx]:
            if cover_url:
                st.image(cover_url, use_container_width=True)
            else:
                st.write("Kein Cover")
            short_title = row["track_name"][:40] + "..." if len(row["track_name"]) > 40 else row["track_name"]
            short_artist = row["artist"][:40] + "..." if len(row["artist"]) > 40 else row["artist"]
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>{short_title}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-style: italic;'>{short_artist}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f}</div>", unsafe_allow_html=True)
            if spotify_link:
                st.markdown(f"<div style='text-align: center;'><a href='{spotify_link}' target='_blank'>Spotify Link</a></div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align: center; font-weight: bold;'>Growth: {row['cumulative_growth']:.1f}%</div>", unsafe_allow_html=True)

st.header("Songs filtern")
if submitted:
    last_data = []
    for song_id, group in df.groupby("song_id"):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0.0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            if prev_pop and prev_pop != 0:
                growth_val = ((last_pop - prev_pop) / prev_pop) * 100
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        last_data.append({
            "song_id": song_id,
            "track_name": meta.get("track_name", "Unbekannter Track"),
            "artist": meta.get("artist", "Unbekannt"),
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
                st.markdown(f"[Spotify Link]({spotify_link})")
            with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                code_input = st.text_input("Code für Messungen eingeben", key=f"code_{row['song_id']}")
                if code_input:
                    fig = build_code_graph(code_input)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
                    else:
                        st.write("Keine Messungen für diesen Code gefunden.")
else:
    st.write("Bitte verwenden Sie das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")

def build_code_graph(code):
    data = get_weeks_entries()
    df_code = [r for r in data if r["track_code"] == code]
    if not df_code:
        return None
    df = pd.DataFrame(df_code)
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
    df = df.sort_values("date")
    if df["date"].nunique() <= 1:
        fig = px.scatter(df, x="date", y="popularity",
                         title=f"Messungen für Code {code}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(df, x="date", y="popularity",
                      title=f"Messungen für Code {code}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    return fig

@st.cache_data(show_spinner=False)
def get_weeks_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    results = []
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        track_code_prop = props.get("Track Code", {}).get("title", [])
        code_str = track_code_prop[0]["plain_text"] if track_code_prop else ""
        song_rel = props.get("Song", {}).get("relation", [])
        song_id = song_rel[0]["id"] if song_rel else None
        pop_score = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        results.append({
            "page_id": page_id,
            "track_code": code_str,
            "song_id": song_id,
            "popularity": pop_score,
            "date_str": date_str
        })
    return results

def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    # Hier deinen Code einfügen...
    st.success("Neue Musik wurde hinzugefügt!")

if __name__ == "__main__":
    st.title("Song Tracking Übersicht")
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
            filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
            filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
            filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
            filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
            filter_days = filter_timeframe_days[filter_timeframe_option]
            submitted = st.form_submit_button("Filter anwenden")
    
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
    now = pd.Timestamp.now(tz='UTC')
    start_2days = now - pd.Timedelta(days=2)
    df_2days = df[df["date"] >= start_2days]
    cumulative = []
    for song_id, group in df_2days.groupby("song_id"):
        group = group.sort_values("date")
        if group.empty:
            continue
        first_pop = group.iloc[0]["popularity"]
        last_pop = group.iloc[-1]["popularity"]
        growth = ((last_pop - first_pop) / first_pop) * 100 if first_pop and first_pop != 0 else 0
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        cumulative.append({
            "song_id": song_id,
            "track_name": meta["track_name"],
            "artist": meta["artist"],
            "release_date": meta["release_date"],
            "spotify_track_id": meta["spotify_track_id"],
            "last_popularity": last_pop,
            "cumulative_growth": growth
        })
    cum_df = pd.DataFrame(cumulative)
    if cum_df.empty:
        st.write("Keine Daten für die Top 10 verfügbar.")
        top10 = pd.DataFrame()
    else:
        top10 = cum_df.sort_values("cumulative_growth", ascending=False).head(10)
    num_columns = 5
    rows = [top10.iloc[i:i+num_columns] for i in range(0, len(top10), num_columns)]
    for row_df in rows:
        cols = st.columns(num_columns)
        for idx, (_, row) in enumerate(row_df.iterrows()):
            cover_url, spotify_link = ("", "")
            if row["spotify_track_id"]:
                cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
            with cols[idx]:
                if cover_url:
                    st.image(cover_url, use_container_width=True)
                else:
                    st.write("Kein Cover")
                short_title = row["track_name"][:40] + "..." if len(row["track_name"]) > 40 else row["track_name"]
                short_artist = row["artist"][:40] + "..." if len(row["artist"]) > 40 else row["artist"]
                st.markdown(f"<div style='text-align: center; font-weight: bold;'>{short_title}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center; font-style: italic;'>{short_artist}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center;'>Release: {row['release_date']}</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center;'>Popularity: {row['last_popularity']:.1f}</div>", unsafe_allow_html=True)
                if spotify_link:
                    st.markdown(f"<div style='text-align: center;'><a href='{spotify_link}' target='_blank'>Spotify Link</a></div>", unsafe_allow_html=True)
                st.markdown(f"<div style='text-align: center; font-weight: bold;'>Growth: {row['cumulative_growth']:.1f}%</div>", unsafe_allow_html=True)
    
    st.header("Songs filtern")
    if submitted:
        last_data = []
        for song_id, group in df.groupby("song_id"):
            group = group.sort_values("date")
            last_pop = group.iloc[-1]["popularity"]
            growth_val = 0.0
            if len(group) >= 2:
                prev_pop = group.iloc[-2]["popularity"]
                if prev_pop and prev_pop != 0:
                    growth_val = ((last_pop - prev_pop) / prev_pop) * 100
            meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
            last_data.append({
                "song_id": song_id,
                "track_name": meta.get("track_name", "Unbekannter Track"),
                "artist": meta.get("artist", "Unbekannt"),
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
                    st.markdown(f"[Spotify Link]({spotify_link})")
                with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
                    code_input = st.text_input("Code für Messungen eingeben", key=f"code_{row['song_id']}")
                    if code_input:
                        fig = build_code_graph(code_input)
                        if fig is not None:
                            st.plotly_chart(fig, use_container_width=True, key=f"chart_{row['song_id']}")
                        else:
                            st.write("Keine Messungen für diesen Code gefunden.")
    else:
        st.write("Bitte verwenden Sie das Filterformular in der Sidebar, um Ergebnisse anzuzeigen.")

def build_code_graph(code):
    data = get_weeks_entries()
    df_code = [r for r in data if r["track_code"] == code]
    if not df_code:
        return None
    df = pd.DataFrame(df_code)
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
    df = df.sort_values("date")
    if df["date"].nunique() <= 1:
        fig = px.scatter(df, x="date", y="popularity",
                         title=f"Messungen für Code {code}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(df, x="date", y="popularity",
                      title=f"Messungen für Code {code}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    return fig

@st.cache_data(show_spinner=False)
def get_weeks_entries():
    url = f"{notion_query_endpoint}/{weeks_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    results = []
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        track_code_prop = props.get("Track Code", {}).get("title", [])
        code_str = track_code_prop[0]["plain_text"] if track_code_prop else ""
        song_rel = props.get("Song", {}).get("relation", [])
        song_id = song_rel[0]["id"] if song_rel else None
        pop_score = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        results.append({
            "page_id": page_id,
            "track_code": code_str,
            "song_id": song_id,
            "popularity": pop_score,
            "date_str": date_str
        })
    return results
