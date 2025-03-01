import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor

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

@st.cache_data(show_spinner=False)
def get_track_name_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track Name" in page["properties"]:
            title_prop = page["properties"]["Track Name"].get("title", [])
            return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

@st.cache_data(show_spinner=False)
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
    data = {"properties": {"Growth": {"number": growth}}}
    response = requests.patch(url, headers=notion_headers, data=json.dumps(data))
    response.raise_for_status()

def get_tracking_entries():
    """Popularity muss aktuell sein, daher kein Caching."""
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
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
    """Artist, Release Date, Track ID etc. (gecacht)."""
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    metadata = {}
    with ThreadPoolExecutor() as executor:
        futures = {}
        for page in data.get("results", []):
            props = page.get("properties", {})
            song_relations = props.get("Song", {}).get("relation", [])
            if song_relations:
                related_page_id = song_relations[0].get("id")
                futures[related_page_id] = executor.submit(get_track_name_from_page, related_page_id)
        track_names = {key: future.result() for key, future in futures.items()}
    for page in data.get("results", []):
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

# === Platzhalterfunktionen für Buttons ===
def get_new_music():
    st.write("Rufe neue Musik aus Playlisten ab...")
    # Hier deinen Code einfügen
    st.success("Neue Musik hinzugefügt!")

def update_popularity():
    st.write("Füge neue Popularity-Messung hinzu...")
    # Hier deinen Code einfügen
    st.success("Popularity aktualisiert!")

# --- Sidebar: Buttons + Filter
st.sidebar.markdown("## Automatische Updates")
if st.sidebar.button("Get New Music"):
    get_new_music()
if st.sidebar.button("Update Popularity"):
    update_popularity()

st.title("Song Tracking Übersicht")

# 1. Oben: Top 10 Songs mit größtem kumulativem Wachstum über 2 Tage
st.header("Top 10 Songs – Wachstum über 2 Tage")

tracking_entries = get_tracking_entries()
metadata = get_metadata_from_tracking_db()

df = pd.DataFrame(tracking_entries)
if df.empty:
    st.write("Keine Tracking-Daten gefunden.")
    st.stop()

df["date"] = pd.to_datetime(df["date"], errors="coerce")
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
top10 = cum_df.sort_values("cumulative_growth", ascending=False).head(10)

# Feste 5-Spalten-Galerie via HTML
css_code = """
<style>
.song-grid {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 1rem;
}
.song-card {
  text-align: center;
  border: 1px solid #ddd;
  border-radius: 5px;
  padding: 0.5rem;
  height: 320px;
  box-sizing: border-box;
}
.song-card img {
  width: 100%;
  height: auto;
  max-height: 200px;
  object-fit: cover;
}
.song-info {
  margin-top: 0.5rem;
  font-size: 0.9rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
"""
st.markdown(css_code, unsafe_allow_html=True)

cards_html = ["<div class='song-grid'>"]
for _, row in top10.iterrows():
    cover_url, spotify_link = "", ""
    if row["spotify_track_id"]:
        cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])

    # Kürze Titel und Artist
    short_title = row["track_name"][:40] + "..." if len(row["track_name"]) > 40 else row["track_name"]
    short_artist = row["artist"][:40] + "..." if len(row["artist"]) > 40 else row["artist"]

    card_html = f"""
    <div class="song-card">
      <img src="{cover_url}" alt="Cover">
      <div class="song-info">
        <strong>{short_title}</strong><br>
        <em>{short_artist}</em><br>
        Release: {row['release_date']}<br>
        Popularity: {row['last_popularity']:.1f}<br>
        Growth: {row['cumulative_growth']:.1f}%<br>
    """
    if spotify_link:
        card_html += f'<a href="{spotify_link}" target="_blank">Spotify Link</a>'
    card_html += """
      </div>
    </div>
    """
    cards_html.append(card_html)
cards_html.append("</div>")
st.markdown("".join(cards_html), unsafe_allow_html=True)

# 2. Unterhalb: Ergebnisse erst anzeigen, wenn Filter gesetzt wurden
st.header("Songs filtern")

with st.sidebar.form("filter_form"):
    search_query = st.text_input("Song/Artist Suche", "")
    filter_pop_range = st.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1, key="filter_pop")
    filter_growth_threshold = st.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5, key="filter_growth")
    filter_sort_option = st.selectbox("Sortiere nach", ["Popularity", "Release Date"], key="filter_sort")
    filter_timeframe_option = st.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"], key="filter_timeframe")
    filter_timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
    filter_days = filter_timeframe_days[filter_timeframe_option]
    submitted = st.form_submit_button("Filter anwenden")

if submitted:
    last_data = []
    for song_id, group in df.groupby("song_id"):
        group = group.sort_values("date")
        last_pop = group.iloc[-1]["popularity"]
        growth_val = 0.0
        if len(group) >= 2:
            prev_pop = group.iloc[-2]["popularity"]
            if prev_pop != 0:
                growth_val = ((last_pop - prev_pop) / prev_pop) * 100
        meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
        last_data.append({
            "song_id": song_id,
            "track_name": meta["track_name"],
            "artist": meta["artist"],
            "release_date": meta["release_date"],
            "spotify_track_id": meta["spotify_track_id"],
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
                song_history = df[df["song_id"] == row["song_id"]].sort_values("date")
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
