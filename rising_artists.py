import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px

# === Notion Konfiguration ===
tracking_db_id = "1a9b6204cede80e29338ede2c76999f2"  # Tracking-Datenbank (enthält Rollups für Artist und Release Date, Relation Song)
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === Spotify Konfiguration ===
def get_spotify_token():
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("accessToken")

SPOTIFY_TOKEN = get_spotify_token()

# Hilfsfunktion: Extrahiere Text bzw. Datum aus einem Rollup-Feld
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

# Hole den Track Name von der verknüpften Seite (Songs-Datenbank)
def get_track_name_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track Name" in page["properties"]:
            title_prop = page["properties"]["Track Name"].get("title", [])
            return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

# Hole den Spotify Track ID aus der verknüpften Seite (angenommen in Property "Track ID" als Rich Text)
def get_track_id_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track ID" in page["properties"]:
            text_prop = page["properties"]["Track ID"].get("rich_text", [])
            return "".join([t.get("plain_text", "") for t in text_prop]).strip()
    return ""

# Hole Metadaten aus der Tracking-Datenbank.
# Annahme: In der Tracking-DB sind folgende Properties vorhanden:
# - "Song": Relation (verweist auf die Song-Seite in der Songs-DB)
# - "Artist": Rollup (enthält den Artist-Namen als Text)
# - "Release Date": Rollup (enthält das Release Date)
def get_metadata_from_tracking_db():
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    metadata = {}
    for page in data.get("results", []):
        props = page.get("properties", {})
        song_relations = props.get("Song", {}).get("relation", [])
        if song_relations:
            related_page_id = song_relations[0].get("id")
            track_name = get_track_name_from_page(related_page_id)
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

# Hole alle Tracking-Einträge (Messungen) aus der Tracking-Datenbank
def get_tracking_entries():
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        pop = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        song_relations = props.get("Song", {}).get("relation", [])
        for relation in song_relations:
            song_id = relation.get("id")
            entries.append({"song_id": song_id, "date": date_str, "popularity": pop})
    return entries

# Hole Spotify-Daten (Cover und Spotify Link) für einen gegebenen Track
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

# === Streamlit App ===

st.title("Song Tracking Übersicht")

# Sidebar: Filter & Sort Optionen
st.sidebar.header("Filter & Sort")
pop_range = st.sidebar.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1)
growth_threshold = st.sidebar.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5)
sort_option = st.sidebar.selectbox("Sortiere nach", ["Popularity", "Release Date"])
timeframe_option = st.sidebar.selectbox("Zeitraum für Graphen", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"])
timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
days = timeframe_days[timeframe_option]

st.write("Lade Tracking-Daten aus Notion...")
tracking_entries = get_tracking_entries()
metadata = get_metadata_from_tracking_db()

df = pd.DataFrame(tracking_entries)
if df.empty:
    st.write("Keine Tracking-Daten gefunden.")
    st.stop()

df["date"] = pd.to_datetime(df["date"], errors="coerce")
# Ergänze die Metadaten anhand der Song-Relation (Schlüssel: Song-ID)
df["track_name"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("track_name", "Unbekannter Track"))
df["artist"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("artist", "Unbekannt"))
df["release_date"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("release_date", ""))
df["spotify_track_id"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("spotify_track_id", ""))

# Berechne pro Song den letzten Messwert und den Growth zwischen den letzten beiden Messungen
last_data = []
for song_id, group in df.groupby("song_id"):
    group = group.sort_values("date")
    last_pop = group.iloc[-1]["popularity"]
    growth = 0.0
    if len(group) >= 2:
        prev_pop = group.iloc[-2]["popularity"]
        if prev_pop != 0:
            growth = ((last_pop - prev_pop) / prev_pop) * 100
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    last_data.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "last_popularity": last_pop,
        "growth": growth
    })
last_df = pd.DataFrame(last_data)

# Filtere nach Popularity Range und Growth
filtered_df = last_df[
    (last_df["last_popularity"] >= pop_range[0]) &
    (last_df["last_popularity"] <= pop_range[1]) &
    (last_df["growth"] >= growth_threshold)
]

# Sortiere die Tabelle
if sort_option == "Popularity":
    filtered_df = filtered_df.sort_values("last_popularity", ascending=False)
elif sort_option == "Release Date":
    filtered_df["release_date_dt"] = pd.to_datetime(filtered_df["release_date"], errors="coerce")
    filtered_df = filtered_df.sort_values("release_date_dt", ascending=True)

st.write("Gefilterte Songs:")
for idx, row in filtered_df.iterrows():
    # Hole Spotify-Daten: Cover und Link (sofern spotify_track_id vorhanden)
    cover_url, spotify_link = ("", "")
    if row["spotify_track_id"]:
        cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
    with st.container():
        # Anzeige der Song-Informationen als Liste
        st.markdown(f"**{row['track_name']}** – {row['artist']}  
Release Date: {row['release_date']}  
Popularity: {row['last_popularity']:.1f} | Growth: {row['growth']:.1f}%")
        if cover_url:
            st.image(cover_url, width=100)
        if spotify_link:
            st.markdown(f"[Spotify Link]({spotify_link})")
        # Expander für den Graphen der Tracking-History
        with st.expander("Tracking-History anzeigen"):
            # Zeitraum-Filter: Berechne start_time als timezone-aware Timestamp in UTC
            now = pd.Timestamp.now(tz='UTC')
            start_time = now - pd.Timedelta(days=days)
            song_history = df[(df["song_id"] == row["song_id"]) & (df["date"] >= start_time)].sort_values("date")
            if song_history.empty:
                st.write("Keine Tracking-Daten im gewählten Zeitraum.")
            else:
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
