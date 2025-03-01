import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px

# === Notion Konfiguration ===
# Tracking-Datenbank (Weeks-Datenbank) – enthält:
# • "Song" (Relation zur Songs‑/Track‑Datenbank)
# • "Artist" (Rollup)
# • "Release Date" (Rollup)
# • "Popularity Score" (Number) und "Date" (Date) als Messdaten
# • "Growth" (Zahl) soll aktualisiert werden
tracking_db_id = "1a9b6204cede80e29338ede2c76999f2"
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

# Funktion: Hole den Track Name von der verknüpften Seite (Songs-Datenbank)
def get_track_name_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track Name" in page["properties"]:
            title_prop = page["properties"]["Track Name"].get("title", [])
            return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

# Funktion: Hole den Spotify Track ID von der verknüpften Seite (Songs-Datenbank)
def get_track_id_from_page(page_id):
    url = f"{notion_page_endpoint}/{page_id}"
    response = requests.get(url, headers=notion_headers)
    if response.status_code == 200:
        page = response.json()
        if "properties" in page and "Track ID" in page["properties"]:
            text_prop = page["properties"]["Track ID"].get("rich_text", [])
            return "".join([t.get("plain_text", "") for t in text_prop]).strip()
    return ""

# Funktion: Aktualisiere den Growth-Wert einer Messung (in der Tracking-/Weeks-Datenbank)
def update_growth_for_measurement(entry_id, growth):
    url = f"{notion_page_endpoint}/{entry_id}"
    data = {"properties": {"Growth": {"number": growth}}}
    response = requests.patch(url, headers=notion_headers, data=json.dumps(data))
    response.raise_for_status()

# Funktion: Hole alle Tracking-Einträge (Messungen) aus der Tracking-Datenbank
def get_tracking_entries():
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
            entries.append({
                "entry_id": entry_id,
                "song_id": song_id,
                "date": date_str,
                "popularity": pop
            })
    return entries

# Funktion: Hole Spotify-Daten (Cover und Spotify Link) für einen gegebenen Track
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

# Funktion: Hole Metadaten (Track Name, Artist, Release Date, Spotify Track ID)
# Diese werden aus der Tracking-Datenbank-Rollups abgerufen:
# - "Song" (Relation): über den ersten Eintrag holen wir Track Name und Spotify Track ID aus der verknüpften Seite
# - "Artist" (Rollup) und "Release Date" (Rollup) werden per parse_rollup_text verarbeitet
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

# === Streamlit App ===

st.title("Song Tracking Übersicht")

# Zuerst: Top 5 Songs mit dem größten kumulativen Wachstum (von erster bis letzter Messung)
st.header("Top 5 Songs mit größtem Wachstum")
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

# Für jede Gruppe (Song) berechnen wir das kumulative Wachstum: ((letzter - erster)/erster)*100
cumulative = []
for song_id, group in df.groupby("song_id"):
    group = group.sort_values("date")
    first_pop = group.iloc[0]["popularity"]
    last_pop = group.iloc[-1]["popularity"]
    if first_pop and first_pop != 0:
        cum_growth = ((last_pop - first_pop) / first_pop) * 100
    else:
        cum_growth = 0
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": "", "spotify_track_id": ""})
    cumulative.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "spotify_track_id": meta["spotify_track_id"],
        "cumulative_growth": cum_growth
    })
cum_df = pd.DataFrame(cumulative)
top5 = cum_df.sort_values("cumulative_growth", ascending=False).head(5)

cols = st.columns(5)
for idx, row in top5.iterrows():
    with cols[idx % 5]:
        cover_url, spotify_link = ("", "")
        if row["spotify_track_id"]:
            cover_url, spotify_link = get_spotify_data(row["spotify_track_id"])
        st.image(cover_url, use_column_width=True) if cover_url else st.write("Kein Cover")
        st.markdown(f"**{row['track_name']}**")
        st.markdown(f"*{row['artist']}*")
        st.markdown(f"Release: {row['release_date']}")
        if spotify_link:
            st.markdown(f"[Spotify Link]({spotify_link})")
        st.markdown(f"**Growth:** {row['cumulative_growth']:.1f}%")

# Jetzt: Alle Songs mit Filter-/Sortieroptionen und Dropdown für den Graphen
st.header("Alle Songs")
st.sidebar.header("Filter & Sort")
pop_range = st.sidebar.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1)
growth_threshold = st.sidebar.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5)
sort_option = st.sidebar.selectbox("Sortiere nach", ["Popularity", "Release Date"])
timeframe_option = st.sidebar.selectbox("Zeitraum für Graphen (Ende)", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"])
timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
days = timeframe_days[timeframe_option]

# Für den Graph verwenden wir alle Messungen, d.h. der Graph beginnt immer mit der ersten Messung.
# Berechne pro Song den letzten Messwert und Growth zwischen den letzten beiden Messungen (wie oben)
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

# Filtere nach Popularity Range und Growth (letzter Messwert und Growth zwischen den letzten beiden Messungen)
filtered_df = last_df[
    (last_df["last_popularity"] >= pop_range[0]) &
    (last_df["last_popularity"] <= pop_range[1]) &
    (last_df["growth"] >= growth_threshold)
]

if sort_option == "Popularity":
    filtered_df = filtered_df.sort_values("last_popularity", ascending=False)
elif sort_option == "Release Date":
    filtered_df["release_date_dt"] = pd.to_datetime(filtered_df["release_date"], errors="coerce")
    filtered_df = filtered_df.sort_values("release_date_dt", ascending=True)

st.write("Gefilterte Songs:")
st.dataframe(filtered_df[["track_name", "artist", "last_popularity", "release_date", "growth"]])

# Hinweis: Zusätzlich aktualisieren wir jede Messung in der Tracking-Datenbank mit ihrem individuellen Growth-Wert.
for song_id, group in df.groupby("song_id"):
    group = group.sort_values("date")
    prev_pop = None
    for idx, row in group.iterrows():
        if prev_pop is None:
            # Erste Messung: kein Growth
            growth_val = 0
        else:
            growth_val = ((row["popularity"] - prev_pop) / prev_pop) * 100 if prev_pop != 0 else 0
            # Aktualisiere die Seite in der Tracking-Datenbank mit diesem Growth-Wert
            update_growth_for_measurement(row["entry_id"], growth_val)
        # Speichere den Growth in der DataFrame (optional)
        df.loc[idx, "growth"] = growth_val
        prev_pop = row["popularity"]

# Anzeige der Graphen – der Graph beginnt immer mit der allerersten Messung
st.write("Graphen der Tracking-History (Beginn: erste Messung) für gefilterte Songs:")
for idx, row in filtered_df.iterrows():
    song_id = row["song_id"]
    song_history = df[df["song_id"] == song_id].sort_values("date")
    if song_history.empty:
        st.write(f"Keine Tracking-Daten für {row['track_name']}.")
        continue
    with st.expander(f"{row['track_name']} - {row['artist']} anzeigen"):
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
