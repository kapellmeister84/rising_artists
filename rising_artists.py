import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px

# === Notion Konfiguration ===
songs_database_id = "b94c8042619d42a3be799c9795984150"  # Songs-Datenbank
week_database_id = "1a9b6204cede80e29338ede2c76999f2"    # Week-Tracking-Datenbank
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === Funktionen zum Laden der Daten aus Notion ===

# Lade alle Week-Tracking-Einträge (Tracking-Messungen)
def get_week_entries():
    url = f"{notion_query_endpoint}/{week_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
        props = page["properties"]
        pop = props.get("Popularity Score", {}).get("number")
        date_str = props.get("Date", {}).get("date", {}).get("start")
        # Song-Relation: jeder Eintrag kann mit mehreren Songs verknüpft sein
        song_relations = props.get("Song", {}).get("relation", [])
        for relation in song_relations:
            song_id = relation["id"]
            entries.append({"song_id": song_id, "date": date_str, "popularity": pop})
    return entries

# Lade Song-Metadaten (Song Title, Artist Name, Release Date) aus der Songs-Datenbank
def get_song_metadata():
    url = f"{notion_query_endpoint}/{songs_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    metadata = {}
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        # Song Title
        title_prop = props.get("Song Title", {}).get("title", [])
        song_title = title_prop[0]["plain_text"].strip() if title_prop else "Unbekannter Song"
        # Artist Name
        artist = "Unbekannt"
        if "Artist Name" in props and props["Artist Name"]["rich_text"]:
            artist = props["Artist Name"]["rich_text"][0]["plain_text"].strip()
        # Release Date
        release_date = ""
        if "Release Date" in props and props["Release Date"].get("date"):
            release_date = props["Release Date"]["date"].get("start", "")
        metadata[page_id] = {"song_title": song_title, "artist": artist, "release_date": release_date}
    return metadata

# === Main App ===

st.title("Song Tracking Graphen")

# Sidebar: Filter- und Sortieroptionen
st.sidebar.header("Filter & Sort")
pop_range = st.sidebar.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1)
growth_threshold = st.sidebar.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", value=0.0, step=0.5)
sort_option = st.sidebar.selectbox("Sortiere nach", ["Popularity", "Release Date"])

st.write("Lade Tracking-Daten aus Notion...")
week_entries = get_week_entries()
song_metadata = get_song_metadata()

df = pd.DataFrame(week_entries)
if df.empty:
    st.write("Keine Tracking-Daten gefunden.")
    st.stop()

# Datum konvertieren
df["date"] = pd.to_datetime(df["date"])
# Füge Song-Metadaten hinzu
df["song_title"] = df["song_id"].map(lambda x: song_metadata.get(x, {}).get("song_title", ""))
df["artist"] = df["song_id"].map(lambda x: song_metadata.get(x, {}).get("artist", "Unbekannt"))
df["release_date"] = df["song_id"].map(lambda x: song_metadata.get(x, {}).get("release_date", ""))

# Berechne pro Song:
# - Den letzten Popularity-Wert (als Indikator)
# - Den prozentualen Growth zwischen den letzten beiden Messungen (sofern vorhanden)
last_data = []
for song_id, group in df.groupby("song_id"):
    group = group.sort_values("date")
    last_pop = group.iloc[-1]["popularity"]
    growth = 0.0
    if len(group) >= 2:
        prev_pop = group.iloc[-2]["popularity"]
        if prev_pop != 0:
            growth = ((last_pop - prev_pop) / prev_pop) * 100
    meta = song_metadata.get(song_id, {"song_title": "Unbekannt", "artist": "Unbekannt", "release_date": ""})
    last_data.append({
        "song_id": song_id,
        "song_title": meta["song_title"],
        "artist": meta["artist"],
        "release_date": meta["release_date"],
        "last_popularity": last_pop,
        "growth": growth
    })
last_df = pd.DataFrame(last_data)

# Filter anwenden:
filtered_df = last_df[
    (last_df["last_popularity"] >= pop_range[0]) &
    (last_df["last_popularity"] <= pop_range[1]) &
    (last_df["growth"] >= growth_threshold)
]

# Sortierung anwenden:
if sort_option == "Popularity":
    filtered_df = filtered_df.sort_values("last_popularity", ascending=False)
elif sort_option == "Release Date":
    filtered_df["release_date_dt"] = pd.to_datetime(filtered_df["release_date"], errors="coerce")
    filtered_df = filtered_df.sort_values("release_date_dt", ascending=True)

st.write("Gefilterte Songs:")
st.dataframe(filtered_df)

st.write("Graphen der Tracking-History (alle Songs):")
# Zeige für jeden Song (egal ob gefiltert oder nicht) den Graphen – du kannst hier auch nur die gefilterten Songs anzeigen.
for idx, row in filtered_df.iterrows():
    song_id = row["song_id"]
    song_history = df[df["song_id"] == song_id].sort_values("date")
    fig = px.line(song_history, x="date", y="popularity",
                  title=f"{row['song_title']} - {row['artist']}",
                  labels={"date": "Datum", "popularity": "Popularity Score"})
    st.plotly_chart(fig)
