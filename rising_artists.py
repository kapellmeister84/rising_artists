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

# Lade alle Week-Tracking-Einträge (Messungen)
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
        # Hole die Song-Relation (Liste von Objekten mit "id")
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
        # Song Title: Alle Textfragmente zusammenfügen
        title_prop = props.get("Song Title", {}).get("title", [])
        song_title = "".join([t.get("plain_text", "") for t in title_prop]).strip() if title_prop else "Unbekannter Song"
        # Artist Name: Alle Textfragmente zusammenfügen
        artist_prop = props.get("Artist Name", {}).get("rich_text", [])
        artist = "".join([t.get("plain_text", "") for t in artist_prop]).strip() if artist_prop else "Unbekannt"
        # Release Date
        release_date = ""
        if "Release Date" in props and props["Release Date"].get("date"):
            release_date = props["Release Date"]["date"].get("start", "")
        metadata[page_id] = {"song_title": song_title, "artist": artist, "release_date": release_date}
    return metadata

# === Streamlit App ===

st.title("Song Tracking Graphen")

# Sidebar: Filter- und Sortieroptionen
st.sidebar.header("Filter & Sort")
pop_range = st.sidebar.slider("Popularity Range (letzter Messwert)", 0, 100, (0, 100), step=1)
growth_threshold = st.sidebar.number_input("Min. Growth % (zwischen den letzten beiden Messungen)", min_value=0.0, value=0.0, step=0.5)
sort_option = st.sidebar.selectbox("Sortiere nach", ["Popularity", "Release Date"])
timeframe_option = st.sidebar.selectbox("Zeitraum für Graphen", ["3 Tage", "1 Woche", "2 Wochen", "3 Wochen"])
# Mapping Zeitraum -> Tage
timeframe_days = {"3 Tage": 3, "1 Woche": 7, "2 Wochen": 14, "3 Wochen": 21}
days = timeframe_days[timeframe_option]

st.write("Lade Tracking-Daten aus Notion...")
week_entries = get_week_entries()
song_metadata = get_song_metadata()

df = pd.DataFrame(week_entries)
if df.empty:
    st.write("Keine Tracking-Daten gefunden.")
    st.stop()

# Konvertiere das Datum in datetime (mit Fehlerbehandlung)
df["date"] = pd.to_datetime(df["date"], errors="coerce")
# Füge Song-Metadaten hinzu (tatsächliche Namen aus der Songs-Datenbank)
df["song_title"] = df["song_id"].map(lambda x: song_metadata.get(x, {}).get("song_title", ""))
df["artist"] = df["song_id"].map(lambda x: song_metadata.get(x, {}).get("artist", "Unbekannt"))
df["release_date"] = df["song_id"].map(lambda x: song_metadata.get(x, {}).get("release_date", ""))

# Berechne für jeden Song den letzten Messwert und den Growth zwischen den letzten beiden Messungen (über alle Messungen)
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

# Filtere Tabelle nach Popularity Range und Growth
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
st.dataframe(filtered_df[["song_title", "artist", "last_popularity", "release_date", "growth"]])

# Zeitraum-Filter: Berechne start_time als timezone-aware Timestamp in UTC
now = pd.Timestamp.now(tz='UTC')
start_time = now - pd.Timedelta(days=days)

st.write(f"Graphen der Tracking-History (Zeitraum: Letzte {timeframe_option}) für gefilterte Songs:")
# Für jeden gefilterten Song
for idx, row in filtered_df.iterrows():
    song_id = row["song_id"]
    # Filtere die Tracking-History nach dem gewählten Zeitraum
    song_history = df[(df["song_id"] == song_id) & (df["date"] >= start_time)].sort_values("date")
    if song_history.empty:
        st.write(f"Keine Tracking-Daten für {row['song_title']} im gewählten Zeitraum.")
        continue
    # Wenn nur ein Messwert vorhanden ist, als Scatter-Plot darstellen
    if len(song_history) == 1:
        fig = px.scatter(song_history, x="date", y="popularity",
                         title=f"{row['song_title']} - {row['artist']}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(song_history, x="date", y="popularity",
                      title=f"{row['song_title']} - {row['artist']}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    st.plotly_chart(fig, use_container_width=True, key=f"chart_{song_id}")
