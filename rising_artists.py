import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px

# === Notion Konfiguration ===
# Diese Datenbank (ID: 1a9b6204cede80e29338ede2c76999f2) enthält:
# • "Song" (Relation) – Verweist auf den Song in der Songs-Datenbank
# • "Artist" (Rollup) – Enthält den Artist-Namen
# • "Release Date" (Rollup) – Enthält das Release Date
# • "Popularity Score" (Number) und "Date" (Date) – Tracking-Daten
tracking_db_id = "1a9b6204cede80e29338ede2c76999f2"
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- Helper-Funktionen ---

# Um aus einer Rollup-Property den zusammengefügten Text zu extrahieren.
def parse_rollup_text(rollup):
    if rollup and "array" in rollup:
        return "".join([item.get("plain_text", "") for item in rollup["array"]]).strip()
    return ""

# Um den tatsächlichen Song/Track-Namen zu erhalten, rufen wir die Seite ab, auf die die Relation zeigt.
# Wir gehen davon aus, dass in der verknüpften Seite die Title-Property "Track Name" heißt.
def get_track_name_from_page(page_id):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    response = requests.get(url, headers=notion_headers)
    response.raise_for_status()
    page = response.json()
    if "properties" in page and "Track Name" in page["properties"]:
        title_prop = page["properties"]["Track Name"].get("title", [])
        return "".join([t.get("plain_text", "") for t in title_prop]).strip()
    return "Unbekannter Track"

# Hole die Metadaten aus der Tracking-Datenbank.
# Für jede Seite in der DB:
# • Aus "Song" (Relation) holen wir den verknüpften Song – wir nutzen den ersten Eintrag, um den tatsächlichen Track-Namen zu ermitteln.
# • Aus dem Rollup "Artist" und "Release Date" extrahieren wir die Texte.
def get_metadata_from_tracking_db():
    url = f"{notion_query_endpoint}/{tracking_db_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    metadata = {}
    for page in data.get("results", []):
        page_id = page.get("id")
        props = page.get("properties", {})
        # Song (Relation): Verwende den ersten verknüpften Song, um den Track-Namen abzurufen.
        song_relations = props.get("Song", {}).get("relation", [])
        if song_relations:
            related_page_id = song_relations[0].get("id")
            song_name = get_track_name_from_page(related_page_id)
        else:
            song_name = "Unbekannter Track"
        # Artist (Rollup)
        artist_rollup = props.get("Artist", {}).get("rollup", {})
        artist = parse_rollup_text(artist_rollup)
        # Release Date (Rollup)
        release_rollup = props.get("Release Date", {}).get("rollup", {})
        release_date = parse_rollup_text(release_rollup)
        # Speichere die Metadaten unter der ID der Tracking-Seite (die für einen Song evtl. mehrfach vorhanden ist)
        # Da die Tracking-Daten pro Song über mehrere Einträge verteilt sein können, nutzen wir die Relation-ID als Schlüssel.
        # Hier verwenden wir den ersten Eintrag der Relation als Schlüssel.
        key = song_relations[0].get("id") if song_relations else page_id
        metadata[key] = {"track_name": song_name, "artist": artist, "release_date": release_date}
    st.write("Abgerufene Metadaten aus der Tracking-DB:", metadata)
    return metadata

# Lade alle Tracking-Einträge (Messungen) aus der Tracking-Datenbank.
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

# === Streamlit App ===

st.title("Song Tracking Graphen")

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

# Konvertiere 'date' in datetime
df["date"] = pd.to_datetime(df["date"], errors="coerce")
# Ergänze die Metadaten (verwende als Schlüssel die Song-ID aus der Relation)
df["track_name"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("track_name", "Unbekannter Track"))
df["artist"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("artist", "Unbekannt"))
df["release_date"] = df["song_id"].map(lambda x: metadata.get(x, {}).get("release_date", ""))

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
    meta = metadata.get(song_id, {"track_name": "Unbekannter Track", "artist": "Unbekannt", "release_date": ""})
    last_data.append({
        "song_id": song_id,
        "track_name": meta["track_name"],
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
st.dataframe(filtered_df[["track_name", "artist", "last_popularity", "release_date", "growth"]])

# Zeitraum-Filter: Berechne start_time als timezone-aware Timestamp in UTC
now = pd.Timestamp.now(tz='UTC')
start_time = now - pd.Timedelta(days=days)

st.write(f"Graphen der Tracking-History (Zeitraum: Letzte {timeframe_option}) für gefilterte Songs:")
for idx, row in filtered_df.iterrows():
    song_id = row["song_id"]
    song_history = df[(df["song_id"] == song_id) & (df["date"] >= start_time)].sort_values("date")
    if song_history.empty:
        st.write(f"Keine Tracking-Daten für {row['track_name']} im gewählten Zeitraum.")
        continue
    if len(song_history) == 1:
        fig = px.scatter(song_history, x="date", y="popularity",
                         title=f"{row['track_name']} - {row['artist']}",
                         labels={"date": "Datum", "popularity": "Popularity Score"})
    else:
        fig = px.line(song_history, x="date", y="popularity",
                      title=f"{row['track_name']} - {row['artist']}",
                      labels={"date": "Datum", "popularity": "Popularity Score"},
                      markers=True)
    st.plotly_chart(fig, use_container_width=True, key=f"chart_{song_id}")
