import streamlit as st
import requests
import pandas as pd
import datetime
import json
import time
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor
import uuid

st.set_page_config(layout="wide")
st.title("Weekly Popularity Graphs - Debug Mode")

# === Notion-Konfiguration für die Weeks-Datenbank ===
week_database_id = "1a9b6204cede80e29338ede2c76999f2"
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_weeks_data():
    """
    Lädt alle Einträge aus der Weeks-Datenbank.
    Extrahiert für jeden Eintrag:
      - Popularity Score,
      - den Zeitstempel aus dem Property "Date"
      - die Notion Track ID (Song-ID) aus der Relation "Song".
    """
    url = f"{notion_query_endpoint}/{week_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        popularity = props.get("Popularity Score", {}).get("number")
        # Verwende das Property "Date" als Zeitstempel
        date_str = props.get("Date", {}).get("date", {}).get("start")
        # Extrahiere die Song-ID aus der Relation "Song"
        song_relations = props.get("Song", {}).get("relation", [])
        song_id = song_relations[0]["id"] if song_relations else None
        entries.append({
            "date": date_str,
            "popularity": popularity,
            "song_id": song_id
        })
    return entries

# Daten laden und DataFrame erstellen
data = get_weeks_data()
df = pd.DataFrame(data)
st.write("Rohdaten aus Notion (unverarbeitete Zeitstempel):", df[["date", "song_id"]].drop_duplicates())

# Datum parsen und nur gültige Einträge behalten
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date", "song_id"])
st.write("Daten nach Datumskonvertierung:", df.head())

# Gruppiere nach Song-ID (jede Song-ID gehört zu einem Song)
grouped = df.groupby("song_id")

# Erstelle für jeden Song ein Diagramm
for song_id, group in grouped:
    group = group.sort_values("date")
    fig = px.line(group, x="date", y="popularity", markers=True,
                  title=f"Song (ID: {song_id})",
                  labels={"date": "Zeit", "popularity": "Popularity Score"})
    fig.update_yaxes(range=[0, 100])
    with st.expander(f"Graph für Song ID: {song_id}"):
        # Der dynamische Schlüssel sorgt für Neuladen beim Aufklappen
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{song_id}_{time.time()}")
