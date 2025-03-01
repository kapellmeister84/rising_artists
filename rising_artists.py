import streamlit as st
import requests
import pandas as pd
import datetime
import plotly.express as px

st.set_page_config(layout="wide")
st.title("Weekly Popularity Graphs")

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
    """Lädt alle Einträge aus der Weeks-Datenbank und extrahiert die benötigten Felder."""
    url = f"{notion_query_endpoint}/{week_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        # Popularity Score als Zahl
        popularity = props.get("Popularity Score", {}).get("number")
        # Zeitstempel aus dem Property "Date"
        date_str = props.get("Date", {}).get("date", {}).get("start")
        # Notion Track ID aus dem Property "Notion Track ID"
        notion_track_id = ""
        if "Notion Track ID" in props:
            rich_text = props["Notion Track ID"].get("rich_text", [])
            notion_track_id = "".join([rt.get("plain_text", "") for rt in rich_text]).strip()
        entries.append({
            "date": date_str,
            "popularity": popularity,
            "notion_track_id": notion_track_id
        })
    return entries

# Daten laden und DataFrame erstellen
data = get_weeks_data()
df = pd.DataFrame(data)
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])

# Gruppieren nach Notion Track ID (also pro Song)
grouped = df.groupby("notion_track_id")

# Für jeden Song ein Diagramm erstellen
for track_id, group in grouped:
    group = group.sort_values("date")
    # Erstelle den Graphen (Liniendiagramm mit Markern) mit fester y-Achse 0 bis 100
    fig = px.line(group, x="date", y="popularity", markers=True, 
                  title=f"Song (Notion Track ID: {track_id})", 
                  labels={"date": "Zeit", "popularity": "Popularity Score"})
    fig.update_yaxes(range=[0, 100])
    # Jeder Graph wird in einem Expander dargestellt, damit du sie einzeln aufklappen kannst
    with st.expander(f"Graph für Song mit Track ID: {track_id}"):
        st.plotly_chart(fig, use_container_width=True)
