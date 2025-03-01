import streamlit as st
import requests
import pandas as pd
import datetime
import plotly.express as px
import time

st.set_page_config(layout="wide")
st.title("Debug: Alle Messwerte für 'Erfolg ist kein Glück'")

# === Notion-Konfiguration für die Weeks-Datenbank ===
week_database_id = "1a9b6204cede80e29338ede2c76999f2"
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_all_weeks_data():
    """Abfrage aller Seiten (Pagination) aus der Weeks-Datenbank."""
    url = f"{notion_query_endpoint}/{week_database_id}/query"
    payload = {}
    entries = []
    has_more = True
    start_cursor = None
    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=notion_headers, json=payload)
        response.raise_for_status()
        data = response.json()
        entries.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
    return entries

def parse_weeks_entries():
    """Extrahiere aus allen Seiten die relevanten Felder."""
    pages = get_all_weeks_data()
    data = []
    for page in pages:
        props = page.get("properties", {})
        popularity = props.get("Popularity Score", {}).get("number")
        # Nutze das Property "Date" als Zeitstempel
        date_str = props.get("Date", {}).get("date", {}).get("start")
        # Notion Track ID aus dem Property "Notion Track ID"
        track_id = ""
        if "Notion Track ID" in props:
            rich_text = props["Notion Track ID"].get("rich_text", [])
            track_id = "".join(rt.get("plain_text", "") for rt in rich_text).strip()
        data.append({
            "date": date_str,
            "popularity": popularity,
            "notion_track_id": track_id
        })
    return data

# Daten abrufen und in einen DataFrame umwandeln
all_data = parse_weeks_entries()
df = pd.DataFrame(all_data)
st.write("**Rohdaten (alle Einträge):**", df)

# Datum parsen
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date", "notion_track_id"])
st.write("**Nach Datumskonvertierung:**", df)

# Filtere für den Song "Erfolg ist kein Glück" (Notion Track ID: 7ocanzdHHuxnYCHgS40CPF)
df_filtered = df[df["notion_track_id"] == "7ocanzdHHuxnYCHgS40CPF"]
st.write("**Alle Messwerte für 'Erfolg ist kein Glück':**", df_filtered)

# Erstelle einen Graphen, der alle Messwerte chronologisch anzeigt
df_filtered = df_filtered.sort_values("date")
fig = px.line(df_filtered, x="date", y="popularity", markers=True,
              title="Erfolg ist kein Glück – Popularity über die Zeit",
              labels={"date": "Zeit", "popularity": "Popularity Score"})
fig.update_yaxes(range=[0, 100])
st.plotly_chart(fig, use_container_width=True)
