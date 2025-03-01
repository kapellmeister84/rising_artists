import streamlit as st
import requests
import pandas as pd
import datetime

st.set_page_config(layout="wide")
st.title("Messwerte für 'Erfolg ist kein Glück'")

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
    Extrahiert:
      - den Popularity Score,
      - den Zeitstempel aus dem Property "Date",
      - die Notion Track ID (welche den Song eindeutig identifiziert).
    """
    url = f"{notion_query_endpoint}/{week_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    entries = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        popularity = props.get("Popularity Score", {}).get("number")
        # Nutze das Property "Date" für den Zeitstempel
        date_str = props.get("Date", {}).get("date", {}).get("start")
        # Extrahiere die Notion Track ID aus dem Property "Notion Track ID"
        track_id = ""
        if "Notion Track ID" in props:
            rich_text = props["Notion Track ID"].get("rich_text", [])
            track_id = "".join(rt.get("plain_text", "") for rt in rich_text).strip()
        entries.append({
            "date": date_str,
            "popularity": popularity,
            "notion_track_id": track_id
        })
    return entries

# Daten abrufen und in einen DataFrame umwandeln
data = get_weeks_data()
df = pd.DataFrame(data)
# Datum parsen
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])
st.write("Rohdaten aus Notion (Zeige die ersten 5 Einträge):", df.head())

# Filtere die Daten für den Song "Erfolg ist kein Glück" (Notion Track ID: 7ocanzdHHuxnYCHgS40CPF)
df_filtered = df[df["notion_track_id"] == "7ocanzdHHuxnYCHgS40CPF"]

st.write("Alle Messwerte für 'Erfolg ist kein Glück':", df_filtered)
