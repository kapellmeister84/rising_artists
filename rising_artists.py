import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time

st.set_page_config(layout="wide")
st.title("Weeks-Datenbank – Debug Script")

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
    Für jeden Eintrag werden:
      - Popularity Score
      - Zeitstempel aus Property "Date"
      - Notion Track ID (Relation "Song" optional, oder falls du in 'Notion Track ID' speicherst)
    extrahiert.
    """
    url = f"{notion_query_endpoint}/{week_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()

    entries = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        popularity = props.get("Popularity Score", {}).get("number")
        # 1) Zeitstempel aus "Date"
        date_str = props.get("Date", {}).get("date", {}).get("start")
        # 2) Notion Track ID aus "Notion Track ID"
        track_id = ""
        if "Notion Track ID" in props:
            rich_text = props["Notion Track ID"].get("rich_text", [])
            track_id = "".join(rt.get("plain_text", "") for rt in rich_text).strip()

        # Optional: Du könntest auch aus der Relation "Song" die Song-ID ziehen, falls du das willst:
        # song_relations = props.get("Song", {}).get("relation", [])
        # song_id = song_relations[0]["id"] if song_relations else None

        entries.append({
            "date": date_str,
            "popularity": popularity,
            "notion_track_id": track_id
        })

    return entries

# --- Daten laden ---
data = get_weeks_data()
df = pd.DataFrame(data)

# Debug 1: Rohdaten ausgeben
st.write("**Debug 1:** Rohdaten (unbearbeitet) – Zeige die ersten Zeilen:", df.head(len(df)))

# Datum parsen
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])

# Debug 2: Daten nach Datumskonvertierung ausgeben
st.write("**Debug 2:** Nach Datumskonvertierung und Drop-NA:", df.head(len(df)))

# Gruppieren nach Notion Track ID
grouped = df.groupby("notion_track_id")

# Für jede Track ID einen Graph erstellen
for track_id, group in grouped:
    group = group.sort_values("date")

    # Debug 3: Zeige alle Einträge, die in dieser Gruppe landen
    st.write(f"**Debug 3:** Gruppe Notion Track ID = {track_id}")
    st.write(group)

    # Plotly-Graph
    fig = px.line(group, x="date", y="popularity", markers=True,
                  title=f"Track ID: {track_id}",
                  labels={"date": "Zeit", "popularity": "Popularity"})
    fig.update_yaxes(range=[0, 100])  # y-Achse fix 0–100

    # Expander, damit der Graph neu geladen wird, wenn du ihn aufklappst
    with st.expander(f"Graph für Notion Track ID: {track_id}"):
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{track_id}_{time.time()}")
    
