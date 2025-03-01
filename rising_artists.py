import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px

# --- Notion Einstellungen ---
songs_database_id = "b94c8042619d42a3be799c9795984150"  # Songs-Datenbank
week_database_id = "1a9b6204cede80e29338ede2c76999f2"    # Week-Tracking-Datenbank
notion_secret = "secret_yYvZbk7zcKy0Joe3usdCHMbbZmAFHnCKrF7NvEkWY6E"
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- Funktionen zum Laden der Tracking-Daten aus der Week-Datenbank ---
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

# Hole Mapping Song Page ID -> Artist Name aus der Songs-Datenbank
def get_song_artist_mapping():
    url = f"{notion_query_endpoint}/{songs_database_id}/query"
    response = requests.post(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    mapping = {}
    for page in data.get("results", []):
        page_id = page["id"]
        props = page["properties"]
        # Annahme: "Artist Name" ist als Rich Text definiert
        if "Artist Name" in props and props["Artist Name"]["rich_text"]:
            artist = props["Artist Name"]["rich_text"][0]["plain_text"]
        else:
            artist = "Unbekannt"
        mapping[page_id] = artist
    return mapping

# Berechne den prozentualen Unterschied zwischen den letzten zwei Messungen pro Artist
def calculate_recent_growth(df, threshold):
    qualified_artists = []
    growth_info = {}
    for artist, group in df.groupby("artist"):
        group = group.sort_values("date")
        if len(group) < 2:
            continue  # nicht genug Daten
        # Nehme die letzten beiden Messungen
        last_two = group.tail(2)
        prev = last_two.iloc[0]["popularity"]
        last = last_two.iloc[1]["popularity"]
        if prev == 0:
            continue
        pct_change = ((last - prev) / prev) * 100
        growth_info[artist] = pct_change
        if pct_change >= threshold:
            qualified_artists.append(artist)
    return qualified_artists, growth_info

# Streamlit App
st.title("Artists mit signifikantem Popularity-Anstieg (Letzte Messungen)")

# Konfigurierbarer Schwellenwert (in Prozent)
threshold = st.number_input("Min. prozentuale Steigerung zwischen den letzten zwei Messungen:", min_value=0.0, value=3.0, step=0.5)

st.write("Lade Tracking-Daten aus der Week-Datenbank...")
week_entries = get_week_entries()
song_artist_mapping = get_song_artist_mapping()

df = pd.DataFrame(week_entries)
if df.empty:
    st.write("Keine Tracking-Daten gefunden.")
else:
    df["date"] = pd.to_datetime(df["date"])
    df["artist"] = df["song_id"].map(song_artist_mapping)
    st.write("Alle Tracking-Daten:")
    st.dataframe(df)

    # Berechne den Wachstum der letzten zwei Messungen pro Artist
    qualified_artists, growth_info = calculate_recent_growth(df, threshold)
    
    st.write(f"Artists mit mindestens {threshold}% Wachstum (letzte zwei Messungen):")
    if qualified_artists:
        for artist in qualified_artists:
            st.write(f"- **{artist}**: Wachstum von {growth_info[artist]:.2f}%")
    else:
        st.write("Keine Artists erfüllen das Kriterium.")

    # Plotte für jeden qualifizierten Artist die komplette Tracking-History
    st.write("Tracking-History Graphen:")
    if qualified_artists:
        for artist in qualified_artists:
            artist_df = df[df["artist"] == artist].sort_values("date")
            fig = px.line(artist_df, x="date", y="popularity",
                          title=f"Tracking History: {artist} (Wachstum: {growth_info[artist]:.2f}%)",
                          markers=True, labels={"date": "Datum", "popularity": "Popularity Score"})
            st.plotly_chart(fig)
    else:
        st.write("Keine Graphen, da keine Artists die Kriterien erfüllen.")
