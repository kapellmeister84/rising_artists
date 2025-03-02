import streamlit as st
import requests
import datetime
import json

# Set page config (optional)
st.set_page_config(layout="wide")
# Optional: set_dark_mode() und set_background() aus utils, falls gewünscht
#from utils import set_dark_mode, set_background
#set_dark_mode()
#set_background("https://wallpapershome.com/images/pages/pic_h/26334.jpg")

#########################
# Notion-Konfiguration  #
#########################
# Wir nutzen die neue Songs-Datenbank ("song-database")
songs_database_id = st.secrets["notion"]["song-database"]
# measurements-database wird hier nicht benötigt, da dieses Script nur die Song-Daten lädt
notion_secret = st.secrets["notion"]["token"]

# Endpunkt und Header für Notion
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#########################
# Songs-Daten abrufen und cachen
#########################
@st.cache_data(show_spinner=False)
def get_songs_metadata():
    """
    Ruft alle Einträge aus der Songs-Datenbank ab und extrahiert folgende Properties:
      - Track Name (Typ: title)
      - Artist Name (Typ: rich_text)
      - Artist ID (Typ: rich_text)
      - Track ID (Typ: rich_text)
      - Release Date (Typ: date)
      - Country Code (Typ: rich_text)
    """
    url = f"{notion_query_endpoint}/{songs_database_id}/query"
    payload = {"page_size": 100}
    pages = []
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=notion_headers, json=payload)
        response.raise_for_status()
        data = response.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    metadata = {}
    for page in pages:
        props = page.get("properties", {})

        # Track Name (title)
        track_name = ""
        if "Track Name" in props and props["Track Name"].get("title"):
            track_name = "".join([t.get("plain_text", "") for t in props["Track Name"]["title"]]).strip()

        # Artist Name (rich_text)
        artist_name = ""
        if "Artist Name" in props and props["Artist Name"].get("rich_text"):
            artist_name = "".join([t.get("plain_text", "") for t in props["Artist Name"]["rich_text"]]).strip()

        # Artist ID (rich_text)
        artist_id = ""
        if "Artist ID" in props and props["Artist ID"].get("rich_text"):
            artist_id = "".join([t.get("plain_text", "") for t in props["Artist ID"]["rich_text"]]).strip()

        # Track ID (rich_text)
        track_id = ""
        if "Track ID" in props and props["Track ID"].get("rich_text"):
            track_id = "".join([t.get("plain_text", "") for t in props["Track ID"]["rich_text"]]).strip()

        # Release Date (date)
        release_date = ""
        if "Release Date" in props and props["Release Date"].get("date"):
            release_date = props["Release Date"]["date"].get("start", "")

        # Country Code (rich_text)
        country_code = ""
        if "Country Code" in props and props["Country Code"].get("rich_text"):
            country_code = "".join([t.get("plain_text", "") for t in props["Country Code"]["rich_text"]]).strip()

        # Verwende als Schlüssel die Track ID (falls vorhanden) oder die Page-ID
        key = track_id if track_id else page.get("id")
        metadata[key] = {
            "page_id": page.get("id"),
            "track_name": track_name,
            "artist_name": artist_name,
            "artist_id": artist_id,
            "track_id": track_id,
            "release_date": release_date,
            "country_code": country_code
        }
    return metadata

# Laden der Songs-Metadaten aus der Notion-Songs-Datenbank
songs_metadata = get_songs_metadata()
st.title("Songs Metadata from Notion")
st.write("Loaded songs metadata:")
st.write(songs_metadata)
