import streamlit as st
import requests
import datetime
import json
from concurrent.futures import ThreadPoolExecutor

# Set page config (optional)
st.set_page_config(layout="wide")

#########################
# Notion-Konfiguration  #
#########################
# Neue Datenbankversion: "song-database" für Songs
songs_database_id = st.secrets["notion"]["song-database"]
notion_secret = st.secrets["notion"]["secret"]

# Endpunkt und Header für Notion
notion_query_endpoint = "https://api.notion.com/v1/databases"
notion_page_endpoint = "https://api.notion.com/v1/pages"
notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#########################
# Caching von Notion-Daten
#########################
@st.cache_data(show_spinner=False)
def get_measurement_details(measurement_id):
    """
    Ruft für eine gegebene Measurement-Seite (ID) die Details ab.
    Erwartete Properties in der Measurements-Datenbank:
      - Song Pop (number)
      - Artist Pop (number)
      - Streams (number)
      - Monthly Listeners (number)
      - Artist Followers (number)
    """
    url = f"{notion_page_endpoint}/{measurement_id}"
    response = requests.get(url, headers=notion_headers)
    response.raise_for_status()
    data = response.json()
    props = data.get("properties", {})
    return {
        "song_pop": props.get("Song Pop", {}).get("number"),
        "artist_pop": props.get("Artist Pop", {}).get("number"),
        "streams": props.get("Streams", {}).get("number"),
        "monthly_listeners": props.get("Monthly Listeners", {}).get("number"),
        "artist_followers": props.get("Artist Followers", {}).get("number")
    }

@st.cache_data(show_spinner=False)
def get_songs_metadata():
    """
    Ruft alle Einträge aus der Songs-Datenbank ab und extrahiert folgende Properties:
      - Track Name (title)
      - Artist Name (rich_text)
      - Artist ID (rich_text)
      - Track ID (rich_text)
      - Release Date (date)
      - Country Code (rich_text)
      - Measurements (relation) – Für jeden verknüpften Eintrag werden die Measurement-Details abgefragt.
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
    with ThreadPoolExecutor() as executor:
        measurement_futures = {}
        for page in pages:
            props = page.get("properties", {})

            # Track Name
            track_name = ""
            if "Track Name" in props and props["Track Name"].get("title"):
                track_name = "".join([t.get("plain_text", "") for t in props["Track Name"]["title"]]).strip()

            # Artist Name
            artist_name = ""
            if "Artist Name" in props and props["Artist Name"].get("rich_text"):
                artist_name = "".join([t.get("plain_text", "") for t in props["Artist Name"]["rich_text"]]).strip()

            # Artist ID
            artist_id = ""
            if "Artist ID" in props and props["Artist ID"].get("rich_text"):
                artist_id = "".join([t.get("plain_text", "") for t in props["Artist ID"]["rich_text"]]).strip()

            # Track ID
            track_id = ""
            if "Track ID" in props and props["Track ID"].get("rich_text"):
                track_id = "".join([t.get("plain_text", "") for t in props["Track ID"]["rich_text"]]).strip()

            # Release Date
            release_date = ""
            if "Release Date" in props and props["Release Date"].get("date"):
                release_date = props["Release Date"]["date"].get("start", "")

            # Country Code
            country_code = ""
            if "Country Code" in props and props["Country Code"].get("rich_text"):
                country_code = "".join([t.get("plain_text", "") for t in props["Country Code"]["rich_text"]]).strip()

            # Measurements: Relation Property
            measurements_ids = []
            if "Measurements" in props and props["Measurements"].get("relation"):
                for rel in props["Measurements"]["relation"]:
                    m_id = rel.get("id")
                    if m_id:
                        measurements_ids.append(m_id)
                        measurement_futures[m_id] = executor.submit(get_measurement_details, m_id)
            key = track_id if track_id else page.get("id")
            metadata[key] = {
                "page_id": page.get("id"),
                "track_name": track_name,
                "artist_name": artist_name,
                "artist_id": artist_id,
                "track_id": track_id,
                "release_date": release_date,
                "country_code": country_code,
                "measurements_ids": measurements_ids
            }
        # Ergänze die Measurement-Details
        for measurement_id, future in measurement_futures.items():
            try:
                details = future.result()
            except Exception as e:
                details = {"song_pop": None, "artist_pop": None, "streams": None, "monthly_listeners": None, "artist_followers": None}
            for key, song_data in metadata.items():
                if measurement_id in song_data.get("measurements_ids", []):
                    if "measurements" not in song_data:
                        song_data["measurements"] = []
                    song_data["measurements"].append({"id": measurement_id, **details})
    return metadata

#########################
# UI – Button "Get New Music"
#########################
st.sidebar.title("Songs Cache")
if st.sidebar.button("Get New Music"):
    # Lösche den Cache und lade die Daten neu
    get_songs_metadata.clear()
    st.experimental_rerun()

# Lade die Songs-Metadaten inklusive Measurements aus der Notion-Songs-Datenbank
songs_metadata = get_songs_metadata()
st.title("Songs Metadata from Notion (with Measurements)")
st.write("Loaded songs metadata:")
st.write(songs_metadata)
