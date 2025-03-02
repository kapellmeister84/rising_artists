import streamlit as st
import requests
import datetime
import base64

# --- Notion Settings (from secrets.toml) ---
song_database_id = st.secrets["notion"]["song-database"]
measurements_database_id = st.secrets["notion"]["measurements-database"]
notion_secret = st.secrets["notion"]["secret"]

notion_page_endpoint = "https://api.notion.com/v1/pages"
song_query_endpoint = f"https://api.notion.com/v1/databases/{song_database_id}/query"
measurements_query_endpoint = f"https://api.notion.com/v1/databases/{measurements_database_id}/query"

notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- Spotify Settings (from secrets.toml) ---
playlist_ids = st.secrets["spotify"]["playlist_ids"]
spotify_client_id = st.secrets["spotify"]["client_id"]
spotify_client_secret = st.secrets["spotify"]["client_secret"]

def get_spotify_token():
    """
    Get a Spotify access token using the Client Credentials Flow.
    """
    auth_str = f"{spotify_client_id}:{spotify_client_secret}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()
    headers = {
        "Authorization": f"Basic {b64_auth_str}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def get_artist_popularity(artist_id, token):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("popularity", 0)
    return 0

def get_playlist_songs(playlist_id, token):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    response = requests.get(url, headers=headers)
    data = response.json()
    songs = []
    for item in data.get("tracks", {}).get("items", []):
        track = item.get("track")
        if track:
            song_name = track.get("name")
            artists = track.get("artists", [])
            artist_names = [artist.get("name") for artist in artists]
            artist_name = ", ".join(artist_names)
            artist_id = artists[0].get("id") if artists and artists[0].get("id") else ""
            track_id = track.get("id")
            # Get song popularity (directly from the track object)
            song_pop = track.get("popularity", 0)
            # Get Release Date and Country Code from the album
            release_date = track.get("album", {}).get("release_date")
            available_markets = track.get("album", {}).get("available_markets", [])
            country_code = available_markets[0] if available_markets else ""
            # Get artist popularity with an additional request
            artist_pop = get_artist_popularity(artist_id, token) if artist_id else 0
            # Streams: currently not available from the API, so set as placeholder 0
            streams = 0
            songs.append({
                "song_name": song_name,
                "artist_name": artist_name,
                "artist_id": artist_id,
                "track_id": track_id,
                "song_pop": song_pop,
                "release_date": release_date,
                "country_code": country_code,
                "artist_pop": artist_pop,
                "streams": streams
            })
    return songs

def query_song_page(track_id):
    """
    Check if a song with the given track_id exists in the song database.
    Return its page ID if found.
    """
    payload = {
        "filter": {
            "property": "Track ID",
            "rich_text": {
                "equals": track_id
            }
        }
    }
    response = requests.post(song_query_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            return results[0]["id"]
    else:
        st.error("Error querying the song database: " + response.text)
    return None

def create_song_page(song_data):
    now_iso = datetime.datetime.now().isoformat()
    payload = {
        "parent": { "database_id": song_database_id },
        "properties": {
            "Track Name": {
                "title": [{ "text": { "content": song_data["song_name"] or "Unknown" } }]
            },
            "Artist Name": {
                "rich_text": [{ "text": { "content": song_data["artist_name"] or "Unknown" } }]
            },
            "Artist ID": {
                "rich_text": [{ "text": { "content": song_data["artist_id"] or "" } }]
            },
            "Track ID": {
                "rich_text": [{ "text": { "content": song_data["track_id"] or "" } }]
            },
            # Properties to be updated with each scan:
            "Release Date": {
                "date": { "start": song_data["release_date"] or "" }
            },
            "Country Code": {
                "rich_text": [{ "text": { "content": song_data["country_code"] or "" } }]
            }
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        page_id = response.json().get("id")
        return f"Created: {song_data['song_name']}", page_id
    else:
        return f"Error creating {song_data['song_name']}: {response.text}", None

def update_song_page(page_id, song_data):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "Release Date": {
                "date": { "start": song_data["release_date"] or "" }
            },
            "Country Code": {
                "rich_text": [{ "text": { "content": song_data["country_code"] or "" } }]
            }
        }
    }
    response = requests.patch(url, headers=notion_headers, json=payload)
    if response.status_code == 200:
        return f"Updated: {song_data['song_name']}"
    else:
        return f"Error updating {song_data['song_name']}: {response.text}"

def query_measurement_entry(song_page_id, week):
    """
    Check if there is already a measurement entry for the given song (via relation)
    for the specified week. Return the measurement page ID if found.
    """
    payload = {
        "filter": {
            "and": [
                {
                    "property": "Name",
                    "rich_text": {
                        "equals": week
                    }
                },
                {
                    "property": "Song",
                    "relation": {
                        "contains": song_page_id
                    }
                }
            ]
        }
    }
    response = requests.post(measurements_query_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            return results[0]["id"]
    else:
        st.error("Error querying the Measurements database: " + response.text)
    return None

def create_measurement_entry(song_page_id, song_name, song_pop, artist_pop, streams, week):
    payload = {
        "parent": { "database_id": measurements_database_id },
        "properties": {
            "Name": {
                "title": [{ "text": { "content": week } }]
            },
            "Song": {
                "relation": [{ "id": song_page_id }]
            },
            "Song Pop": {
                "number": song_pop
            },
            "Artist Pop": {
                "number": artist_pop
            },
            "Streams": {
                "number": streams
            }
        }
    }
    response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
    if response.status_code == 200:
        return f"Measurement created for {song_name}"
    else:
        return f"Error creating measurement for {song_name}: {response.text}"

def update_measurement_entry(measurement_page_id, song_pop, artist_pop, streams):
    url = f"https://api.notion.com/v1/pages/{measurement_page_id}"
    payload = {
        "properties": {
            "Song Pop": {"number": song_pop},
            "Artist Pop": {"number": artist_pop},
            "Streams": {"number": streams}
        }
    }
    response = requests.patch(url, headers=notion_headers, json=payload)
    if response.status_code == 200:
        return "Measurement updated."
    else:
        return f"Error updating measurement: {response.text}"

def get_new_music():
    """
    Fetch new songs from the defined playlists, add them to the song database (or update existing entries),
    and update the measurement entries for the current week.
    """
    spotify_token = get_spotify_token()
    st.write("Spotify Access Token:", spotify_token)
    all_songs = []
    for pid in playlist_ids:
        songs = get_playlist_songs(pid, spotify_token)
        all_songs.extend(songs)
    st.write(f"Total songs collected: {len(all_songs)}")
    messages = []
    current_week = f"KW {datetime.date.today().isocalendar()[1]}"
    for song in all_songs:
        if song["track_id"]:
            song_page_id = query_song_page(song["track_id"])
            if song_page_id:
                msg = update_song_page(song_page_id, song)
                messages.append(msg)
            else:
                msg, song_page_id = create_song_page(song)
                messages.append(msg)
            meas_id = query_measurement_entry(song_page_id, current_week)
            if meas_id:
                msg = update_measurement_entry(meas_id, song["song_pop"], song["artist_pop"], song["streams"])
                messages.append(f"{song['song_name']}: {msg}")
            else:
                msg = create_measurement_entry(song_page_id, song["song_name"], song["song_pop"], song["artist_pop"], song["streams"], current_week)
                messages.append(msg)
        else:
            messages.append(f"{song['song_name']} has no Track ID and will be skipped.")
    return messages

def query_all_songs():
    """
    Query all songs in the song database.
    """
    payload = {}
    response = requests.post(song_query_endpoint, headers=notion_headers, json=payload)
    songs = []
    if response.status_code == 200:
        results = response.json().get("results", [])
        for page in results:
            # Extract the page id and Track ID (assumes a rich_text property for Track ID)
            page_id = page["id"]
            track_prop = page["properties"].get("Track ID", {})
            track_text = ""
            if track_prop.get("rich_text"):
                if len(track_prop["rich_text"]) > 0:
                    track_text = track_prop["rich_text"][0]["text"]["content"]
            songs.append({
                "page_id": page_id,
                "track_id": track_text
            })
    else:
        st.error("Error querying the song database: " + response.text)
    return songs

def update_song_data(song, token):
    """
    Given a song (with a track_id), query Spotify for the latest data.
    Returns a dict with updated values (release_date, country_code, song_pop, artist_pop).
    """
    url = f"https://api.spotify.com/v1/tracks/{song['track_id']}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        release_date = data.get("album", {}).get("release_date")
        available_markets = data.get("album", {}).get("available_markets", [])
        country_code = available_markets[0] if available_markets else ""
        song_pop = data.get("popularity", 0)
        # For artist popularity, we use the first artist id from the track data
        artists = data.get("artists", [])
        artist_id = artists[0].get("id") if artists and artists[0].get("id") else ""
        artist_pop = get_artist_popularity(artist_id, token) if artist_id else 0
        # Streams remain as placeholder 0 (or integrate another API if available)
        streams = 0
        return {
            "release_date": release_date,
            "country_code": country_code,
            "song_pop": song_pop,
            "artist_pop": artist_pop,
            "streams": streams
        }
    else:
        st.error(f"Error fetching data for track {song['track_id']}: {response.text}")
        return {}

def get_measurements():
    """
    For every song in the song database, fetch the latest measurement data from Spotify,
    update the song page with updated Release Date and Country Code,
    and then update (or create) the measurement entry for the current week.
    """
    spotify_token = get_spotify_token()
    st.write("Spotify Access Token:", spotify_token)
    songs = query_all_songs()
    st.write(f"Total songs in song database: {len(songs)}")
    messages = []
    current_week = f"KW {datetime.date.today().isocalendar()[1]}"
    for song in songs:
        # For each song, update its data from Spotify
        updated_data = update_song_data(song, spotify_token)
        if updated_data:
            # Update the song page with new Release Date and Country Code
            msg = update_song_page(song["page_id"], updated_data)
            messages.append(msg)
            # Now update the measurement entry
            meas_id = query_measurement_entry(song["page_id"], current_week)
            if meas_id:
                msg2 = update_measurement_entry(meas_id, updated_data["song_pop"], updated_data["artist_pop"], updated_data["streams"])
                messages.append(f"Measurement for song (ID: {song['track_id']}): {msg2}")
            else:
                msg2 = create_measurement_entry(song["page_id"], "Unknown", updated_data["song_pop"], updated_data["artist_pop"], updated_data["streams"], current_week)
                messages.append(msg2)
    return messages

st.title("Spotify to Notion Music Sync")

# Sidebar buttons
if st.sidebar.button("Get New Music"):
    st.write("Fetching new music and updating measurements...")
    results = get_new_music()
    for res in results:
        st.write(res)

if st.sidebar.button("Get Measurements"):
    st.write("Updating measurements for existing songs...")
    meas_results = get_measurements()
    for res in meas_results:
        st.write(res)
