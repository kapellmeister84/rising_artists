import streamlit as st
import requests
import datetime
import base64
import json
import re
import time

#########################
# Notion Configuration  #
#########################
# Song, Measurements, and Artist database IDs
song_database_id = st.secrets["notion"]["song-database"]
measurements_database_id = st.secrets["notion"]["measurements-database"]
artist_database_id = st.secrets["notion"]["artist-database"]
notion_secret = st.secrets["notion"]["secret"]

# Endpoints and headers
notion_page_endpoint = "https://api.notion.com/v1/pages"
song_query_endpoint = f"https://api.notion.com/v1/databases/{song_database_id}/query"
measurements_query_endpoint = f"https://api.notion.com/v1/databases/{measurements_database_id}/query"
artist_query_endpoint = f"https://api.notion.com/v1/databases/{artist_database_id}/query"

notion_headers = {
    "Authorization": f"Bearer {notion_secret}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#########################
# Spotify Configuration #
#########################
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

#########################
# SONG & MEASUREMENTS   #
#########################
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
            # Get song popularity from track data
            song_pop = track.get("popularity", 0)
            # Get Release Date and Country Code from the album
            release_date = track.get("album", {}).get("release_date")
            available_markets = track.get("album", {}).get("available_markets", [])
            country_code = available_markets[0] if available_markets else ""
            # Get artist popularity via additional request
            artist_pop = get_artist_popularity(artist_id, token) if artist_id else 0
            # Streams: placeholder 0 (or integrate another API if available)
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
            # Properties updated on each scan:
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

def update_song_measurements():
    """
    For every song in the song database, fetch updated Spotify data (release date, country code, etc.)
    and update its measurement entry.
    """
    spotify_token = get_spotify_token()
    st.write("Spotify Access Token:", spotify_token)
    # Query all songs from the song database
    payload = {}
    response = requests.post(song_query_endpoint, headers=notion_headers, json=payload)
    songs = []
    if response.status_code == 200:
        for page in response.json().get("results", []):
            page_id = page["id"]
            track_prop = page["properties"].get("Track ID", {})
            track_text = ""
            if track_prop.get("rich_text") and len(track_prop["rich_text"]) > 0:
                track_text = track_prop["rich_text"][0]["text"]["content"]
            songs.append({
                "page_id": page_id,
                "track_id": track_text
            })
    else:
        st.error("Error querying the song database: " + response.text)
        return []
    messages = []
    current_week = f"KW {datetime.date.today().isocalendar()[1]}"
    for song in songs:
        updated_data = update_song_data(song, spotify_token)
        if updated_data:
            msg = update_song_page(song["page_id"], updated_data)
            messages.append(msg)
            meas_id = query_measurement_entry(song["page_id"], current_week)
            if meas_id:
                msg2 = update_measurement_entry(meas_id, updated_data["song_pop"], updated_data["artist_pop"], updated_data["streams"])
                messages.append(f"Measurement for song (ID: {song['track_id']}): {msg2}")
            else:
                msg2 = create_measurement_entry(song["page_id"], "Unknown", updated_data["song_pop"], updated_data["artist_pop"], updated_data["streams"], current_week)
                messages.append(msg2)
    return messages

def update_song_data(song, token):
    """
    For a given song (by track_id), fetch the latest Spotify data.
    Returns a dict with updated values.
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
        artists = data.get("artists", [])
        artist_id = artists[0].get("id") if artists and artists[0].get("id") else ""
        artist_pop = get_artist_popularity(artist_id, token) if artist_id else 0
        streams = 0
        return {
            "song_name": data.get("name"),
            "release_date": release_date,
            "country_code": country_code,
            "song_pop": song_pop,
            "artist_pop": artist_pop,
            "streams": streams
        }
    else:
        st.error(f"Error fetching data for track {song['track_id']}: {response.text}")
        return {}

#########################
# ARTIST DATA FUNCTIONS #
#########################
def get_all_artist_pages():
    """
    Query all artist pages from the artist database.
    """
    payload = {}
    response = requests.post(artist_query_endpoint, headers=notion_headers, json=payload)
    pages = []
    if response.status_code == 200:
        for page in response.json().get("results", []):
            # Expecting properties: NAME (title), Spotify ID (rich_text),
            # Bürgerlicher Name (rich_text) - if applicable.
            name = ""
            prop_name = page["properties"].get("NAME")
            if prop_name and prop_name.get("title") and len(prop_name["title"]) > 0:
                name = prop_name["title"][0]["plain_text"].strip()
            spotify_id = ""
            prop_spotify = page["properties"].get("Spotify ID")
            if prop_spotify and prop_spotify.get("rich_text") and len(prop_spotify["rich_text"]) > 0:
                spotify_id = prop_spotify["rich_text"][0]["plain_text"].strip()
            civil_name = ""
            prop_civil = page["properties"].get("Bürgerlicher Name")
            if prop_civil and prop_civil.get("rich_text") and len(prop_civil["rich_text"]) > 0:
                civil_name = prop_civil["rich_text"][0]["plain_text"].strip()
            pages.append({
                "page_id": page["id"],
                "name": name,
                "spotify_id": spotify_id,
                "civil_name": civil_name
            })
    else:
        st.error("Error querying the artist database: " + response.text)
    return pages

def get_artist_details(artist_id, token):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    else:
        st.error(f"Error retrieving artist data for ID {artist_id}: {resp.status_code}")
        return None

def search_spotify_artist(artist_name, token, market="DE"):
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"q": artist_name, "type": "artist", "limit": 10}
    if market:
        params["market"] = market
    resp = requests.get(url, headers=headers, params=params).json()
    if "artists" in resp and resp["artists"]["items"]:
        return resp["artists"]["items"]
    return []

def choose_artist(results, artist_name, token):
    # If multiple artists are found, choose the first one (or implement a selection logic)
    if results:
        return results[0]
    return None

def get_monthly_listeners_from_html(artist_id):
    url = f"https://open.spotify.com/artist/{artist_id}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        html = r.text
        match = re.search(r'([\d\.,]+)\s*(?:Monthly Listeners|Hörer monatlich)', html, re.IGNORECASE)
        if match:
            value = match.group(1)
            value = value.replace('.', '').replace(',', '')
            try:
                return int(value)
            except Exception as e:
                st.error("Error converting monthly listeners: " + str(e))
        else:
            st.error(f"No monthly listeners value found for artist {artist_id}.")
    else:
        st.error(f"Error fetching artist page for {artist_id}: Status {r.status_code}")
    return None

def update_artist_page(page_id, new_name, monthly_listeners, spi, profile_picture_url, spotify_id, followers, real_name, spotify_followers, current_civil_name):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    properties = {
        "NAME": {"title": [{"text": {"content": new_name}}]},
        "Monthly Listeners": {"number": monthly_listeners},
        "SPI": {"number": spi},
        "Spotify ID": {"rich_text": [{"text": {"content": spotify_id}}]},
        "Spotify Follower": {"number": spotify_followers}
    }
    # Only update civil/real name if not already set
    if not current_civil_name and real_name:
        properties["Bürgerlicher Name"] = {"rich_text": [{"text": {"content": real_name}}]}
    data = {
        "properties": properties,
        "icon": {"external": {"url": profile_picture_url}},
        "cover": {"external": {"url": profile_picture_url}}
    }
    resp = requests.patch(url, headers=notion_headers, data=json.dumps(data))
    return resp.json()

def format_real_name(name):
    if "," in name:
        parts = name.split(",")
        if len(parts) >= 2:
            first = parts[1].strip()
            last = parts[0].strip()
            return f"{first} {last}"
    return name

def get_real_name(artist_name):
    url = f"https://musicbrainz.org/ws/2/artist/?query=artist:\"{artist_name}\"&fmt=json"
    headers = {"User-Agent": "StreamlitApp/1.0 (your.email@example.com)"}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            if data.get("artists"):
                time.sleep(1)  # rate limiting
                first = data["artists"][0]
                if first.get("sort-name"):
                    return format_real_name(first["sort-name"])
    except Exception as e:
        st.error("MusicBrainz error: " + str(e))
    return ""

def sync_artists_with_spotify():
    """
    For each artist in the artist database, update their Spotify data: current popularity (SPI), monthly listeners, followers, etc.
    """
    spotify_token = get_spotify_token()
    st.write("Spotify Access Token:", spotify_token)
    pages = get_all_artist_pages()
    if not pages:
        st.error("No artist pages found.")
        return []
    messages = []
    for page in pages:
        artist_name = page["name"]
        current_civil_name = page["civil_name"]
        stored_spotify_id = page["spotify_id"]
        if not artist_name:
            continue
        st.write(f"Processing artist: {artist_name}")
        if stored_spotify_id:
            artist_data = get_artist_details(stored_spotify_id, spotify_token)
            if not artist_data:
                messages.append(f"No data found for Spotify ID {stored_spotify_id}.")
                continue
        else:
            results = search_spotify_artist(artist_name, spotify_token, market="DE")
            if not results:
                st.write(f"No German Spotify artist found for {artist_name}, searching international...")
                results = search_spotify_artist(artist_name, spotify_token, market=None)
                if not results:
                    messages.append(f"No Spotify match for {artist_name}.")
                    continue
            artist_data = choose_artist(results, artist_name, spotify_token)
            if not artist_data:
                messages.append(f"No artist selected for {artist_name}.")
                continue
        new_name = artist_data["name"]
        spi = artist_data["popularity"]
        followers = artist_data["followers"]["total"]
        profile_picture_url = artist_data["images"][0]["url"] if artist_data.get("images") else ""
        monthly_listeners = get_monthly_listeners_from_html(artist_data["id"])
        if monthly_listeners is None:
            monthly_listeners = followers  # fallback
        spotify_id = artist_data["id"]
        real_name = ""
        if not current_civil_name:
            real_name = get_real_name(new_name)
        spotify_followers = followers
        st.write(f"Updating Notion for {new_name} | Monthly Listeners: {monthly_listeners} | Popularity: {spi} | Followers: {spotify_followers} | Real Name: {real_name if real_name else '[unchanged]'}")
        update_resp = update_artist_page(page["page_id"], new_name, monthly_listeners, spi, profile_picture_url, spotify_id, followers, real_name, spotify_followers, current_civil_name)
        messages.append(f"Updated artist {new_name}: {update_resp}")
    return messages

#########################
# Streamlit App Layout  #
#########################
st.title("Spotify to Notion Music Sync")

if st.sidebar.button("Get New Music"):
    st.write("Fetching new music and updating measurements...")
    results = get_new_music()
    for res in results:
        st.write(res)

if st.sidebar.button("Get Measurements"):
    st.write("Updating measurements for existing songs...")
    meas_results = update_song_measurements()
    for res in meas_results:
        st.write(res)

if st.sidebar.button("Update Artist Data"):
    st.write("Syncing artist data with Spotify...")
    artist_results = sync_artists_with_spotify()
    for res in artist_results:
        st.write(res)
