def update_popularity():
    import uuid  # Falls noch nicht importiert

    # --- Notion Einstellungen für Update Popularity ---
    songs_database_id = "1a9b6204cede8006b67fd247dc660ba4"  # Songs-Datenbank
    week_database_id = "1a9b6204cede80e29338ede2c76999f2"    # Week-Datenbank

    def get_all_song_page_ids():
        url = f"{notion_query_endpoint}/{songs_database_id}/query"
        response = requests.post(url, headers=notion_headers)
        response.raise_for_status()
        data = response.json()
        song_pages = []
        for page in data.get("results", []):
            page_id = page["id"]
            popularity = 0
            if "Popularity" in page["properties"]:
                popularity = page["properties"]["Popularity"].get("number", 0)
            song_pages.append({"page_id": page_id, "popularity": popularity})
        return song_pages

    def get_song_name(page_id):
        url = f"{notion_page_endpoint}/{page_id}"
        response = requests.get(url, headers=notion_headers)
        response.raise_for_status()
        data = response.json()
        props = data.get("properties", {})
        if "Name" in props and "title" in props["Name"]:
            title_items = props["Name"].get("title", [])
            song_name = "".join(item.get("plain_text", "") for item in title_items).strip()
            if song_name:
                return song_name
        return page_id  # Fallback: Page-ID nutzen

    def create_week_entry(song_page_id, popularity_score, track_id):
        now_iso = datetime.datetime.now().isoformat()
        payload = {
            "parent": { "database_id": week_database_id },
            "properties": {
                "Name": { 
                    "title": [
                        { "text": { "content": f"Week of {now_iso[:10]}" } }
                    ]
                },
                "Song": {
                    "relation": [
                        { "id": song_page_id }
                    ]
                },
                "Popularity Score": {
                    "number": popularity_score
                },
                "Date": {
                    "date": { "start": now_iso }
                },
                "Notion Track ID": {
                    "rich_text": [
                        { "text": { "content": track_id } }
                    ]
                }
            }
        }
        response = requests.post(notion_page_endpoint, headers=notion_headers, json=payload)
        if response.status_code == 200:
            st.write(f"Week Entry für Song {song_page_id} mit Track ID {track_id} erstellt.")
        else:
            st.write(f"Fehler beim Erstellen des Week Entry für Song {song_page_id}: {response.text}")

    song_pages = get_all_song_page_ids()
    st.write(f"Gefundene Songs: {len(song_pages)}")

    song_to_track = {}
    for song in song_pages:
        page_id = song["page_id"]
        song_name = get_song_name(page_id)
        if not song_name:
            st.write(f"Song-Seite {page_id} liefert keinen Songnamen, überspringe.")
            continue
        if song_name not in song_to_track:
            song_to_track[song_name] = str(uuid.uuid4())
        track_id = song_to_track[song_name]
        create_week_entry(page_id, song["popularity"], track_id)

    st.success("Popularity wurde aktualisiert!")
