import streamlit as st
import requests
import datetime
import json
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide")

# --- Minimalbeispiel: wir tun so, als hätten wir schon 10 Songs im DataFrame "top10_songs" ---
# In deiner App holst du die Daten aus Notion + Spotify wie gehabt. Hier sind nur Dummies:
top10_songs = [
    {
        "cover_url": "https://i.scdn.co/image/ab67616d0000b273c1856689ad98a80116b54504",
        "title": "Ein ziemlich langer Songtitel, der sich sehr weit streckt und das Layout sprengt",
        "artist": "Some Artist",
        "release_date": "2025-02-28",
        "popularity": 41.0,
        "growth": 8.5,
        "spotify_link": "https://open.spotify.com/track/dummy"
    },
    {
        "cover_url": "https://i.scdn.co/image/ab67616d0000b273b8fe9477be9fcdd16bf6a3f8",
        "title": "Kurz",
        "artist": "Jemand Anderes",
        "release_date": "2025-02-26",
        "popularity": 25.0,
        "growth": 12.3,
        "spotify_link": "https://open.spotify.com/track/dummy2"
    },
    # ... weitere 8 Songs ...
]

# --- CSS für ein fixes Grid und Hover-Overlay ---
custom_css = """
<style>
.song-grid {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 1rem;
}

.song-card {
  position: relative;
  width: 100%;
  height: 300px; /* Feste Höhe, damit nichts verrutscht */
  overflow: hidden;
  text-align: center;
  border: 1px solid #ddd;
  border-radius: 5px;
}

/* Das Cover füllt die Breite aus, wird aber maximal 100% hoch. */
.song-card img {
  width: 100%;
  height: auto;
  max-height: 100%;
  display: block;
  object-fit: cover;
}

/* Overlay: erst unsichtbar, beim Hover wird es eingeblendet. */
.song-info-overlay {
  position: absolute;
  top: 0; left: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(0,0,0,0.6);
  color: #fff;
  padding: 0.5rem;
  display: none;
  box-sizing: border-box;
  overflow: auto;
}

/* Beim Hover auf die Karte: Overlay anzeigen */
.song-card:hover .song-info-overlay {
  display: block;
}

/* Titel, Artist etc. mittig platzieren */
.song-info-overlay .info-text {
  margin-top: 10%;
  text-align: center;
  padding: 0.5rem;
  line-height: 1.2;
  font-size: 0.9rem;
  word-wrap: break-word;
}

/* Langen Titel etwas kürzen, optional */
.song-title {
  font-weight: bold;
  margin-bottom: 0.5rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  width: 100%;
  display: inline-block;
}
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# --- Galerie-Grid erzeugen ---
html_cards = []
html_cards.append('<div class="song-grid">')

for song in top10_songs:
    cover_url = song["cover_url"]
    title = song["title"]
    artist = song["artist"]
    release_date = song["release_date"]
    popularity = song["popularity"]
    growth = song["growth"]
    link = song["spotify_link"]

    # HTML für eine Karte
    card_html = f"""
    <div class="song-card">
      <img src="{cover_url}" alt="Cover" />
      <div class="song-info-overlay">
        <div class="info-text">
          <div class="song-title">{title}</div>
          <div>{artist}</div>
          <div>Release: {release_date}</div>
          <div>Popularity: {popularity:.1f}</div>
          <div>Growth: {growth:.1f}%</div>
          <div><a href="{link}" target="_blank" style="color: #fff; text-decoration: underline;">Spotify Link</a></div>
        </div>
      </div>
    </div>
    """
    html_cards.append(card_html)

html_cards.append('</div>')
st.markdown("".join(html_cards), unsafe_allow_html=True)

st.write("Fertig ist das Hover-Layout!")
