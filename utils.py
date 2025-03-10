import streamlit as st

def set_background(url: str, overlay_color: str = "rgba(0, 0, 0, 0.5)"):
    """
    Setzt den Hintergrund der Streamlit-App auf das Bild von der angegebenen URL
    und legt ein halbtransparentes Overlay über das Bild, um es abzudunkeln.
    
    :param url: URL des Hintergrundbildes
    :param overlay_color: CSS-Farbwert für das Overlay, Standard: halbtransparentes Schwarz
    """
    page_bg_img = f"""
    <style>
    .stApp {{
        background: 
            linear-gradient({overlay_color}, {overlay_color}),
            url("{url}");
        background-size: cover;
        background-attachment: fixed;
        background-position: center;
    }}
    </style>
    """
    st.markdown(page_bg_img, unsafe_allow_html=True)

def set_dark_mode():
    """
    Erzwingt einen Darkmode für die gesamte Streamlit-App, inkl. Sidebar.
    Dabei werden auch die Eingabefelder und Buttons in der Sidebar angepasst,
    sodass die Schriftfarbe dunkelgrau (#555555) erscheint.
    """
    dark_mode_css = """
    <style>
    :root {
        color-scheme: dark;
    }
    body {
        background-color: #0e1117;
        color: #c9d1d9;
    }
    .stApp {
        background-color: #0e1117;
        color: #c9d1d9;
    }
    /* Sidebar anpassen */
    [data-testid="stSidebar"] {
        background-color: #0e1117;
        color: #c9d1d9;
    }
    [data-testid="stSidebar"] * {
        color: #c9d1d9;
    }
    /* Eingabefelder in der Sidebar: dunkelgraue Schrift */
    [data-testid="stSidebar"] input {
        color: #607B8B !important;
    }
    /* Buttons in der Sidebar: dunkelgraue Schrift */
    [data-testid="stSidebar"] button {
        color: #607B8B !important;
    }
    </style>
    """
    st.markdown(dark_mode_css, unsafe_allow_html=True)
