import streamlit as st

def set_background(url: str, overlay_color: str = "rgba(0, 0, 0, 0.5)"):
    """
    Setzt den Hintergrund der Streamlit-App auf das Bild von der angegebenen URL und legt
    ein halbtransparentes Overlay über das Bild, um es abzudunkeln.
    
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
