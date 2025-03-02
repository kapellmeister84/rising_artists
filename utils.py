import streamlit as st

def set_background(url: str):
    """
    Setzt den Hintergrund der Streamlit-App auf das Bild von der angegebenen URL.
    """
    page_bg_img = f"""
    <style>
    .stApp {{
        background-image: url("{url}");
        background-size: cover;
        background-attachment: fixed;
        background-position: center;
    }}
    </style>
    """
    st.markdown(page_bg_img, unsafe_allow_html=True)
