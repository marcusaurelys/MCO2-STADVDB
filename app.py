import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from st_aggrid import AgGrid
user = st.secrets['USER']
password = st.secrets['PASSWORD']
host = st.secrets['HOST']
database = 'games'
port = st.secrets['PRIMARY_PORT']  # Default MySQL port
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')



st.set_page_config(page_title="Steam Games Management System", page_icon=":video_game:", layout="wide")
st.title("🎮 Steam Games Management System")
st.markdown('<style> div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)



st.header("View all Games")
query = """
        SELECT `AppID`, `Name`, `Price`, `Developers`, `Publishers`, `Language 1`, `Language 2`, `Language 3`, `Genre 1`, `Genre 2`, `Genre 3`, `Windows`, `Mac`, `Linux` FROM games;
        """
games_df = pd.read_sql(query, engine)
AgGrid(games_df, width=150)