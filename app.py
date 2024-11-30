import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from sqlalchemy.exc import SQLAlchemyError

user = st.secrets['USER']
password = st.secrets['PASSWORD']
host = st.secrets['HOST']
database = 'games'
port = st.secrets['PRIMARY_PORT']
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')

st.set_page_config(page_title="Steam Games Management System", page_icon=":video_game:", layout="wide")
st.title("🎮 Steam Games Management System")
st.markdown('<style> div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)

#Sidebar
action = st.sidebar.selectbox("Choose an action", ["View Games", "Add Game", "Update Game", "Delete Game"])

# read operation
if action == "View Games":
    st.header("View All Games")
    query = """
        SELECT `AppID`, `Name`, `Price`, `Developers`, `Publishers`, `Language 1`, `Language 2`, `Language 3`, 
               `Genre 1`, `Genre 2`, `Genre 3`, `Windows`, `Mac`, `Linux`
        FROM games;
    """
    try:
        games_df = pd.read_sql(query, engine)
        AgGrid(games_df, width=150)
    except SQLAlchemyError as e:
        st.error(f"Error: {str(e)}")

#create operation
elif action == "Add Game":
    st.header("➕ Add a New Game")
    with st.form("add_game_form", clear_on_submit=True):
        name = st.text_input("Game Name")
        price = st.number_input("Price ($)", min_value=0.0, format="%.2f")
        developers = st.text_input("Developers")
        publishers = st.text_input("Publishers")
        languages = st.text_input("Languages (comma-separated, up to 3)")
        genres = st.text_input("Genres (comma-separated, up to 3)")
        platforms = st.multiselect("Platforms", ["Windows", "Mac", "Linux"])
        submitted = st.form_submit_button("Add Game")

        if submitted:
            windows = "Windows" in platforms
            mac = "Mac" in platforms
            linux = "Linux" in platforms

            insert_query = """
                INSERT INTO games (Name, Price, Developers, Publishers, `Language 1`, `Genre 1`, Windows, Mac, Linux)
                VALUES (:name, :price, :developers, :publishers, :language_1, :genre_1, :windows, :mac, :linux);
            """
            params = {
                "name": name,
                "price": price,
                "developers": developers,
                "publishers": publishers,
                "language_1": languages.split(",")[0] if languages else None,
                "genre_1": genres.split(",")[0] if genres else None,
                "windows": windows,
                "mac": mac,
                "linux": linux,
            }
            try:
                with engine.connect() as conn:
                    conn.execute(text(insert_query), params)
                st.success(f"Game '{name}' added successfully!")
            except SQLAlchemyError as e:
                st.error(f"Error: {str(e)}")

# update operation
elif action == "Update Game":
    st.header("✏️ Update Game Information")
    game_id = st.number_input("Enter the AppID of the game to update", min_value=1, step=1)

    
    query = "SELECT * FROM games WHERE AppID = %s;"
    try:
        game_to_update = pd.read_sql(query, engine, params=(game_id,))
        if not game_to_update.empty:
            st.write("Editing Game:", game_to_update.iloc[0]["Name"])
            with st.form("update_game_form"):
                name = st.text_input("Game Name", value=game_to_update.iloc[0]["Name"])
                price = st.number_input("Price ($)", min_value=0.0, format="%.2f", value=game_to_update.iloc[0]["Price"])
                developers = st.text_input("Developers", value=game_to_update.iloc[0]["Developers"])
                publishers = st.text_input("Publishers", value=game_to_update.iloc[0]["Publishers"])
                submitted = st.form_submit_button("Update Game")

                if submitted:
                    update_query = """
                        UPDATE games
                        SET Name = %s, Price = %s, Developers = %s, Publishers = %s
                        WHERE AppID = %s;
                    """
                    params = (name, price, developers, publishers, game_id)
                    with engine.connect() as conn:
                        conn.execute(text(update_query), params)
                    st.success("Game updated successfully!")
        else:
            st.warning("No game found with the provided AppID.")
    except SQLAlchemyError as e:
        st.error(f"Error: {str(e)}")


# delete operation
elif action == "Delete Game":
    st.header("🗑️ Delete a Game")
    game_id = st.number_input("Enter the AppID of the game to delete", min_value=1, step=1)

    if st.button("Delete Game"):
        delete_query = "DELETE FROM games WHERE AppID = :game_id;"
        try:
            with engine.connect() as conn:
                result = conn.execute(text(delete_query), {"game_id": game_id})
                if result.rowcount > 0:
                    st.success("Game deleted successfully!")
                else:
                    st.warning("No game found with the provided AppID.")
        except SQLAlchemyError as e:
            st.error(f"Error: {str(e)}")
