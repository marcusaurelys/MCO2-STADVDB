import streamlit as st
import pandas as pd
import requests
import traceback
 
url = st.secrets['url']

st.set_page_config(page_title="Steam Games Management System", page_icon=":video_game:", layout="wide")
st.title("🎮 Steam Games Management System")
st.markdown('<style> div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)

#Sidebar
action = st.sidebar.selectbox("Choose an action", ["View Games", "Search Game", "Add Game", "Update Game", "Delete Game"])

# read operation
if action == "View Games":
    st.header("View All Games")
    query = """
        SELECT `AppID`, `Name`, `Price`, `Developers`, `Publishers`, `Language 1`, `Language 2`, `Language 3`, 
               `Genre 1`, `Genre 2`, `Genre 3`, `Windows`, `Mac`, `Linux`
        FROM games;
    """
    try:
        endpoint = url + '/select'
        data = {
            "query": query,
            "target_node": "node1"
        }
        response = requests.post(endpoint, json=data)
        response = response.json()

        if response['status'] != "success":
            raise Exception(response['message'])

        games_df = pd.DataFrame(response['results'])
        games_df = games_df[['AppID', 'Name', 'Price', 'Developers', 'Publishers', 'Language 1', 'Language 2', 'Language 3', 'Genre 1', 'Genre 2', 'Genre 3', 'Windows', 'Mac', 'Linux']]
        st.dataframe(games_df)
    except Exception as e:
        st.error(f"Error: {str(e)}")
    
elif action == 'Search Game':
    st.header("🔎 Search Game")
    game_id = st.number_input("Enter the AppID of the game to update", min_value=1, step=1)
    query = "SELECT * FROM games WHERE AppID = %s;"

    try:
        endpoint = url + '/select'
        data = {
            "query": query,
            "params": game_id,
            "target_node": "node1"
        }
        response = requests.post(endpoint, json=data)
        response = response.json()

        if response['status'] != "success":
            raise Exception(response['message'])

        game_to_view = pd.DataFrame(response['results'])
        
        if game_to_view.empty:
            st.warning("No game found with the provided AppID.")
        else:
            st.dataframe(game_to_view)

    except Exception as e:
        st.error(f"Error: {str(e)}")
     


#create operation
elif action == "Add Game":
    st.header("➕ Add a New Game")
    with st.form("add_game_form", clear_on_submit=True):
        id = st.number_input("App ID", step=1, value=0)
        name = st.text_input("Game Name")
        price = st.number_input("Price ($)", min_value=0.0, format="%.2f")
        developers = st.text_input("Developers")
        publishers = st.text_input("Publishers")
        languages = st.text_input("Languages (comma-separated, up to 3)")
        genres = st.text_input("Genres (comma-separated, up to 3)")
        platforms = st.multiselect("Platforms", ["Windows", "Mac", "Linux"])
        release_date = st.date_input("Release Date")
        submitted = st.form_submit_button("Add Game")

        if submitted:
            windows = "Windows" in platforms
            mac = "Mac" in platforms
            linux = "Linux" in platforms

            query = """
                INSERT INTO games (AppID, Name, Price, Developers, Publishers, `Language 1`, `Language 2`, `Language 3`, `Genre 1`, `Genre 2`, `Genre 3`, Windows, Mac, Linux, `Release date`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            params = {
                "id" : id,
                "name": name,
                "price": price,
                "developers": developers,
                "publishers": publishers,
                "language_1": languages.split(",")[0] if languages else None,
                "language_2": languages.split(",")[1] if len(languages.split(",")) >= 2 else None,
                "language_3": languages.split(",")[2] if len(languages.split(",")) >= 3 else None,
                "genre_1": genres.split(",")[0] if genres else None,
                "genre_2": genres.split(",")[1] if len(genres.split(",")) >= 2 else None,
                "genre_3": genres.split(",")[2] if len(genres.split(",")) >= 3 else None,
                "windows": windows,
                "mac": mac,
                "linux": linux,
                "release_date": release_date
            }

            try:
                endpoint = url + '/write'
                tx1 = {}
                tx2 = {}
                release_year = params['release_date'].year
                params['release_date'] = params['release_date'].isoformat()
                if release_year < 2020:
                    tx1 = {
                        "query": query,
                        "params": params,
                        "target_node": "node1"
                    }

                    tx2 = {
                        "query": query,
                        "params": params,
                        "target_node": "node2"
                    }
                else:
                    tx1 = {
                        "query": query,
                        "params": params,
                        "target_node": "node1"
                    }

                    tx2 = {
                        "query": query,
                        "params": params,
                        "target_node": "node3"
                    }

                transactions = [tx1, tx2]
                
                response = requests.post(endpoint, json=transactions)
                response = response.json()

                if response['status'] != "queued":
                    raise Exception(response['message'])
            
                st.success(f"Game '{name}' added to queue!")
            except Exception as e:
                print(traceback.format_exc())
                st.error(f"Error: {str(e)}")

# update operation
elif action == "Update Game":
    st.header("✏️ Update Game Information")
    game_id = st.number_input("Enter the AppID of the game to update", min_value=1, step=1)
    platform_options = ["Windows", "Mac", "Linux"]
    
    query = "SELECT * FROM games WHERE AppID = %s;"
    try:
        endpoint = url + '/select'
        data = {
            "query": query,
            "params": game_id,
            "target_node": "node1"
        }
        response = requests.post(endpoint, json=data)
        response = response.json()

        if response['status'] != "success":
            raise Exception(response['message'])

        game_to_update = pd.DataFrame(response['results'])
     
        if not game_to_update.empty:
            st.write("Editing Game:", game_to_update.iloc[0]["Name"])
            game_to_update['Price'] = pd.to_numeric(game_to_update['Price'], errors='coerce')
            with st.form("update_game_form"):
                name = st.text_input("Game Name", value=game_to_update.iloc[0]["Name"])
                price = st.number_input("Price ($)", min_value=0.0, format="%.2f", value=game_to_update.iloc[0]["Price"])
                developers = st.text_input("Developers", value=game_to_update.iloc[0]["Developers"])
                publishers = st.text_input("Publishers", value=game_to_update.iloc[0]["Publishers"])
                language_1 = st.text_input("Language 1", value=game_to_update.iloc[0]["Language 1"])
                language_2 = st.text_input("Language 2", value=game_to_update.iloc[0]["Language 2"])
                language_3 = st.text_input("Language 3", value=game_to_update.iloc[0]["Language 3"])
                genre_1 = st.text_input("Genre 1", value=game_to_update.iloc[0]["Genre 1"])
                genre_2 = st.text_input("Genre 2", value=game_to_update.iloc[0]["Genre 2"])
                genre_3 = st.text_input("Genre 3", value=game_to_update.iloc[0]["Genre 3"])
                platforms = st.multiselect("Platforms", platform_options, [name for name in platform_options if game_to_update.iloc[0][name]])
                submitted = st.form_submit_button("Update Game")

                if submitted:
                    query = """
                        UPDATE games
                        SET Name = %s, Price = %s, Developers = %s, Publishers = %s, Windows = %s, Mac = %s, Linux = %s, `Language 1`= %s, `Language 2` = %s, `Language 3` = %s, `Genre 1` = %s, `Genre 2` = %s, `Genre 3` = %s
                        WHERE AppID = %s;       
                    """

                    windows = "Windows" in platforms
                    mac = "Mac" in platforms
                    linux = "Linux" in platforms
                    
                    try:
                        endpoint = url + '/write'
                        tx1 = {}
                        tx2 = {}

                        params = {
                            "name": name,
                            "price": price,
                            "developers": developers,
                            "publishers": publishers,
                            "windows": windows,
                            "mac": mac,
                            "linux": linux,
                            "language_1": language_1,
                            "language_2": language_2,
                            "language_3": language_3,
                            "genre_1": genre_1,
                            "genre_2": genre_2,
                            "genre_3": genre_3,
                            "AppID": game_id
                        }

                        game_to_update['Release date'] = pd.to_datetime(game_to_update['Release date'], errors='coerce').dt.year
                        if game_to_update.iloc[0]['Release date'] < 2020:
                            tx1 = {
                                "query": query,
                                "params": params,
                                "target_node": "node1"
                            }

                            tx2 = {
                                "query": query,
                                "params": params,
                                "target_node": "node2"
                            }
                        else:
                            tx1 = {
                                "query": query,
                                "params": params,
                                "target_node": "node1"
                            }

                            tx2 = {
                                "query": query,
                                "params": params,
                                "target_node": "node3"
                            }

                        transactions = [tx1, tx2]
                
                        response = requests.post(endpoint, json=transactions)
                        response = response.json()

                        if response['status'] != "queued":
                            raise Exception(response['message'])

                        st.success("Game update added to queue!")
                        
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        else:
            st.warning("No game found with the provided AppID.")
    except Exception as e:
        st.error(f"Error: {str(e)}")

                                 
# delete operation
elif action == "Delete Game":
    st.header("🗑️ Delete a Game")
    game_id = st.number_input("Enter the AppID of the game to delete", min_value=1, step=1)
    game_to_delete = None
    query = "SELECT * FROM games WHERE AppID = %s;"
    
    try:
        endpoint = url + '/select'
        data = {
            "query": query,
            "params": game_id,
            "target_node": "node1"
        }
        response = requests.post(endpoint, json=data)
        response = response.json()

        if response['status'] != "success":
            raise Exception(response['message'])

        game_to_delete = pd.DataFrame(response['results'])
        
    except Exception as e:
         st.error(f"Error: {str(e)}")
    
    if st.button("Delete Game"):
        query = "DELETE FROM games WHERE AppID = %s"
        try:
            if game_to_delete.empty:
                raise ValueError()

            game_to_delete['Release date'] = pd.to_datetime(game_to_delete['Release date'], errors='coerce').dt.year
            endpoint = url + '/write'
            tx1 = {}
            tx2 = {}
            params = {
                "AppID": game_id
            }
            
            if game_to_delete.iloc[0]['Release date'] < 2020:
                tx1 = {
                    "query": query,
                    "params": params,
                    "target_node": "node1"
                }

                tx2 = {
                    "query": query,
                    "params": params,
                    "target_node": "node2"
                }
            else:
                tx1 = {
                    "query": query,
                    "params": params,
                    "target_node": "node1"
                }

                tx2 = {
                    "query": query,
                    "params": params,
                    "target_node": "node3"
                }

            transactions = [tx1, tx2]
    
            response = requests.post(endpoint, json=transactions)
            response = response.json()

            if response['status'] != "queued":
                raise Exception(response['message'])

            st.success("Game queued for deletion")

        except ValueError as e:
            st.warning("No game found with the provided AppID.")
        except Exception as e:
            st.error(f"Error: {str(e)}")
