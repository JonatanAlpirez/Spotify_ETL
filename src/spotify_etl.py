import requests 
import base64
import datetime
import pandas as pd 
import sqlite3
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import urlencode


def get_token(client_id, client_secret, refresh_token):
    #To Get access token (this token has a time life: 1 hour)
    #args:
    #       client_id and client_secret: Spotify users credentials
    #       refresh_token: Refresh token
    #return: Access token, this token is used to get access to Spotify Api
    encoded_credentials = base64.b64encode(client_id.encode() + b':' + client_secret.encode()).decode("utf-8")

    token_headers = {
        "Authorization": "Basic " + encoded_credentials,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    token_data = {
        "refresh_token": refresh_token,
        "grant_type": 'refresh_token'
    }

    r = requests.post("https://accounts.spotify.com/api/token", data=token_data, headers=token_headers)
    
    return  r.json()["access_token"]

def get_songs(token):
    #To get songs data from Spotify Api (this token has a time life: 1 hour)
    #args:
    #       token: access token
    #return: data, It's a json object with all songs data played last 24 hrs.
    headers = {
        "Content-Type": "application/json",
        "Authorization" : "Bearer " + token
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp())*1000

    response = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix}&limit=50", headers = headers)
    
    data = response.json()
    return data

def check_if_valid_data(df: pd.DataFrame) -> bool:
    #To check and validate data extracted. 
    #args:
    #       df: DataFrame to analyze
    #return: true/false. data extracted integrity.
    
    # Check if DataFrame is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at_list']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["date"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

def main_spotify_etl():
    client_id = "4eac65e0b3bc4a308eaca84d724f82c7"
    client_secret = "8a4f297978b6424ca0b21195c3209dd8"

    refresh_token = 'AQBJgjZNuzQCYQ8d3_uurglz2BMAyze9q4Onr3h7CDRJikmynPS_PHvcwl9ZP3UsEi4EJ6pAEa1iCMSNr6v2ZFaxYjZiLn4lfo3aYp9NymYT15jb641wGyqMAPXsCUzsf9E'

    token = get_token(client_id, client_secret, refresh_token)
    data = get_songs(token)

    song_names = []
    artist_names = []
    duration_ms = []
    played_at_list = []
    album_name = []
    album_image = []
    explicit = []
    popularity = []
    date = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        duration_ms.append(song["track"]["duration_ms"])
        played_at_list.append(song["played_at"])
        album_name.append(song["track"]["album"]["name"])
        album_image.append(song["track"]["album"]["images"][0]["url"])
        explicit.append(song["track"]["explicit"])
        popularity.append(song["track"]["popularity"])
        date.append(song["played_at"][0:10])

    song_dict = {
            "song_name" : song_names,
            "artist_name": artist_names,
            "duration_ms" : duration_ms,
            "played_at_list" : played_at_list,
            "album_name" : album_name,
            "album_image" : album_image,
            "explicit" : explicit,
            "popularity": popularity,
            "date" : date
        }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "duration_ms", "played_at_list", "album_name", "album_image", "explicit", "popularity", "date"])

    # Load

    engine = sqlalchemy.create_engine("sqlite:///my_played_tracks.sqlite")
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            duration_ms VARCHAR(200),
            played_at_list VARCHAR(200),
            album_name VARCHAR(200),
            album_image VARCHAR(200),
            explicit VARCHAR(200),
            popularity VARCHAR(200),
            date VARCHAR(200)
        )
        """
    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", con=engine, index=False, if_exists='append')
        print("Done")
    except SQLAlchemyError as err:
            print(err)

    conn.close()
    engine.dispose()
    print("Close database successfully")

    return 0