import sqlite3
import sqlalchemy
import pandas as pd 
from sqlalchemy.exc import SQLAlchemyError

ejemplo = pd.read_csv('file_name.csv')  
print(ejemplo)


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
    ejemplo.to_sql("my_played_tracks", con=engine, index=False, if_exists='append')
    print("Done")
except SQLAlchemyError as err:
        print(err)

conn.close()
engine.dispose()
print("Close database successfully")