import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from datetime import datetime

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))

register_adapter(np.ndarray, addapt_numpy_array)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)


def process_song_file(cur, conn, filepath):
    
    # Get files
    df_song_files = []
    files = get_files(filepath)
    for file in files:
        #df_song_files.append(pd.read_json(file, typ='series'))
        file_opened = open(file).read()
        df_song_files.append(pd.DataFrame([json.loads(file_opened)]))
    # open song file
    df = pd.concat(df_song_files)
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(song_data.values)
    cur.executemany(song_table_insert, song_data)
    conn.commit()
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data.drop_duplicates(subset=['artist_id'], inplace=True)
    artist_data = list(artist_data.values)
    cur.executemany(artist_table_insert, artist_data)
    conn.commit()


def split_date(row):
    date = datetime.fromtimestamp(row['ts']/1000.0)
    row['ts'] = row['ts']/1000.0
    row['hour'] = date.hour
    row['day'] = date.day
    row['month'] = date.month
    row['year'] = date.year
    row['week'] = date.isocalendar()[1]
    row['weekday'] = date.weekday()
    return row

def process_log_file(cur, conn, filepath):
    
    # Get files
    df_log_files = []
    files = get_files(filepath)
    for file in files:
        file_opened = open(file).read()
        df_log_files.append(pd.read_json(file_opened, lines=True))
    
    # open log file
    df = pd.concat(df_log_files)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df[['ts']]
    t.drop_duplicates(inplace=True)
    
    # insert time data records
    time_df = t.apply(lambda row: split_date(row), axis=1)
    time_data = list(time_df.values)
    cur.executemany(time_table_insert, time_data)
    conn.commit()

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df['userId'] = user_df['userId'].astype(int)
    user_df.drop_duplicates(subset=['userId'], inplace=True)

    # insert user records
    user_data = list(user_df.values)
    cur.executemany(user_table_insert, user_data)
    conn.commit()

    # insert songplay records
    cur.execute(song_select)
    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df_results = pd.DataFrame(results, columns=columns)
    
    songplay_data = []
    for index, row in df.iterrows():
        
        #get songid and artistid from song and artist
        df = df_results[(df_results['title'] == row.song) & (df_results['name'] == row.artist)]
        if not df.empty:
            df_json = json.loads(df.to_json(orient='records'))
            df_json = df_json[0]
            songid = df_json.get('song_id')
            artistid = df_json.get('artist_id')
        else:
            songid, artistid = None, None
        
        songplay_data.append((row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent))
        
    cur.executemany(songplay_table_insert, songplay_data)
    conn.commit()

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_song_file(cur, conn, 'data/song_data')
    process_log_file(cur, conn, 'data/log_data')

    conn.close()


if __name__ == "__main__":
    main()