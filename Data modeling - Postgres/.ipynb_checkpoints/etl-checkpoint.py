import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    """
    Through the cursor search, transform and insert the first record and its variables on 
    json files contained in song_data folder.
    
    Parameters
    ----------
    cur : Psycopg2 cursor
          This function help us how a structure control to over pass the records from database.   
    filepath: String
              Indicate the rout to get first record in JSON format of the music datasets.  
    
    """
    df = pd.read_json(filepath, lines=True)

    # insert song record
    for index, row in df.iterrows():
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
        try:
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e:
            print("Error: Inserting row for table: songs")
            print (e)
    
    # insert artist record
    for index, row in df.iterrows():
        artist_data = (row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        try:
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e:
            print("Error: Inserting row for table: artists")
            print (e)


def process_log_file(cur, filepath):
    # open log file
    """
    Through the cursor search, transform and insert the first record and its variables on the json files
    contained in log_data folder.
    
    Parameters
    ----------
    cur : Psycopg2 cursor
          This function help us how a structure control to over pass the records from database.   
    filepath: String
              Indicate the route to get the first record in JSON format of the music datasets.      
    """
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df.ts, unit='ms')
    t = df.copy()
    
    # insert time data records
    time_data = (t.ts, t.ts.dt.hour, t.ts.dt.day, t.ts.dt.weekofyear, t.ts.dt.month, t.ts.dt.year, t.ts.dt.weekday)
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame({
                            'timestamp': time_data[0],
                            'hour': time_data[1],
                            'day': time_data[2],
                            'week of year': time_data[3],
                            'month': time_data[4],
                            'year': time_data[5],
                            'weekday': time_data[6]
                            })

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName','gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    """
    Through the list, you can get all extension files, how many files there are.
    
    Parameters
    --------------
    cur: Psycopg2 cursor
         This function help us how a structure control to over pass the records from database.   
    conn: Psycopg2 objetc
          Create the conexion to connect and create a new database session.  
    filepath: String
              Indicate the route to get the first record in JSON format of the music datasets.
    func: Function (?)
          Get the cursor and the pivot to iterate along the allfiles list elements.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()