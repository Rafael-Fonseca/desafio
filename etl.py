import os
import glob
import pandas as pd
import datetime
from sql_queries import *
from create_tables import *


def data_to_insert(list_columns, list_values):
    data = dict(zip(list_columns, list_values))
    tuple_query = insert_query(**data)
    return tuple_query


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def process_song_file(file):
    """Process song file.
    
    Args:
        file: song file path (json).
    """
    # open song file
    df = pd.read_json(file, lines=True)

    # insert song record
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    values = df[columns].values.flatten().tolist()
    insert_in_table('songs', data_to_insert(columns, values))

    # insert artist record
    columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    values = df[columns].values.flatten().tolist()
    insert_in_table('artists', data_to_insert(columns, values))


def divide(dividend):
    return dividend / 1000


def process_log_file(file):
    """Process log file.
    
    Args:
        file: log file path (json).
    """
    # open log file
    df = pd.read_json(file, lines=True)

    # filter by NextSong action
    filtered_df = df.query('page == "NextSong"')
    df = None  # free memory

    # convert timestamp column to datetime
    transformed_df = filtered_df.assign(ts=filtered_df.ts.apply(divide).apply(datetime.datetime.fromtimestamp))
    filtered_df = None  # free memory

    # insert data in tables time, users and songplays
    for index, row in transformed_df.iterrows():
        # insert time data records
        columns = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
        date = row['ts']
        values = [date.timestamp() * 1000, date.hour, date.day, date.week, date.month, date.year, date.weekday()]
        insert_in_table('time', data_to_insert(columns, values))

        # insert user data records
        columns = ['userId', 'firstName', 'lastName', 'gender', 'level']  # Like json file
        values = row[columns].values.flatten().tolist()
        columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']  # Like postgres table
        insert_in_table('users', data_to_insert(columns, values))

        # get song and artists ids
        columns = ['song', 'artist']
        values = row[columns].values.flatten().tolist()
        ids = get_song_artist_ids(values)

        columns = ['userId', 'level', 'sessionId', 'location', 'userAgent']  # Like json file
        values = row[columns].values.flatten().tolist()

        # columns like psql table
        columns = ['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']

        # values adapt to columns like psql table
        values.insert(0, date.timestamp() * 1000)
        if ids is not None:
            values.insert(3, ids[0])  # Like psql table
            values.insert(4, ids[1])  # Like psql table
        else:
            values.insert(3, 'NULL')
            values.insert(4, 'NULL')

        # insert songplay records
        insert_in_table('songplays', data_to_insert(columns, values))


def process_data(filepath, func):
    """Process data.
    
    Args:
        filepath: directory path of data.
        func: function of processing.
    """
    # get all files matching extension from directory
    files = get_files(filepath)

    # get total number of files found
    # total = len(files)

    # iterate over files and process
    for file in files:
        func(file)


def main():
    process_data(filepath='data/song_data', func=process_song_file)
    process_data(filepath='data/log_data', func=process_log_file)


if __name__ == "__main__":
    main()
