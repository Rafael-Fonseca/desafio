from psycopg2 import sql


# DROP TABLES
def drop_table(table_name):
    return sql.SQL(f'drop table if exists {table_name};')


def songplay_table_drop():
    return drop_table('songplays')


def user_table_drop():
    return drop_table('users')


def song_table_drop():
    return drop_table('songs')


def artist_table_drop():
    return drop_table('artists')


def time_table_drop():
    return drop_table('time')


# CREATE TABLES


def create_table(con, name_table, *attributes):
    # Create table statement
    table_name = sql.Identifier(name_table).as_string(con)
    attrbs = ', '.join(*attributes)

    return sql.SQL(f'create table if not exists {table_name} ({attrbs});')


def songplay_table_create(con):
    table_name = 'songplays'
    attributes = [
        'songplay_id SERIAL PRIMARY KEY NOT NULL',
        'start_time bigint NOT NULL',
        'user_id varchar(100) NOT NULL',
        'song_id varchar(100)',
        'session_id int NOT NULL',
        'location varchar(100)',
        'user_agent varchar(256)'
    ]
    return create_table(con, table_name, attributes)


def user_table_create(con):
    table_name = 'users'
    attributes = [
        'user_id varchar(100) PRIMARY KEY NOT NULL',
        'first_name varchar(100) NOT NULL',
        'last_name varchar(100) NOT NULL',
        'gender varchar(10)',
        'level varchar(10) NOT NULL'
    ]
    return create_table(con, table_name, attributes)


def song_table_create(con):
    table_name = 'songs'
    attributes = [
        'song_id varchar(100) PRIMARY KEY NOT NULL',
        'title varchar(100) NOT NULL',
        'artist_id varchar(100) NOT NULL',
        'year int',
        'duration double precision NOT NULL'
    ]
    return create_table(con, table_name, attributes)


def artist_table_create(con):
    table_name = 'artists'
    attributes = [
        'artist_id varchar(100) PRIMARY KEY NOT NULL',
        'artist_name varchar(100) NOT NULL',
        'artist_location varchar(100)',
        'artist_latitude double precision',
        'artist_longitude double precision'
    ]
    return create_table(con, table_name, attributes)


def time_table_create(con):
    table_name = 'time'
    attributes = [
        'start_time bigint PRIMARY KEY NOT NULL',
        'hour int NOT NULL',
        'day int NOT NULL',
        'week int NOT NULL',
        'month int NOT NULL',
        'year int NOT NULL',
        'weekday int NOT NULL'
    ]
    return create_table(con, table_name, attributes)


# INSERT RECORDS
def insert_query(**kwargs):
    columns = ''
    values = ''
    strings = []
    for key, value in kwargs.items():
        columns += key + ', '
        if isinstance(value, str):
            values += '%s, '
            strings.append(value)
        elif value != value:  # nan == nan get false, so is Null
            values += 'NULL, '
        else:
            values += str(value) + ', '

    # Exclude final comma and space in >> 'string, in, columns, or, values, '
    columns = columns[:-2]
    values = values[:-2]

    return columns, values, strings


# FIND SONGS
def find_songs_query(search):
    title, artist = search
    """
    In release 1 query = sql.SQL(
    '''SELECT song_id, sg.artist_id FROM songs sg INNER JOIN artists art ON sg.artist_id = art.artist_id
            WHERE title = {} AND artist_name = {}''').......
    """
    query = sql.SQL("""SELECT song_id FROM songs sg INNER JOIN artists art ON sg.artist_id = art.artist_id
            WHERE title = {} AND artist_name = {}""").format(
        sql.Literal(title),
        sql.Literal(artist))
    return query


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
