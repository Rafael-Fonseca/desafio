import psycopg2
from psycopg2 import sql, errors

from sql_queries import create_table_queries, drop_table_queries, find_songs_query


def connect(db_name='sparkifydb',
            user='student',
            password='student',
            autocommit=False):
    # Create sql query
    query = f'dbname={db_name} user={user} password={password}'

    # Connect to the PostgreSQL database server
    con = psycopg2.connect(query)
    if autocommit:
        con.autocommit = True

    # Get cursor object from the database connection
    cur = con.cursor()

    return con, cur


def disconnect(con, cur):
    cur.close()
    con.close()


def drop_database(db_name='sparkifydb'):
    con, cur = connect('postgres', 'student', 'student', True)
    cur.execute(sql.SQL(f"""DROP DATABASE IF EXISTS {db_name}"""))
    disconnect(con, cur)


def create_database(db_name='sparkifydb'):
    # connect to default database
    con, cur = connect('postgres', 'student', 'student', True)

    # create sparkify database with UTF8 encoding
    cur.execute(sql.SQL(f"""CREATE DATABASE {db_name} ENCODING = 'UTF8'"""))

    # close connection to default database
    disconnect(con, cur)

    # connect to sparkify database
    # con, cur = connect()  # I prefer call connect when it's necessary


def drop_tables():
    con, cur = connect()
    for query in drop_table_queries:
        cur.execute(query())
        con.commit()
    disconnect(con, cur)


def create_tables():
    con, cur = connect()
    for query in create_table_queries:
        cur.execute(query(con))
        con.commit()
    disconnect(con, cur)


def insert_in_table(table_name, tuple_query):
    columns, values, strings = tuple_query
    con, cur = connect()

    try:
        if len(strings) > 0:
            cur.execute(
                sql.SQL("""INSERT INTO {} ({}) VALUES ({})""").format(
                    sql.Identifier(table_name),
                    sql.SQL(columns),
                    sql.SQL(values),
                ),
                strings
            )
        else:
            cur.execute(
                sql.SQL("""INSERT INTO {} ({}) VALUES ({})""").format(
                    sql.Identifier(table_name),
                    sql.SQL(columns),
                    sql.SQL(values),
                )
            )
    except psycopg2.errors.UniqueViolation:
        # psycopg2 is telling that we are trying add replicated data, pass without add it.
        pass

    con.commit()
    disconnect(con, cur)


def get_song_artist_ids(search):
    con, cur = connect()
    cur.execute(find_songs_query(search))
    result = cur.fetchone()
    con.commit()
    disconnect(con, cur)
    return result


def main():
    drop_database()
    create_database()
    create_tables()
    # drop_tables()


if __name__ == "__main__":
    main()
