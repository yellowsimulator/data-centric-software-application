"""
Implemets databases operations such as 
- creates database
- creates table
- inssert rows into a table
- drop a table
Uses local host by default.
"""

import psycopg2 as psg
from sql_queries import create_table_queries
from sql_queries import drop_table_queries


def connect_to_database():
    """Creates a connection.
    """
    conn = psg.connect(host='127.0.0.1', dbname='music_library', \
                 user='postgres', password='admin')
    cursor = conn.cursor()
    return cursor, conn


def create_database(database_name: str, username: str, password: str):
    """Creates a database

    Args:
        database_name: the target database name. 
        username: the username
        password: the password
    """
    conn = psg.connect(host='127.0.0.1', user='postgres', password='admin')
    cursor = conn.cursor()
    conn.set_session(autocommit=True)
    cursor.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cursor.execute(f'CREATE DATABASE {database_name} WITH ENCODING "utf8" TEMPLATE template0')
    conn.close()


def create_table(conn, cursor):
    for query in create_table_queries:
        cursor.execute(query)
        conn.commit()
    conn.close()


def drop_tables(conn, cursor):
    for query in drop_table_queries:
        cursor.execute(query)
        conn.commit()
    conn.close()


# def run_task(task='create_tables'):
#     """[summary]

#     Args:
#         task (str, optional): [description]. Defaults to 'create_tables'.
#     """
#     cursor, conn = connect_to_database()
#     tasks = {'create_tables': create_table(conn, cursor), \
#              'drop_tables': drop_tables(conn, cursor)}
#     tasks[task]



if __name__ == '__main__':
    database_name = 'music_library'
    username = 'postgres'
    password = 'admin'
    #create_database(database_name, username, password)
    create_table()