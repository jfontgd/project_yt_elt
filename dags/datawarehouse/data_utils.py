from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = 'yt_api'

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db')
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor

def close_conn_cursor(conn, cursor):
    conn.close()
    cursor.close()

def create_schema(schema):
    conn, cursor = get_conn_cursor()

    schema_sql =  f"Create SCHEMA IF NOT EXISTS {schema};"
    cursor.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn, cursor)

def create_table(schema, table):
    conn, cursor = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        
         "video_id" VARCHAR(255) PRIMARY KEY NOT NULL,
         "title"    VARCHAR(255) NOT NULL,
         "published_at" TIMESTAMP NOT NULL,
         "duration" VARCHAR(255) NOT NULL,
         "view_count" INTEGER,
         "like_count" INTEGER,
         "comment_count" INTEGER
        );
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
        
         "video_id" VARCHAR(255) PRIMARY KEY NOT NULL,
         "title"    VARCHAR(255) NOT NULL,
         "published_at" TIMESTAMP NOT NULL,
         "duration" VARCHAR(255) NOT NULL,
         "video_type" VARCHAR(255) NOT NULL,
         "view_count" INTEGER,
         "like_count" INTEGER,
         "comment_count" INTEGER
        );
        """   
    cursor.execute(table_sql)
    conn.commit()

    close_conn_cursor(conn, cursor)

def get_video_ids(cursor,schema):

    cursor.execute(f"""SELECT "video_id" FROM {schema}.{table};""")
    ids= cursor.fetchall()

    video_ids=[row['video_id'] for row in ids]
    
    return video_ids


        