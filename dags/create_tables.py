from datetime import datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator


@dag(
    dag_id='create_tables',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    description='Create all tables in Redshift'
)
def create_tables():
    
    start = DummyOperator(task_id='start')
    
    # Create staging tables first
    create_staging_events = PostgresOperator(
        task_id='create_staging_events',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public.staging_events (
                artist varchar(256),
                auth varchar(256),
                firstname varchar(256),
                gender varchar(256),
                iteminsession int4,
                lastname varchar(256),
                length numeric(18,0),
                "level" varchar(256),
                location varchar(256),
                "method" varchar(256),
                page varchar(256),
                registration numeric(18,0),
                sessionid int4,
                song varchar(256),
                status int4,
                ts int8,
                useragent varchar(256),
                userid int4
            );
        """
    )
    
    create_staging_songs = PostgresOperator(
        task_id='create_staging_songs',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public.staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(512),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(512),
                song_id varchar(256),
                title varchar(512),
                duration numeric(18,0),
                "year" int4
            );
        """
    )
    
    staging_complete = DummyOperator(task_id='staging_complete')
    
    # Create fact table
    create_songplays = PostgresOperator(
        task_id='create_songplays',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public.songplays (
                playid varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                userid int4 NOT NULL,
                "level" varchar(256),
                songid varchar(256),
                artistid varchar(256),
                sessionid int4,
                location varchar(256),
                user_agent varchar(256),
                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            );
        """
    )
    
    fact_complete = DummyOperator(task_id='fact_complete')
    
    # Create dimension tables
    create_users = PostgresOperator(
        task_id='create_users',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public.users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            );
        """
    )
    
    create_songs = PostgresOperator(
        task_id='create_songs',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public.songs (
                songid varchar(256) NOT NULL,
                title varchar(512),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );
        """
    )
    
    create_artists = PostgresOperator(
        task_id='create_artists',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public.artists (
                artistid varchar(256) NOT NULL,
                name varchar(512),
                location varchar(512),
                lattitude numeric(18,0),
                longitude numeric(18,0)
            );
        """
    )
    
    create_time = PostgresOperator(
        task_id='create_time',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE public."time" (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            );
        """
    )
    
    end = DummyOperator(task_id='end')
    
    # Set dependencies - staging tables first, then fact, then dimensions
    start >> [create_staging_events, create_staging_songs] >> staging_complete
    staging_complete >> create_songplays >> fact_complete
    fact_complete >> [create_users, create_songs, create_artists, create_time] >> end


create_tables_dag = create_tables()

