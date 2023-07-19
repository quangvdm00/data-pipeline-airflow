from datetime import datetime, timedelta
import pendulum
import os

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from helpers.sql_statements import SqlQueries

default_args = {
    'owner': 'QuangDinhMinhVo',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    # Create staging_events table
    create_staging_events_task = PostgresOperator(
        task_id="create_staging_events",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.staging_events (
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
        """,
        postgres_conn_id="redshift"
    )

    # Create staging_songs table
    create_staging_songs_task = PostgresOperator(
        task_id="create_staging_songs",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(256),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(256),
                song_id varchar(256),
                title varchar(256),
                duration numeric(18,0),
                "year" int4
        );
        """,
        postgres_conn_id="redshift"
    )

    # Create songplays table
    create_songplays_fact_task = PostgresOperator(
        task_id="create_fact_songplays_table",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.songplays (
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
        """,
        postgres_conn_id="redshift"
    )

    # Create dimension artists table
    create_artist_dim_task = PostgresOperator(
        task_id="create_dim_artist",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.artists (
            	artistid varchar(256) NOT NULL,
            	name varchar(256),
                location varchar(256),
                lattitude numeric(18,0),
                longitude numeric(18,0)
        );
        """,
        postgres_conn_id="redshift"
    )

    # Create dimension songs table
    create_song_dim_task = PostgresOperator(
        task_id="create_dim_song",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.songs (
                songid varchar(256) NOT NULL,
                title varchar(256),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );
        """,
        postgres_conn_id="redshift"
    )

    # Create dimension time table
    create_time_dim_task = PostgresOperator(
        task_id="create_dim_time",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.time (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
        """,
        postgres_conn_id="redshift"
    )

    # Create dimension users table
    create_users_dim_task = PostgresOperator(
        task_id="create_dim_users",
        sql="""            
            CREATE TABLE IF NOT EXISTS public.users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
        """,
        postgres_conn_id="redshift"
    )

    # load stage_events data
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_path="s3://udacity-dend/log_data",
        json_path="s3://udacity-dend/log_json_path.json"
    )

    # load stage_songs data
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_path="s3://udacity-dend/song_data",
        json_path="auto"
    )

    # load songplays table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        load_sql_statement=SqlQueries.songplay_table_insert
    )

    # load data to user dimenstion table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        load_sql_statement=SqlQueries.user_table_insert,
        truncate_table=True
    )

    # load data to song dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        load_sql_statement=SqlQueries.song_table_insert,
        truncate_table=True
    )

    # load data to artist dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        load_sql_statement=SqlQueries.artist_table_insert,
        truncate_table=True
    )

    # load data to time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        load_sql_statement=SqlQueries.time_table_insert,
        truncate_table=True
    )

    # run data quality check
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'songs', 'artists', 'time', 'users']
    )

    end_operator = DummyOperator(task_id='End_execution')

    # DAGs RELATIONSHIP
    start_operator >> create_staging_events_task
    start_operator >> create_staging_songs_task

    create_staging_events_task >> stage_events_to_redshift
    create_staging_songs_task >> stage_songs_to_redshift

    stage_events_to_redshift >> create_songplays_fact_task
    stage_songs_to_redshift >> create_songplays_fact_task

    create_songplays_fact_task >> load_songplays_table

    load_songplays_table >> create_artist_dim_task
    load_songplays_table >> create_song_dim_task
    load_songplays_table >> create_time_dim_task
    load_songplays_table >> create_users_dim_task

    create_users_dim_task >> load_user_dimension_table
    create_song_dim_task >> load_song_dimension_table
    create_artist_dim_task >> load_artist_dimension_table
    create_time_dim_task >> load_time_dimension_table

    load_artist_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()