from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries


default_args = {
    'owner': 'mike_mora',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),   
    'email_on_retry': False,
    'catchup': False
}


with DAG('project_airflow_Mike_dag',
          start_date=datetime(2022, 9, 27),
          default_args = default_args,   
          template_searchpath='/home/workspace/airflow',
          schedule_interval = '@hourly') as dag:
    
    
    start_operator = DummyOperator(task_id='Begin_execution') 
    
    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql= 'create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id = 'Stage_events',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        region='us-west-2',
        table = 'staging_events',
        s3_bucket = "udacity-dend",
        s3_key = "log_data",
        copy_json_option="s3://udacity-dend/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id = 'Stage_songs',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        region='us-west-2',
        table = 'staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        copy_json_option='auto'
    )


    songplays_table_load = LoadFactOperator(
        task_id = 'Songplays_fact_table_load',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials_id ="aws_credentials",
        table = 'songplays',
        sql = SqlQueries.songplay_table_insert
    )

    users_table_load = LoadDimensionOperator(
        task_id = 'Users_dim_table_load',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials_id = "aws_credentials",
        table = 'users',
        sql = SqlQueries.user_table_insert
    )

    songs_table_load = LoadDimensionOperator(
        task_id = 'Songs_dim_table_load',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials_id = "aws_credentials",
        table = 'songs',
        sql = SqlQueries.song_table_insert
    )

    artist_table_load = LoadDimensionOperator(
        task_id = 'Artist_dim_table_load',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials = 'awsuser',
        table = 'artists',
        sql = SqlQueries.artist_table_insert
    )

    time_table_load = LoadDimensionOperator(
        task_id = 'Time_dim_table_load',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials = 'awsuser',
        table = 'time',
        sql = SqlQueries.time_table_insert
    )

    run_data_quality_checks = DataQualityOperator(
        task_id = 'Quality_check',
        dag = dag,
        redshift_conn_id = 'redshift',
        aws_credentials = 'awsuser',
        tables = ['songplays', 'users', 'songs', 'artists', 'time']

    )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

    start_operator >> create_trips_table
    create_trips_table >> [stage_events_to_redshift, stage_songs_to_redshift] >> songplays_table_load
    songplays_table_load >> [users_table_load, songs_table_load, artist_table_load, time_table_load] >> run_data_quality_checks
    run_data_quality_checks >> end_operator


