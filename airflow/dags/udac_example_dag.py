#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This file contains an AirFlow DAG to load data from an S3 bucket into Redshift tables. The steps taken are as follows:

  1. Create the tables on Redshift if they don't yet exist.
  2. Copy the data from the S3 bucket to 2 staging tables on Redshift.
  3. Copy the facts to a table on Redshift.
  4. Copy the dimensions to 4 tables on Redshift.
  5. Check the quality of the data copied.

The configurations are loaded from `dp.cfg`, where the S3 bucket name and folders are stored.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from helpers import SqlQueries
import configparser

CFG_FILE = '/home/miguel/udacity/project_5/dp.cfg'
config = configparser.ConfigParser()
config.read_file(open(CFG_FILE))

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift_credentials',
    table='staging_events',
    columns='userid, ts, artist, firstname, lastname, gender, length, song, level, sessionid, location, useragent, page',
    s3_bucket=config.get('S3','bucket'),
    s3_key=config.get('S3','log_folder'),
    json_path=config.get('S3','log_jsonpath')
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift_credentials',
    table='staging_songs',
    columns='song_id, artist_id, artist_latitude, artist_longitude, artist_location, title, year, duration, artist_name',
    s3_bucket=config.get('S3','bucket'),
    s3_key=config.get('S3','song_folder'),
    json_path=config.get('S3','song_jsonpath')
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    to_table="songplays",
    sql_select=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="users",
    sql_select=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="songs",
    sql_select=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="artists",
    sql_select=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="time",
    sql_select=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    sql_queries=SqlQueries.select_nulls_count,
    expected_values=[ [(0,)] for x in SqlQueries.select_nulls_count ]
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
