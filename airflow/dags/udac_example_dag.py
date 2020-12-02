#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO description
TODO usage example
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

fd = open('/home/miguel/udacity/project_5/airflow/create_tables.sql','r')
create_tables_sql = fd.read()
fd.close()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
'''
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
'''
    'email_on_failure': False,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
#    schedule_interval='0 * * * *',
    schedule_interval=None,
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

create_tables_on_redshift = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift_credentials',
    sql=create_tables_sql
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
    json_path="s3://my-udacity-dend-sa-east-1/jsonpath/staging_log_data.jsonpath"
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
    json_path="s3://my-udacity-dend-sa-east-1/jsonpath/staging_song_data.jsonpath"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    delete_before_insert=True,
    to_table="songplays",
    sql_select=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_on_redshift
create_tables_on_redshift >> stage_events_to_redshift
create_tables_on_redshift >> stage_songs_to_redshift
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
