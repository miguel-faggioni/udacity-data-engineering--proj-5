- [Data Pipelines - Udacity](#org020220d)
  - [Introduction](#orgb00a15a)
- [Folder structure](#org4e5434c)
- [Airflow](#org2f21557)
  - [Installation](#orga151b1e)
  - [Running](#org72fc91d)
- [Usage](#orgc32b383)


<a id="org020220d"></a>

# Data Pipelines - Udacity

This repository is intended for the the fifth project of the Udacity Data Engineering Nanodegree Programa: Data Pipelines.

The introduction was taken from the Udacity curriculum, since they summarize the activity better than I could.


<a id="orgb00a15a"></a>

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


<a id="org4e5434c"></a>

# Folder structure

```
/
├── airflow
│   ├── create_tables.sql - SQL queries to create the necessary tables on Redshift
│   ├── dags - Airflow DAGs for the project
│   │   └── udac_example_dag.py - DAG for the project
│   └── plugins - custom Airflow plugins for the project
│       ├── helpers
│       │   └── sql_queries.py - all the SQL queries needed to run the project
│       └── operators - custom Airflow operators for the project
│           ├── data_quality.py - DataQualityOperator
│           ├── load_dimension.py - LoadDimensionOperator
│           ├── load_fact.py - LoadFactOperator
│           └── stage_redshift.py - StageToRedshiftOperator
├── jsonpath - folder containing the jsonpath for copying data from S3 to Redshift
│   ├── staging_log_data.jsonpath
│   └── staging_song_data.jsonpath
├── README.md - this file in markdown
├── README.org - this file in orgmode
├── dp.py - code to parse the dp.cfg file and create the necessary AWS resources
└── dp.cfg - config file with the requirements for the AWS resources to be used and for the DAG to run
```


<a id="org2f21557"></a>

# Airflow


<a id="orga151b1e"></a>

## Installation

In order to install Airflow locally, the following command must be run:

```bash
$ pip install apache-airflow==1.10.12 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.8.txt"
```

Then update the file \`~/airflow/airflow.cfg\` and set \`dags<sub>folder</sub>\` to the dags folder of the project, and set \`plugins<sub>folder</sub>\` to the plugins folder of the project. And run:

```bash
$ airflow initdb
```

Optionally you can set \`load<sub>examples</sub>\` to False before running \`airflow initdb\`, to show only the DAGs related to the project.


<a id="org72fc91d"></a>

## Running

After installing Airflow, run the following command:

```bash
$ airflow scheduler
```

And in another terminal:

```bash
$ airflow webserver
```

These will start the Airflow scheduler and webserver, respectively. The Airflow webserver will be available on \`<http://localhost:8080/>\`


<a id="orgc32b383"></a>

# Usage

After starting the AirFlow server, go to the AirFlow webserver and turn on the \`Udacity - Data Engineering - Data Pipelines with Airflow\` DAG. The DAG is configured retry at most 3 times when a task fails, and with a 5 minute interval between retries.

The DAG will start running shortly in order to backfill its runs, starting on data from 12/1/2019, which is defined on \`./airflow/dags/udac<sub>example</sub><sub>dag.py</sub>\`.

When running, the DAG will:

1.  Create the necessary tables on Redshift if they don't yet exist.
2.  Copy the data from the S3 bucket to 2 staging tables on Redshift.
3.  Copy the facts to a table on Redshift.
4.  Copy the dimensions to 4 tables on Redshift.
5.  Check the quality of the data copied.

The quality check verifies that the fact and dimension tables have no \`null\` values on their id columns.