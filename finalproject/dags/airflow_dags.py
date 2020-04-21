import datetime, logging
import configparser
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from . import generate_tables
# from .generate_tables import create_tables
from finalproject.dags.generate_tables import create_tables, drop_tables
from finalproject.dags.sql_scripts import HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT


def createTables():
    config = configparser.ConfigParser()
    config.read('connection.cfg')
    # logging.INFO("going to create tables"+DB_PORT)
    # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port=5439".format(HOST, DB_NAME, DB_USER, DB_PASSWORD))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    logging.info("done creating tables ****")


finalprojectDag = DAG(
    'finalprojectDag',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    schedule_interval="@daily")

createTablesTask = PythonOperator(
    task_id="createTables",
    python_callable=createTables,
    dag=finalprojectDag
)