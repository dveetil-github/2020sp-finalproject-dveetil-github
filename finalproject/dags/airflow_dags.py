import datetime, logging

import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from . import generate_tables
# from .generate_tables import create_tables
from finalproject.dags.generate_tables import create_tables, drop_tables
from finalproject.dags.sql_scripts import HOST, DB_NAME, DB_USER, DB_PASSWORD, data_stagg_qrys, data_transform_qrys

def connect_to_db(f):
    def dbConnection(*args, **kwargs):
        # connect to database
        conn = psycopg2.connect("host={} dbname={} user={} password={} port=5439".format(HOST, DB_NAME, DB_USER, DB_PASSWORD))
        try:
            fn = f(conn, *args, **kwargs)
        except Exception:
            conn.rollback()
            raise
        else:
            print("in else commit")
            conn.commit()
        finally:
            print("closing db connection")
            conn.close()

        return fn
    return dbConnection

@connect_to_db
def createTables(conn):

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    logging.info("done creating tables ****")

@connect_to_db
def dataStaging(conn):
    """
    this method will load the json data to staging tables
    :param conn:
    :return:
    """
    logging.info("data staging started")
    cur = conn.cursor()
    for query in data_stagg_qrys:
        logging.info("query---->" + query)
        cur.execute(query)
    logging.info("completed data staging tables")

@connect_to_db
def dataTransformation(conn):
    """
    this method will insert data to analytic tables
    :param cur:
    :param conn:
    :return:
    """
    logging.info("dataTransformation started")
    cur = conn.cursor()
    for query in data_transform_qrys:
        logging.info("insert query is -->"+query)
        cur.execute(query)
    logging.info("dataTransformation completed")


finalprojectDag = DAG(
    'finalprojectDag',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    # start_date=datetime.datetime.now(),
    schedule_interval="@daily")

createTablesTask = PythonOperator(
    task_id="createTables",
    python_callable=createTables,
    dag=finalprojectDag
)

dataStagingTask = PythonOperator(
    task_id="dataStaging",
    python_callable=dataStaging,
    dag=finalprojectDag

)

dataTransformationTask = PythonOperator(
    task_id="dataTransformation",
    python_callable=dataTransformation,
    dag=finalprojectDag

)

##8056 staging_events
## 5242 staging_songs
## select count(*),level from public."songplays" group by level;
## select count(*), song_id from public.songplays group  by song_id having count(*) > 2;
## select count(*), sp.song_id,s.title  from public.songplays sp, public.songs s where sp.song_id=s.song_id  group  by sp.song_id, s.title having count(*) > 2;

## do analysis, write to a parquet file using atomic write, then visualize using matplotlib


# task dependencies

#dataStagingTask >> dataValidationTask >> dataTransformationTask >> showVisualTask
createTablesTask >> dataStagingTask  >> dataTransformationTask

# if __name__ == '__main__':
#     finalprojectDag.clear(reset_dag_runs=True)
#     # os.system("airflow initdb")
#     # dag.run()
#     createTablesTask.run()
#     dataStagingTask.run()
#     dataTransformationTask.run()