import os
import shutil

import datetime, logging

import psycopg2
import pandas as pd
import dask.dataframe as dd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from . import generate_tables
# from .generate_tables import create_tables
from csci_utils.luigi.dask.target import ParquetTarget
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

from finalproject.dags.generate_tables import create_tables, drop_tables
from finalproject.dags.sql_scripts import HOST, DB_NAME, DB_USER, DB_PASSWORD, data_stagg_qrys, data_transform_qrys,DB_PORT
from csci_utils.io import atomic_write

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

# def showVisual():
#     # data = pd.read_csv("results.csv")
#     conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
#     engine = create_engine(conn_string)
#     df = pd.read_sql_query("select count(*), sp.song_id,s.title  from public.songplays sp, public.songs s where sp.song_id=s.song_id  group  by sp.song_id, s.title having count(*) > 2;", engine)
#     dest_file = os.path.join(os.getcwd(), 'data', "res.parquet")
#     with atomic_write(dest_file) as f:
#         df.to_csv(f.name)
#     import matplotlib.pyplot as plt
#     plt.plot([1, 2, 3, 4])
#     plt.ylabel('some numbers')
#     plt.savefig('books_read.png')

def performAnalysis():
    """
    This method will perform the data analysis and save to a csv file which can be used for visualization
    :return:
    """
    logging.info("Started analysis")
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
    df1 = dd.read_sql_table('songs', conn_string, index_col='year', npartitions=3)
    # print(df1.head())
    df2 = dd.read_sql_table('songplays', conn_string, index_col='songplay_id', npartitions=3)
    # print(df2.head())
    ddf = dd.merge(df1, df2, how='inner', on='song_id').groupby('title').song_id.count().compute()
    # res = res.sort_values(ascending=False)
    # print(res.head())
    dest_file = os.path.abspath('data') + '/results.csv'
    with atomic_write(dest_file) as f:
        ddf.to_csv(path_or_buf=f.name, header=True)

    logging.info("End of analysis")

def Visualization():
    """
    Visualization of the analysed data
    :return:
    """
    logging.info("starting visualization")
    nameOfCSV = os.path.abspath('data') + '/results.csv'
    pdf = pd.read_csv(nameOfCSV)
    pdf = pdf.sort_values(by=["song_id"], ascending=False)
    pdf = pdf.rename(columns={'song_id':'count'})

    # filter_condn = pdf['song_id'] >= 5
    # pdf = pdf.where(filter_condn)
    ax = plt.gca()
    pdf[:5].plot(kind='bar', x='title', y='count', ax=ax)
    #pdf.plot()
    plt.savefig('song_data.png')
    completed_folder = os.path.abspath('completed') + '/'
    shutil.move(nameOfCSV,completed_folder)
    dt = str(datetime.datetime.now())
    os.rename(completed_folder+'results.csv', completed_folder+'results.csv'+ dt)
    logging.info("starting visualization")

    ## read songplay table
    ## merge 2 dds using song_id
    ## grpby, get count, save into disk
    ## then read it and plot and save the image

finalprojectDag = DAG(
    'finalprojectDag',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    #start_date=datetime.datetime.now(),
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

performAnalysisTask = PythonOperator(
    task_id="performAnalysis",
    python_callable=performAnalysis,
    dag=finalprojectDag

)

VisualizationTask = PythonOperator(
    task_id="Visualization",
    python_callable=Visualization,
    dag=finalprojectDag

)

##8056 staging_events
## 5242 staging_songs
## select count(*),level from public."songplays" group by level;
## select count(*), song_id from public.songplays group  by song_id having count(*) > 2;
## select count(*), sp.song_id,s.title  from public.songplays sp, public.songs s where sp.song_id=s.song_id  group  by sp.song_id, s.title having count(*) > 2;

## do analysis, write to a parquet file using atomic write, then visualize using matplotlib


# task dependencies

createTablesTask >> dataStagingTask  >> dataTransformationTask >> performAnalysisTask >> VisualizationTask

# if __name__ == '__main__':
#      finalprojectDag.clear(reset_dag_runs=True)
#      createTablesTask.run()
#      dataStagingTask.run()
#      dataTransformationTask.run()
#      performAnalysisTask.run()
#      VisualizationTask.run()


