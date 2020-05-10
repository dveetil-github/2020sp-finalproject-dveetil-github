import os
import shutil

import datetime, logging

import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt

from finalproject.dags.generate_tables import create_tables, drop_tables
from finalproject.dags.sql_scripts import HOST, DB_NAME, DB_USER, DB_PASSWORD, data_stagg_qrys, data_transform_qrys,DB_PORT,connect_to_db
from csci_utils.io import atomic_write

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

def performAnalysis():
    """
    This method will perform the data analysis and save to a csv file which can be used for visualization
    read songplay table
    Read song table
    merge 2 dds using song_id
    grpby, get count, save into disk
    :return:
    """
    logging.info("Started analysis")
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
    # the index_col is year and npartitions is decided based on the criteria that the analysis is done for songs realesed in the last 3 years
    df1 = dd.read_sql_table('songs', conn_string, index_col='year', npartitions=3)
    print(df1.head())
    #the index column is location () and divisions is done to do 13 partitions in the lexographical order of locations
    # df2 = dd.read_sql_table('songplays', conn_string, index_col='location', divisions=list('acegikmoqsuwz'))
    df2 = dd.read_sql_table('songplays', conn_string, index_col='level', divisions=list('fp'))
    print("songplays")
    print(df2.head())
    # https://docs.dask.org/en/latest/dataframe-best-practices.html
    # based on the instructions here, reduce to samll size by group by and then convert to pandas.
    ddf = dd.merge(df1, df2, how='inner', on='song_id').groupby('title').song_id.count().compute()
    print(ddf.head())
    #dest_file = os.path.abspath('data') + '/results.csv'
    dest_file = os.path.abspath('data') + '/results.parquet'
    with atomic_write(dest_file) as f:
        ddf.to_frame().to_parquet(f.name, engine='fastparquet',compression='GZIP')
        # ddf.to_csv(path_or_buf=f.name, header=True)


    logging.info("End of analysis")

def Visualization():
    """
    Visualization of the analysed data
    then read analysed data  and plot and save the image
    :return:
    """
    logging.info("starting visualization")
    # nameOfResFile = os.path.abspath('data') + '/results.csv'
    nameOfResFile = os.path.abspath('data') + '/results.parquet'
    # pdf = pd.read_csv(nameOfResFile)
    pdf = pd.read_parquet(nameOfResFile)
    pdf = pdf.sort_values(by=["song_id"], ascending=False)
    pdf = pdf.rename(columns={'song_id':'count'})

    ax = plt.gca()
    #pdf[:5].plot(kind='bar', x='title', y='count', ax=ax)
    pdf['count'][:5].plot(kind='bar')
    plt.savefig('song_data.png')
    completed_folder = os.path.abspath('completed') + '/'
    shutil.move(nameOfResFile,completed_folder)
    dt = str(datetime.datetime.now())
    # os.rename(completed_folder+'results.csv', completed_folder+'results.csv'+ dt)
    os.rename(completed_folder + 'results.parquet', completed_folder + 'results.parquet' + dt)
    logging.info("starting visualization")
