import configparser
import logging

import psycopg2
from finalproject.dags.sql_scripts import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    logging.info("inside drop_tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    logging.info("inside create_tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# def main():
#     config = configparser.ConfigParser()
#     config.read('connection.cfg')
#     print("going to create tables ****")
#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     cur = conn.cursor()
#
#     drop_tables(cur, conn)
#     create_tables(cur, conn)
#
#     conn.close()
#     print("done creating tables ****")
#
#
# if __name__ == "__main__":
#     main()