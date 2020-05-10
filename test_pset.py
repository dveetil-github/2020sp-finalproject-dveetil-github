import datetime
import pytest

from airflow import DAG
from airflow.models import DagBag
import unittest
import os
from finalproject.dags.airflow_dags import createTablesTask, dataStagingTask, dataTransformationTask, performAnalysisTask, VisualizationTask
from sqlalchemy import create_engine

from finalproject.dags.sql_scripts import DB_USER, DB_PASSWORD, DB_PORT, HOST, DB_NAME


class TestFinalProjectDAG(unittest.TestCase):
   @classmethod
   def setUpClass(cls):
       cls.dagbag = DagBag()

   def test_dag_loaded(self):
       """testing if the dag loaded properly"""
       dag = self.dagbag.get_dag(dag_id='finalprojectDag')
       self.assertDictEqual(self.dagbag.import_errors, {})
       self.assertIsNotNone(dag)
       self.assertEqual(len(dag.tasks), 7)


   def test_createTableTask(self):
       """testing createTable task of the dag"""
       createTablesTask.run()
       conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
       engine = create_engine(conn_string, echo=True)
       self.assertTrue(engine.dialect.has_table(engine, 'users'))
       self.assertTrue(engine.dialect.has_table(engine, 'artists'))
       self.assertTrue(engine.dialect.has_table(engine, 'songs'))
       self.assertTrue(engine.dialect.has_table(engine, 'songplays'))
       self.assertTrue(engine.dialect.has_table(engine, 'staging_events'))
       self.assertTrue(engine.dialect.has_table(engine, 'staging_songs'))
       self.assertTrue(engine.dialect.has_table(engine, 'time'))

   def test_dataSTaging(self):
       """testing data staging task of the dag"""
       dataStagingTask.run()
       conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
       engine = create_engine(conn_string, echo=True)
       cnt = 0
       with engine.connect() as connection:
           result = connection.execute("select count(1) from public.staging_events limit 10")
           row = result.fetchone()
           cnt = row['count']
           result.close()
       self.assertGreater(cnt, 0)

       cnt = 0
       with engine.connect() as connection:
           result = connection.execute("select count(1) from public.staging_songs limit 10")
           row = result.fetchone()
           cnt = row['count']
           result.close()
       self.assertGreater(cnt, 0)


   def test_dataTranformtask(self):
       """testing data staging task of the dag"""
       dataTransformationTask.run()
       conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
       engine = create_engine(conn_string, echo=True)
       cnt = 0
       with engine.connect() as connection:
           result = connection.execute("select count(1) from public.songs limit 10")
           row = result.fetchone()
           cnt = row['count']
           result.close()
       self.assertGreater(cnt, 0)

   def test_performAnalysis(self):
          """testing data staging task of the dag"""
          performAnalysisTask.run()
          destn = os.path.abspath('data') + '/results.parquet'
          assert os.path.exists(destn)

   def test_dag(self):
       return DAG(
           "finalprojectDag",
           default_args={"owner": "airflow", "start_date": datetime.datetime.now()},
           schedule_interval="@daily",)

