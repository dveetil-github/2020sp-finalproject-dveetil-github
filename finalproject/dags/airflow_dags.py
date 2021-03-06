import datetime, logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine

from finalproject.dags.sql_scripts import DB_USER, DB_PASSWORD, DB_PORT, HOST, DB_NAME
from finalproject.dags.tasks import createTables, dataStaging, dataTransformation, performAnalysis, Visualization

#this is the Dag which has all the tasks attached to it and tasks will be executed based on the schedule defines here
finalprojectDag = DAG(
    'finalprojectDag',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    # start_date=datetime.datetime.now(),
    schedule_interval="@daily")

#This is the create table task
createTablesTask = PythonOperator(
    task_id="createTables",
    python_callable=createTables,
    dag=finalprojectDag,
    #provide_context=True,
    #on_failure_callback=failure_email
)
##This is the data staging task
dataStagingTask = PythonOperator(
    task_id="dataStaging",
    python_callable=dataStaging,
    dag=finalprojectDag

)
# #This is the task which coverts satged data to star schema
dataTransformationTask = PythonOperator(
    task_id="dataTransformation",
    python_callable=dataTransformation,
    dag=finalprojectDag

)
# any one of the preceding tasks has been successful performAnalysisTask should be executed.
# trigger_rule=TriggerRule.ONE_SUCCESS,

#this task will do analysis using dask , dask will read data from redshift table and merge and group by to get the counts
performAnalysisTask = PythonOperator(
    task_id="performAnalysis",
    python_callable=performAnalysis,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=finalprojectDag

)
# this will plot the data and save as image file
VisualizationTask = PythonOperator(
    task_id="Visualization",
    python_callable=Visualization,
    dag=finalprojectDag

)
# this will check in the first step whether tables in the Reshift has data, if yes, then direcly perform analysis. Else start from create tables
def branch_func(**kwargs):
    try:
        conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
        engine = create_engine(conn_string)
        import pandas as pd
        sq_qury = pd.read_sql_query("select * from public.songplays limit 10", engine)
        df = pd.DataFrame(sq_qury, columns=['song_id'])
        dataExists = False
        if df.song_id.count() > 0 :
            dataExists = True
        logging.info("dataExists")
        logging.info(dataExists)
        if dataExists:
            logging.info("data is ready for analysis")
            return 'continue_task'
        else:
            print("data does not exists")
            return 'createTables'
    except:
        return 'createTables'
    finally:
        logging.info("branch function")

# def branch_func(**kwargs):
#         dataExists = False
#         if dataExists:
#             logging.info("data is ready for analysis")
#             return 'continue_op'
#         else:
#             return 'createTables'

branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    dag=finalprojectDag)

continue_op = DummyOperator(task_id='continue_task', dag=finalprojectDag)

# task dependencies

branch_op >> [createTablesTask , continue_op]
createTablesTask >> dataStagingTask  >> dataTransformationTask >> performAnalysisTask >> VisualizationTask
continue_op >> performAnalysisTask >> VisualizationTask

# if __name__ == '__main__':
#     finalprojectDag.clear(reset_dag_runs=True)
#     # branch_op.run()
#     createTablesTask.run()
#     dataStagingTask.run()
#     dataTransformationTask.run()
#     performAnalysisTask.run()
#     VisualizationTask.run()




