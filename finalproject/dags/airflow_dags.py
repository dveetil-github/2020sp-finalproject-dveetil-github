import datetime, logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine

from finalproject.dags.sql_scripts import DB_USER, DB_PASSWORD, DB_PORT, HOST, DB_NAME
from finalproject.dags.tasks import createTables, dataStaging, dataTransformation, performAnalysis, Visualization

finalprojectDag = DAG(
    'finalprojectDag',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    #start_date=datetime.datetime.now(),
    schedule_interval="@daily")


createTablesTask = PythonOperator(
    task_id="createTables",
    python_callable=createTables,
    dag=finalprojectDag,
    #provide_context=True,
    #on_failure_callback=failure_email
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
# any one of the preceding tasks has been successful performAnalysisTask should be executed.
# trigger_rule=TriggerRule.ONE_SUCCESS,

performAnalysisTask = PythonOperator(
    task_id="performAnalysis",
    python_callable=performAnalysis,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=finalprojectDag

)

VisualizationTask = PythonOperator(
    task_id="Visualization",
    python_callable=Visualization,
    dag=finalprojectDag

)

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
# performAnalysisTask >> VisualizationTask
continue_op >> performAnalysisTask >> VisualizationTask

# if __name__ == '__main__':
#     finalprojectDag.clear(reset_dag_runs=True)
#     branch_op.run()
#      createTablesTask.run()
#      dataStagingTask.run()
#      dataTransformationTask.run()
#      performAnalysisTask.run()
#      VisualizationTask.run()





##8056 staging_events
## 5242 staging_songs
## select count(*),level from public."songplays" group by level;
## select count(*), song_id from public.songplays group  by song_id having count(*) > 2;
## select count(*), sp.song_id,s.title  from public.songplays sp, public.songs s where sp.song_id=s.song_id  group  by sp.song_id, s.title having count(*) > 2;

## do analysis, write to a parquet file using atomic write, then visualize using matplotlib
# createTablesTask >> dataStagingTask  >> dataTransformationTask >> performAnalysisTask >> VisualizationTask
# branch_op >> [createTablesTask , continue_op]
# createTablesTask >> dataStagingTask  >> dataTransformationTask >> performAnalysisTask >> VisualizationTask
# continue_op >> performAnalysisTask >> VisualizationTask

# branch_op >> createTablesTask >> dataStagingTask  >> dataTransformationTask >> performAnalysisTask >> VisualizationTask
# branch_op >> performAnalysisTask >> VisualizationTask

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


# from airflow.utils.email import send_email
# def failure_email(context):
#     email_title = "Airflow Task {{ task.task_id }} Failed"
#     email_body = "{{ task.task_id }} in {{ dag.dag_id }} failed."
#     send_email('deepa.veetil@gmail.com', email_title, email_body)
#
# def build_email(**context):
#         email_op = EmailOperator(
#             task_id='send_email',
#             to="deepa.veetil@gmail.com",
#             subject="Test Email Please Ignore",
#             html_content=None,
#         )
#         email_op.execute(context)
#
#
# email_op_python = PythonOperator(
#     task_id="python_send_email", python_callable=build_email, provide_context=True, dag=finalprojectDag
# )