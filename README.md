# CSCIE-29-Final Project Submitted by Deepa Thazhathu Veetil
# Overview
This project is to build data pipeline using python and automate it using Apache Airflow. 
Data resides in S3 and data pipeline in python will help to make the data available in Redshift database for Analysis. 
Used the python techniques learned in the class to build an effective data pipeline.
Data pipeline is automated using Apache Airflow.
#Tasks performed in the project
Using Python, read data from S3 and stage into Amazon Redshift tables. 
Transform the data from staging tables to 'star' schema using Python and make the data ready for analysis in the database with fact and dimension tables.
Perform analysis and save results to a parquet file.
Provide visualization of result using Matplotlib in Python. 
Automate the tasks using Apache Airflow.
# Advanced Python Learnings Applied in the project
Decorator pattern→ to manage connections to database -->create connection, commit and close the connection
Context Manager pattern → writing files
Atomic write
Csci-utils library
Dask dataframe to perform analysis.
Pandas dataframe to read file.
Matplotlib to plot the analysis results.
Cookiecutter is used to create project.
# code 
The finalproject/dags folder has the airflow_dags.py file. This has finalprojectDag defined. 
The same folder has tasks.py folder where all the task related pythin functions are defined.
sql_scripts has the sql scripts required in the project.
# Technologies used 
Docker - airflow server is running in docker
Apache airflow
Redshift - 2 node cluster running in AWS
S3 - the data files that are read for the data pipeline reside in here
# Environment set up
Set up a docker container with Airflow and other required packages installed
Step 1:
docker-compose build from the project directory
You will see below message once image is ready.
Successfully tagged 2020sp-finalproject-dveetil-github_app:latest
Step 2:
run the docker server-->
docker run -d -p 8080:8080 -v C:\Users\deepa\Documents\AdvPythonDS\PSETS\2020sp-finalproject-dveetil-github:/usr/local/airflow/dags --env-file C:\Users\deepa\Documents\AdvPythonDS\PSETS\2020sp-finalproject-dveetil-github\.env 2020sp-finalproject-dveetil-github_app:latest

Step3:
install csci-utils-->(Tried to install as part of docker-compose build, but it was not able to recognize the CI_USER_TOKEN provided. So installed in the container and used docker commit to build an image from it. Used this image to start the container.
Then opened bash and installed csci-utils)
docker exec -u root -it bd1d00f47d01 bash
pip install -e git+https://github.com/csci-e-29/2020sp-csci-utils-dveetil-github@557dbe2c286cd7acfe2612e1dbb76a03d30b27e8#egg=csci-utils
pip install fastparquet (I had to do this step here because when I include it in docker file, it failed with ip._vendor.urllib3.exceptions.ReadTimeoutError: HTTPSConnectionPool(host='files.pythonhosted.org', port=443): Read timed out.)

# start Redshift cluster in AWS
Started a 2 node cluster in AWS. 
Once the cluster is available, get the endpoint and update the HOST variable in the .env file
Please update all the .env variables according to user settings.

# start the Airflow UI
http://localhost:8080/admin/

You can see the finalprojectDag

Turn it On and Trigger the dag.

This Dag will follow the path based on the branch_task check
If the data ready for analysis, it will skip createTables, dataStaging and dataTransformation
If not, it will run the tasks for createTables, dataStaging, dataTransformation, followed by performAnalysis, Visualization.

create table task--> create tables required in Redshift
data staging task-->will load the data from S3 to staging tables.
data tranformation task--> will tranform data from staging to star schema
perform analysis task--> data is read from database using dask and merged and grouped and then saved atomically into parquet file.
Visualization task-->  the file is read and plotted a bar graph and saved into an image file. This can be extended to use tools like Tableau.

https://github.com/dveetil-github/2020sp-finalproject-dveetil-github/blob/develop/Dag_success.PNG

# Testing
Test cases are added to test the tasks in the test_pset.py.
The code is submitted as it should run from the Apache airflow UI when Trigger Dag is executed.  

For test cases to run, please do following steps:  

comment out # task dependencies part of the code as shown below.   

CODE:&nbsp
branch_op >> [createTablesTask , continue_op] &nbsp
createTablesTask >> dataStagingTask  >> dataTransformationTask >> performAnalysisTask >> VisualizationTask &nbsp
continue_op >> performAnalysisTask >> VisualizationTask &nbsp

They are for airflow to determine the dependency of tasks. 

also comment out --> start_date=datetime.datetime.now() - datetime.timedelta(days=1), and uncomment start_date=datetime.datetime.now(),
Other wise when test cases are executed, tasks will be added to queue.  

for the test cases for test_performAnalysis--> create data directory in /usr/local/airflow/dags

# Comparison with Luigi
Apache Airflow has nice UI 
Has built in scheduler
Can easily test DAGs
Luigi scheduling is with cron


