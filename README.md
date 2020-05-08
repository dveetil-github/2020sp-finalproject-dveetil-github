# CSCIE-29-Final Project Submitted by Deepa Thazhathu Veetil
# Overview
This project is to build data pipeline using python and automate it using Apache Airflow. 
Data resides in S3 and data pipeline in python will help to make the data available in Redshift database for Analysis. 
Focus was on using the python techniques learned in the class to build an effective data pipeline.
Data pipeline is automated using Apache Airflow.
#Tasks performed in the project
Using Python, read data from S3 and stage into Amazon Redshift tables. 
Transform the data from staging tables to 'star' schema using Python and make the data ready for analysis in the database with fact and dimension tables.
Perform analysis and save results to a file.
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
install csci-utils-->(Tried to install as part of docker-compose build, but it was not able to recognize the CI_USER_TOKEN provided. So installed in the container and used docker commit to build an image from it)
docker exec -u root -it bd1d00f47d01 bash
pip install -e git+https://github.com/csci-e-29/2020sp-csci-utils-dveetil-github@557dbe2c286cd7acfe2612e1dbb76a03d30b27e8#egg=csci-utils

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
If not, it will run the tasks for createTables, dataStaging, dataTransformation, followed by performAnalysis, Visualization

https://github.com/dveetil-github/2020sp-finalproject-dveetil-github/blob/develop/Dag_success.PNG

# Comparison with Luigi
Apache Airflow has nice UI 
Has built in scheduler
Can easily test DAGs
Luigi scheduling is with cron
