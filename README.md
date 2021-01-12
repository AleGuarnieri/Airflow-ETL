
# Airflow project

## Installation and execution
In order to run this project it is necessary to have installed python, to have a properly set AWS account and to set Airflow connection. 

Once Airflow environment is connected to the DAG and the connections to AWS and Redshift are inserted into "Airflow Connections",
you can run the piplene directly into Airflow.

## Motivation
This project implements an ETL between S3 data storage and a Redshift Datawarehouse, running the entire pipeline with Apache Airfow. 

The data transformation queries were provided by Udacity and the purpose was to implement the DAG and the related operators
in order to correctly execute the ETL and the final data quality checks. 

## Details

The source datasets are public datasets available at "s3a://udacity-dend/". Data are copied from S3 to staging tables in Redshift cluster and then manipulated
to correctly insert them in the analytical tables. The pipeline cosists of 6 different steps:
- Begin_execution: to start the pipeline
- Stage_*: to Extract data from source tables into staging tables
- Load_songplays_fact_table: to Transform and Load data from staging into fact table
- Load_*_dim_table: to Transform and Load data from staging into dimension tables
- Run_data_quality_checks: to check that important columns on analytics tables did not have NULL values

Follow a screenshot of the pipiline from Airflow

!(https://github.com/AleGuarnieri/Airflow-ETL/blob/main/Airflow_DAG.png)

## File Description 
```dags```: it contains the main function with the dags part of the pipeline

```plugins```: it contains two folders. *helpers* contains script which performs queries responsible for inserting and transforming data; 
		*operators* contains the 4 operators called on the main script which perform the different steps of the pipeline (connection and ETL)

```create_tables```: it contains the queries to create both staging and analytical tables

## Acknowledgements
Udacity provided the course material necessary to implement the project
