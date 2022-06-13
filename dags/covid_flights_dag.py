from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

# Configurations
BUCKET_NAME = "{your-bucket-name}"  # replace this with your bucket name
airports_data = "./dags/data/airports.csv"
covid_data = "./dags/data/covid.csv"
countries_data = "./dags/data/countries.json"
continents_data = "./dags/data/continents.csv"
ind_states_data = "./dags/data/india-states.csv"

s3_airports_data = "raw/airports.csv"
s3_covid_data = "raw/covid.csv"
s3_countries_data = "raw/countries.json"
s3_continents_data = "raw/continents.csv"
s3_ind_states_data = "raw/india-states.csv"
s3_flights_data = "raw/opensky/*.csv"

local_script = "./dags/scripts/spark/covid_flights_etl.py"
s3_script = "scripts/covid_flights_etl.py"

SPARK_STEPS = [

    {
        "Name": "Data tranformation",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "Covid Flights ETL",
    "ReleaseLabel": "emr-6.3.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# helper function
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "covid_flights_dag",
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

airports_to_s3 = PythonOperator(
    dag=dag,
    task_id="airports_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": airports_data, "key": s3_airports_data,},
)
covid_to_s3 = PythonOperator(
    dag=dag,
    task_id="covid_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": covid_data, "key": s3_covid_data,},
)
countries_to_s3 = PythonOperator(
    dag=dag,
    task_id="countries_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": countries_data, "key": s3_countries_data,},
)
continents_to_s3 = PythonOperator(
    dag=dag,
    task_id="continents_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": continents_data, "key": s3_continents_data,},
)
ind_states_to_s3 = PythonOperator(
    dag=dag,
    task_id="ind_states_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": ind_states_data, "key": s3_ind_states_data,},
)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script,},
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_script": s3_script,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> [airports_to_s3, covid_to_s3, countries_to_s3, continents_to_s3, ind_states_to_s3, script_to_s3] >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
