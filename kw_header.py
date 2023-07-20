from airflow import models
from datetime import datetime, timedelta
from google.cloud import bigquery
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.oauth2 import service_account
import requests
import pandas as pd
import os


with models.DAG(
    dag_id='KW_NPS_HEADER',
    schedule="0 02 * * *",
    start_date = (datetime.today() - timedelta(days=1)),
    catchup=False,
    tags=["live"],
) as dag:

    ################################## EXTRACT ######################################
    def extract():
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%m-%d-%Y')
        completed_date = yesterday
        headers = {'APIKey': os.environ.get('kw_api_key')}
        URL = f"https://api.cys.group/public/api/project?projectId=1&versionId=-1&filter=sys_completedDate%20%3E%3D%20'{completed_date}'"
        request = requests.get(URL, headers=headers, verify=False)
        res = request.json() 
        return res

    ################################## TRANSFORM ######################################

    def transform(Response):
        # Extracting the Headers and transforming it into a data frame 
        header = Response['Data']['Headers']
        df = pd.DataFrame(header)
        return df

    ################################## LOAD ######################################

    def load(df):
        # BigQuery credentials 
        PROJECT_ID = os.environ.get('kw_project_id')
        DATASET_ID =  os.environ.get('kw_dataset_id')
        TABLE = TABLE = os.environ.get('kw_header_table')
        TABLE_ID=f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"

        # Construct a BigQuery client object
        client = bigquery.Client()

        # Load the data into the table.
        load_job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        )
        
        load_job = client.load_table_from_dataframe(
            df, 
            TABLE_ID,
            job_config=load_job_config,
        )  

        load_job.result()  

        # Get the updated table data.
        data = client.get_table(TABLE_ID)  
        return data

    ################################## ARGUMENTS ######################################

    def arguments():
        header = extract()
        print("Extracting Done")
        df = transform(header)
        print("Transforming Done")
        load(df)
        print("Loading Done")
        return "Data Table Uploaded"

    ################################## OPERATORS ######################################
    start_job = BashOperator(
        task_id="START",
        bash_command='echo "START PIPELINE"; sleep 10',
        dag=dag,

    )

    proccess_data = BashOperator(
        task_id="PROCESS_JOB",
        bash_command='echo "PROCESS DATA JOB"; sleep 10',
        dag=dag,

    )

    data = PythonOperator(
        task_id='UPLOAD_HEADERS',
        python_callable=arguments,
        dag=dag,
    )

    data_success = BashOperator(
        task_id="UPLOAD_SUCCEEDED",
        bash_command='echo "DATA UPLOAD SUCCEEDED"',
        dag=dag,
        trigger_rule="all_success"
        
    )

    data_fail = BashOperator(
        task_id="UPLOAD_FAILED",
        bash_command='echo "DATA UPLOAD FAILED"',
        dag=dag,
        trigger_rule="all_failed"
    )

    end_data = BashOperator(
        task_id="JOB_ENDED",
        bash_command='echo "DATA JOB ENDED"',
        dag=dag,
        trigger_rule="all_done"
    )
    job_ended = BashOperator(
        task_id="END",
        bash_command='echo "THE HOLe PIPELINE ENDED SUCCESSFULLY"; sleep 10',
        dag=dag,
        trigger_rule="all_done"
    )

    ################################## TASK DEPENDENCIES ##############################

start_job >> proccess_data >> data >> data_success
data_fail << data
data_success >> end_data >> job_ended
data_fail >> end_data >> job_ended