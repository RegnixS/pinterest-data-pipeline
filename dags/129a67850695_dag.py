from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

default_args = {
    'owner': 'rob',
    'email': ['rgducke@gmail.com'],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1)
}

with DAG('129a67850695_dag',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    # task to access data from S3 bucket
    notebook_task = {
        'notebook_path': '/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Access S3 Without Mounting'
    }
    opr_access_s3 = DatabricksSubmitRunOperator(
        task_id='access_s3',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )

    # task to clean pin data
    notebook_task = {
        'notebook_path': '/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Clean Pin Data'
    }
    opr_clean_pin = DatabricksSubmitRunOperator(
        task_id='clean_pin',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    
    # task to clean geo data
    notebook_task = {
        'notebook_path': '/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Clean Geo Data'
    }
    opr_clean_geo = DatabricksSubmitRunOperator(
        task_id='clean_geo',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    
    # task to clean user data
    notebook_task = {
        'notebook_path': '/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Clean User Data'
    }
    opr_clean_user = DatabricksSubmitRunOperator(
        task_id='clean_user',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )

    # task to do data analysis
    notebook_task = {
        'notebook_path': '/Repos/rgducke@gmail.com/pinterest-data-pipeline/databricks_notebooks/Data Analysis'
    }
    opr_data_analysis = DatabricksSubmitRunOperator(
        task_id='data_analysis',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    ) 
    opr_access_s3 >> [opr_clean_pin, opr_clean_geo, opr_clean_user] >> opr_data_analysis
