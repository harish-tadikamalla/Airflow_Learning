from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
from scripts.harish_tadikamalla.incremental_backup import incremental_etl
from scripts.harish_tadikamalla.etl_functions import (
    late_payment_aggregated_view_task,
    billing_amount_aggregated_view_task
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection parameters
POSTGRESQL_CONFIG = {
    "host": "telecom.c2b3frbss0vu.ap-south-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "Telecom_Airflow123",
    "database": "wetelco_telecom",
    "port": 5432,
}

DESTINATION_BUCKET = "tredence-backup-bucket/harish_tadikamalla"
AGGREGATED_VIEW_BUCKET = "tredence-aggregated-view-bucket/harish_tadikamalla"

# Define default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "email": ["harish.tadikamalla@tredence.com"],
    "email_on_retry": True,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": True,
}

# Instantiate the DAG
with DAG(
    dag_id="harish_tadikamalla_taskgroup",
    default_args=default_args,
    schedule_interval="30 21 * * *",  # Adjusted to run at 3:00 AM IST
    catchup=False,
) as dag:

    # Task Group for Incremental Backup Tasks
    with TaskGroup("incremental_backup_tasks") as incremental_backup_tasks:
        process_plans = PythonOperator(
            task_id="process_plans",
            python_callable=incremental_etl,
            op_args=["plans", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        process_customer_information = PythonOperator(
            task_id="process_customer_information",
            python_callable=incremental_etl,
            op_args=["customer_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        process_billing = PythonOperator(
            task_id="process_billing",
            python_callable=incremental_etl,
            op_args=["billing", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        process_customer_rating = PythonOperator(
            task_id="process_customer_rating",
            python_callable=incremental_etl,
            op_args=["customer_rating", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        process_device_information = PythonOperator(
            task_id="process_device_information",
            python_callable=incremental_etl,
            op_args=["device_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )
        
    # Aggregated View Tasks
    late_payment_aggregated_view = PythonOperator(
        task_id="late_payment_aggregated_view",
        python_callable=late_payment_aggregated_view_task,
        op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
    )

    billing_amount_aggregated_view = PythonOperator(
        task_id="billing_amount_aggregated_view",
        python_callable=billing_amount_aggregated_view_task,
        op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
    )

    # Define task dependencies
    # Ensure incremental backups complete before aggregated views run
    incremental_backup_tasks >> [late_payment_aggregated_view, billing_amount_aggregated_view]