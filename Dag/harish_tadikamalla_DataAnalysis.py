from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import logging
from airflow.models import Variable
from airflow.utils.dates import days_ago
from scripts.harish_tadikamalla.incremental_backup import incremental_etl
from scripts.harish_tadikamalla.etl_functions import (
    late_payment_aggregated_view_task,
    billing_amount_aggregated_view_task
)

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
# Define the default arguments
default_args = {
    "owner": "airflow",  # Owner of the DAG
    "start_date": datetime(2024, 8, 25),
    "email": ["harish.tadikamalla@tredence.com"],  # List of emails to send notifications
    "email_on_retry": True,  # Send email on retry
    "email_on_failure": True,  # Send email on failure
    "retries": 3,  # Number of retries
    "retry_delay": timedelta(minutes=1),  # Delay between retries
    "catchup": True,  # Whether to catch up on missed runs
}

# Instantiate the DAG
with DAG(
    dag_id="harish_tadikamalla_dataanalysis",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # we want it to run on daily at 3 AM IST
    catchup=False,
) as dag:

    process_billing = PythonOperator(
        task_id=f"process_billing",
        python_callable=incremental_etl,
        op_args=["billing", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    process_customer_rating = PythonOperator(
        task_id=f"process_customer_rating",
        python_callable=incremental_etl,
        op_args=["customer_rating", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    process_device_information = PythonOperator(
        task_id=f"process_device_information",
        python_callable=incremental_etl,
        op_args=["device_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    process_plans = PythonOperator(
        task_id=f"process_plans",
        python_callable=incremental_etl,
        op_args=["plans", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    process_customer_information = PythonOperator(
        task_id=f"process_customer_information",
        python_callable=incremental_etl,
        op_args=["customer_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    )

    # Aggregated View Task Group
    with TaskGroup("customer_rating_aggregated_view") as customer_rating_aggregated_view:
        # Aggregation Tasks
        late_payment_aggregated_view = PythonOperator(
            task_id="late_payment_aggregated_view",
            python_callable=late_payment_aggregated_view_task,
            op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
        )

        # Aggregation Tasks
        
        billing_amount_aggregated_view = PythonOperator(
            task_id="billing_amount_aggregated_view",
            python_callable=billing_amount_aggregated_view_task,
            op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
        )

    # Define task dependencies
    
    process_plans, process_customer_information 
    process_customer_information >> [process_billing, process_device_information, process_customer_rating]
    process_customer_rating >> customer_rating_aggregated_view
