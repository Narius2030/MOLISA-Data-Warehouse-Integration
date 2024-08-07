from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from timelocation.load_date import readTimeExcel

def print_welcome():
    print('Starting to Integrate Time-Location data!')

def print_date():
    print('Finish at {}'.format(datetime.today().date()))


with DAG(
    'Integrate_Time_Location_Dimension',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:
    print_welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome,
        dag=dag
    )

    refresh = SQLExecuteQueryOperator(
        task_id='refresh_stages',
        conn_id = 'postgres_ldtbxh_stage',
        sql=Variable.get('timelocation_data_dir')+"/refresh.stage.sql",
        dag=dag
    )

    bronze_time = PythonOperator(
        task_id='bronze_time',
        python_callable=readTimeExcel,
        dag=dag
    )

    bronze_location = SQLExecuteQueryOperator(
        task_id='bronze_location',
        conn_id = 'postgres_ldtbxh_stage',
        sql=Variable.get('timelocation_data_dir')+"/load_location.stage.sql",
        dag=dag
    )

    print_date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
        dag=dag
    )

print_welcome_task >> refresh >> [bronze_time, bronze_location] >> print_date_task