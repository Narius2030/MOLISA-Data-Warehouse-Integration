from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from timelocation.load_date import readTimeExcel

def start():
    print('Starting to Integrate Time-Location data...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))


with DAG(
    'Integrate_Time_Location',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:
    print_start_task = PythonOperator(
        task_id='print_start',
        python_callable=start,
        dag=dag
    )

    refresh = SQLExecuteQueryOperator(
        task_id='refresh_stages',
        conn_id = 'postgres_ldtbxh_stage',
        sql="./sql/refresh_timeloc.stage.sql",
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
    
    load_timeloc = SQLExecuteQueryOperator(
        task_id='load_time',
        conn_id = 'postgres_ldtbxh_dwh',
        sql=Variable.get('timelocation_data_dir')+"/load_timeloc.dwh.sql",
        dag=dag
    )

    print_end_task = PythonOperator(
        task_id='print_end',
        python_callable=end,
        dag=dag
    )

print_start_task >> refresh >> [bronze_time, bronze_location] >> load_timeloc >> print_end_task