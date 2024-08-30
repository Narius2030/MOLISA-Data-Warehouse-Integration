from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime

from medallion.hongheo.bronze import run_bronze_hongheo
from medallion.hongheo.gold import run_gold_hongheo
from medallion.hongheo.silver import run_silver_hongheo
from medallion.ncc.silver import run_silver_ncc
from tracking.track import track_transform, track_load


def start():
    print('Starting to Integrate Data Warehouse...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))
    
    
with DAG(
    'Integrate_Data_Warehouse',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:
    # START
    print_start_task = PythonOperator(
        task_id='print_start',
        python_callable=start,
        dag=dag
    )
    
    refresh = SQLExecuteQueryOperator(
        task_id='refresh_stages',
        conn_id = 'postgres_ldtbxh_stage',
        sql="./sql/refresh.stage.sql",
        dag=dag
    )
    
    # Bronze Stage
    bronze_hongheo = PythonOperator(
        task_id='bronze_hongheo',
        params={'bearer_token': Variable.get('bearer_token')},
        python_callable=run_bronze_hongheo,
        dag=dag
    )
    bronze_atvsld = SQLExecuteQueryOperator(
        task_id='bronze_atvsld',
        conn_id = 'postgres_ldtbxh_stage',
        sql=Variable.get('atvsld_data_dir')+"/load_atvsld.stage.sql",
        dag=dag
    )
    bronze_ncc =  SQLExecuteQueryOperator(
        task_id='bronze_ncc',
        conn_id = 'postgres_ldtbxh_stage',
        sql=Variable.get('ncc_data_dir')+"/load_ncc.stage.sql",
        dag=dag
    )
    
    # Silver Stage
    silver_hongheo = PythonOperator(
        task_id='silver_hongheo',
        python_callable=run_silver_hongheo,
        dag=dag
    )
    silver_ncc = PythonOperator(
        task_id='silver_ncc',
        python_callable=run_silver_ncc,
        dag=dag
    )
    
    # Gold Stage
    gold_hongheo = PythonOperator(
        task_id='gold_hongheo',
        python_callable=run_gold_hongheo,
        dag=dag
    )
    
    # Tracking
    tracking_transform = PythonOperator(
        task_id='logging_transform',
        python_callable=track_transform,
        dag=dag
    )
    
    # Load to Data Warehouse
    
    # TODO: Load to Hongheo DWH
    load_hongheo = SQLExecuteQueryOperator(
        task_id='load_hongheo',
        conn_id = 'postgres_ldtbxh_dwh',
        sql=Variable.get('hongheo_data_dir')+"/load_hongheo.dwh.sql",
        dag=dag
    )
    
    # TODO: Load to ATVSLD DWH
    load_atvsld = SQLExecuteQueryOperator(
        task_id='load_atvsld',
        conn_id = 'postgres_ldtbxh_dwh',
        sql=Variable.get('atvsld_data_dir')+"/load_atvsld.dwh.sql",
        dag=dag
    )
    
     # TODO: Load to NCC DWH
    load_ncc = SQLExecuteQueryOperator(
        task_id='load_ncc',
        conn_id = 'postgres_ldtbxh_dwh',
        sql=Variable.get('ncc_data_dir')+"/load_ncc.dwh.sql",
        dag=dag
    )
    
    # Tracking
    tracking_load = PythonOperator(
        task_id='logging_load',
        python_callable=track_load,
        dag=dag
    )
    
    #END
    print_end_task = PythonOperator(
        task_id='print_end',
        python_callable=end,
        dag=dag
    )
    
    print_start_task >> refresh 

    refresh >> bronze_hongheo >> silver_hongheo >> gold_hongheo >> tracking_transform
    refresh >> bronze_atvsld >> tracking_transform
    refresh >> bronze_ncc >> silver_ncc >> tracking_transform
    
    tracking_transform >> load_hongheo >> tracking_load >> print_end_task
    tracking_transform >> load_atvsld >> tracking_load >> print_end_task
    tracking_transform >> load_ncc >> tracking_load >> print_end_task