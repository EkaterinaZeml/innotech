from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import datetime

conn_pg_params = {
    'host': '10.4.49.51',
    'database': 'student09_zemlyanskaya_ea',
    'user': 'airflow',
    'password': 'airflow',
}

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}


def connect(params_dic):
    # NOTE подключение к серверу
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit(1)
    return conn


def generate_new_order(**kwargs):
    conn_pg = connect(conn_pg_params)
    conn_pg.cursor().execute('select * from oltp_src_system.create_order();')
    conn_pg.commit()
    conn_pg.close()


def update_order(**kwargs):
    conn_pg = connect(conn_pg_params)
    conn_pg.cursor().execute('select * from oltp_src_system.update_order();')
    conn_pg.commit()
    conn_pg.close()


def delete_order(**kwargs):
    conn_pg = connect(conn_pg_params)
    conn_pg.cursor().execute('select * from oltp_src_system.delete_order();')
    conn_pg.commit()
    conn_pg.close()


def load_cdc_order_data_changes(**kwargs):
    conn_pg = connect(conn_pg_params)
    conn_pg.cursor().execute('select * from dwh_stage.load_cdc_order_data_changes();')
    conn_pg.commit()
    conn_pg.close()


def load_order_data_hist(**kwargs):
    conn_pg = connect(conn_pg_params)
    conn_pg.cursor().execute('select * from dwh_ods.load_order_data_hist();')
    conn_pg.commit()
    conn_pg.close()


def generate_report(**kwargs):
    conn_pg = connect(conn_pg_params)
    conn_pg.cursor().execute('select * from report.generate_report();')
    conn_pg.commit()
    conn_pg.close()


with DAG(
    dag_id='st09_zemlyanskaya_ea_order_data',
    default_args=default_args,
    description='',
    schedule_interval='*/10 * * * *',
    start_date=datetime.datetime(2023, 5, 18),
    catchup=False,
    max_active_runs=1,
    tags=['case_3', 'zemlyanskaya_ea'],
) as dag:

    start_process = DummyOperator(
        task_id='start_process',
    )

    end_process = DummyOperator(
        task_id='end_process',
    )

    with TaskGroup(group_id="generate_order_data") as generate_order_data:

        task_1_generate_new_order = PythonOperator(
            task_id='task_1_generate_new_order',
            python_callable=generate_new_order,
            op_kwargs={},
        )

        task_2_update_order = PythonOperator(
            task_id='task_2_update_order',
            python_callable=update_order,
            op_kwargs={},
        )

        task_3_delete_order = PythonOperator(
            task_id='task_3_delete_order',
            python_callable=delete_order,
            op_kwargs={},
        )

        task_1_generate_new_order >> task_2_update_order >> task_3_delete_order

    task_4_load_cdc_order_data = PythonOperator(
        task_id='task_4_load_cdc_order_data',
        python_callable=load_cdc_order_data_changes,
        op_kwargs={},
    )

    task_5_load_order_data_hist = PythonOperator(
        task_id='task_5_load_order_data_hist',
        python_callable=load_order_data_hist,
        op_kwargs={},
    )

    task_6_generate_report = PythonOperator(
        task_id='task_6_generate_report',
        python_callable=generate_report,
        op_kwargs={},
    )

    start_process >> generate_order_data >> task_4_load_cdc_order_data >> task_5_load_order_data_hist >> task_6_generate_report >> end_process
