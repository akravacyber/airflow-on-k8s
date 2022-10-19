from airflow import DAG
from airflow.providers.oracle.operators.oracle import (
    OracleOperator,
    OracleStoredProcedureOperator,
)
from datetime import datetime, timedelta

oracle_conn = "oracle_demo_connection"

with DAG(
    dag_id="oracle_upload_data",
    start_date=datetime.utcnow(),
    schedule_interval=timedelta(minutes=3),
    max_active_runs=3,
    default_args={"oracle_conn_id": oracle_conn},
    catchup=False,
    tags=['example', 'oracle', 'new']
) as dag:
    
    opr_create_tbl= OracleOperator(
        task_id='task_create_tbl',
        sql= 'create table1 (a INT, b INT, c INT, d TIMESTAMP) if not exists',
        autocommit ='True')
  
    opr_insert = OracleOperator(
        task_id='task_insert_values',
        sql= 'insert into table1 (a,b,c) values (1, 2, 3, CURRENT_TIMESTAMP)',
        autocommit ='True')
    
    opr_create_tbl >> opr_insert
    
