from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration
import psycopg2
import papermill as pm

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('bi-nba', default_args=default_args, schedule_interval='@daily', 
    is_paused_upon_creation=False,
    catchup=False
    ) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=pm.execute_notebook,
        op_kwargs={
            'input_path': '/opt/airflow/dags/bi-nba/NBA_Extract.ipynb',
            'output_path': "/opt/airflow/dags/bi-nba/NBA_Extract_output.ipynb",
            'kernel_name': 'python3',
            'parameters': {"output_dir": "/opt/airflow/dags/bi-nba/xlsx_files/"}
        }
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=pm.execute_notebook,
        op_kwargs={
            'input_path': '/opt/airflow/dags/bi-nba/NBA_Transform.ipynb',
            'output_path': "/opt/airflow/dags/bi-nba/NBA_Transform_output.ipynb",
            'kernel_name': 'python3',
            'parameters': {"input_dir": "/opt/airflow/dags/bi-nba/xlsx_files/", "output_dir": "/opt/airflow/dags/bi-nba/csv_files/"}
        }
    )

    def load(postgres_conn_id : str, table_name : str, file_path : str):
        print("Load data to database")
        import pandas as pd
        from sqlalchemy import create_engine
        import os

        from airflow.models.connection import Connection
        conn = Connection.get_connection_from_secrets(postgres_conn_id)
        conn_uri = conn.get_uri()
        conn_uri = conn_uri.replace("postgres://", "postgresql+psycopg2://")
        print (conn_uri)
        engine = create_engine(conn_uri)
        df = pd.read_csv(file_path)
        print(df.head(5))
        if len(df) > 0:
            colunas_df = list(df)
            # cria (coluna1, coluna2, ...)
            colunas = ', '.join(colunas_df)

            # Cria VALUES (%s, %s, ...) para cada coluna
            valores = "VALUES({})".format(", ".join(["%s" for _ in colunas_df]))

            # Cria INSERT INTO tabela (coluna1, coluna2, ...) VALUES (%s, %s, ...)
            insert = "INSERT INTO {} ({}) {}".format(table_name, colunas, valores)

            conn = engine.raw_connection()
            cursor = conn.cursor()
            psycopg2.extras.execute_batch(cursor, insert, df.values)
            conn.commit()
            cursor.close()
            

        # with engine.connect() as conn:
        #     df.to_sql(table_name, con=conn, if_exists='replace', index=False)

    load_dim_jogador = PythonOperator(
        task_id='load_dim_jogador',
        python_callable=load,
        op_kwargs={
            'postgres_conn_id': 'nba_conn',
            'table_name': 'nba.dim_jogador',
            'file_path': '/opt/airflow/dags/bi-nba/csv_files/dim_jogador.csv'
        }
    )

    load_dim_ano = PythonOperator(
        task_id='load_dim_ano',
        python_callable=load,
        op_kwargs={
            'postgres_conn_id': 'nba_conn',
            'table_name': 'nba.dim_ano',
            'file_path': '/opt/airflow/dags/bi-nba/csv_files/dim_ano.csv'
        }
    )

    load_fato_nba = PythonOperator(
        task_id='load_fato_nba',
        python_callable=load,
        op_kwargs={
            'postgres_conn_id': 'nba_conn',
            'table_name': 'nba.fato_nba',
            'file_path': '/opt/airflow/dags/bi-nba/csv_files/fato_nba.csv'
        }
    )

    create_schema_nba = SQLExecuteQueryOperator(
        task_id='create_schema_nba',
        sql='CREATE SCHEMA IF NOT EXISTS nba;',
        conn_id='nba_conn',
    )

    create_tables_nba = SQLExecuteQueryOperator(
        task_id='create_tables_nba',
        sql= """
            DROP TABLE IF EXISTS nba.dim_ano;
            CREATE TABLE IF NOT EXISTS dim_ano (
            sk_ano SERIAL PRIMARY KEY,
            player_year INTEGER
            );
            DROP TABLE IF EXISTS nba.dim_jogador;
            CREATE TABLE IF NOT EXISTS dim_jogador (
            sk_jogador SERIAL PRIMARY KEY,
            player_name VARCHAR(255),
            player_position VARCHAR(255)
            );
            DROP TABLE IF EXISTS nba.fato_nba;
            CREATE TABLE IF NOT EXISTS fato_nba (
            sk_ano INTEGER,
            sk_jogador INTEGER,
            assists INTEGER,
            blocks INTEGER,
            games INTEGER,
            minutes INTEGER,
            points INTEGER,
            rebounds INTEGER
            )
            ;""",
        conn_id='nba_conn',
    )



    extract >> transform >> create_schema_nba >> create_tables_nba >> [load_dim_jogador, load_dim_ano, load_fato_nba]