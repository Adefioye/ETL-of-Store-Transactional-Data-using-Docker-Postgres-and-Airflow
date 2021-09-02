from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email import EmailOperator

date_of_transaction = "2019-11-26"

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 9, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    dag_id="store_dag",
    default_args=default_args,
    schedule_interval="@daily",
    template_searchpath=['/opt/airflow/sql_files']
)

# Check to see if file is present using FileSensor
is_store_file_available = FileSensor(
    task_id="is_store_file_available",
    fs_conn_id="file_path",
    filepath="raw_store_transactions.csv",
    poke_interval=5,
    timeout=20,
    dag=dag
)

# Clean raw store trasaction data
clean_raw_data = PythonOperator(
    task_id="clean_raw_data",
    python_callable=data_cleaner,
    dag=dag
)

# Create a table in postgres for storing the clean transaction data
create_postgres_table = PostgresOperator(
    task_id="create_postgres_table",
    postgres_conn_id="postgres_conn",
    sql="create_table.sql",
    dag=dag
)

# Insert data into the 
insert_into_postgres_table = PostgresOperator(
    task_id="insert_into_postgres_table",
    postgres_conn_id="postgres_conn",
    sql="insert_table.sql",
    dag=dag
)

# Query the postgres table for location and store-based profit
store_and_location_profit = PostgresOperator(
    task_id="store_and_location_profit",
    postgres_conn_id="postgres_conn",
    sql="location_and_store_profit.sql",
    dag=dag
)

# Rename the location_wise_profit.csv to reflect the date of transaction
concat_date_to_location_based_profit = BashOperator(
    task_id="concat_date_to_location_based_profit",
    bash_command="cat /opt/airflow/store_files/location-based-profit.csv && mv /opt/airflow/store_files/location-based-profit.csv /opt/airflow/store_files/location-based-profit-%s.csv" % date_of_transaction,
    dag=dag
)

# Rename the store_wise_profit.csv to reflect the date of transaction
concat_date_to_store_based_profit = BashOperator(
    task_id="concat_date_to_store_based_profit",
    bash_command="cat /opt/airflow/store_files/store-based-profit.csv && mv /opt/airflow/store_files/store-based-profit.csv /opt/airflow/store_files/store-based-profit-%s.csv" % date_of_transaction,
    dag=dag
)

# Rename the raw_transaction data to include day of transaction to avoid ambiguity. Since, the client is gonna send
# the report the next day
rename_raw_store_transaction_data = BashOperator(
    task_id="rename_raw_store_transaction_data",
    bash_command="mv /opt/airflow/store_files/raw_store_transactions.csv /opt/airflow/store_files/raw_store_transactions_%s.csv" % date_of_transaction,
    dag=dag
)




is_store_file_available >> clean_raw_data >> create_postgres_table >> insert_into_postgres_table

insert_into_postgres_table >> store_and_location_profit >> concat_date_to_location_based_profit

concat_date_to_location_based_profit >> concat_date_to_store_based_profit >> rename_raw_store_transaction_data







