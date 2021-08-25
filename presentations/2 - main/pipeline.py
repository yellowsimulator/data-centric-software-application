
# [START import_module]
from datetime import timedelta, datetime
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import PythonOperator
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'end_date': datetime(2021, 12, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1),
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    task1 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_json,
    )

    task2 = PythonOperator(
        task_id='trasform_data',
        depends_on_past=True,
        python_callable=transform_data,
        retries=3,
    )

    task3 = PythonOperator(
        task_id='load_data_to_azure_datalake',
        depends_on_past=True,
        python_callable=load_data_to_azure_datalake,
        retries=3,
    )

    task4 = PythonOperator(
        task_id='load_data_to_postgres',
        depends_on_past=True,
        python_callable=load_data_to_postgresdb,
        retries=3,
    )
    # [END basic_task]

    # [START documentation]
    task1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beggining of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    # [END documentation]


    task1 >> [t2, t3]
# [END tutorial]