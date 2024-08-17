import datetime
import functools
import operator

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_generate_simple_example",
    start_date=datetime.datetime(2024, 8, 1),
    schedule=None
):
    tasks_names = ['start', 'task1','task2','task3','task4','task5', 'end']

    tasks = [EmptyOperator(task_id=task) for task in tasks_names]
    functools.reduce(operator.rshift, tasks[1:], tasks[0])
