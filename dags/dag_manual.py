import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_manual",
    start_date=datetime.datetime(2024, 8, 1),
    schedule=None
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task1_1 = EmptyOperator(task_id="task1_1")
    task1_2 = EmptyOperator(task_id="task1_2")

    task2 = EmptyOperator(task_id="task2")

    task3_1_1 = EmptyOperator(task_id="task3_1_1")
    task3_1_2 = EmptyOperator(task_id="task3_1_2")
    task3_1_3 = EmptyOperator(task_id="task3_1_3")

    task3_2_1 = EmptyOperator(task_id="task3_2_1")
    task3_2_2 = EmptyOperator(task_id="task3_2_2")
    task3_2_3 = EmptyOperator(task_id="task3_2_3")

    task3_3_1 = EmptyOperator(task_id="task3_3_1")
    task3_3_2 = EmptyOperator(task_id="task3_3_2")
    task3_3_3 = EmptyOperator(task_id="task3_3_3")

    task4_1 = EmptyOperator(task_id="task4_1")
    task4_2 = EmptyOperator(task_id="task4_2")
    task4_3 = EmptyOperator(task_id="task4_3")

    task5_1 = EmptyOperator(task_id="task5_1")
    task5_2 = EmptyOperator(task_id="task5_2")
    task5_3 = EmptyOperator(task_id="task5_3")

    task6 = EmptyOperator(task_id="task6")

    task7_1_1 = EmptyOperator(task_id="task7_1_1")
    task7_1_2 = EmptyOperator(task_id="task7_1_2")
    task7_1_3 = EmptyOperator(task_id="task7_1_3")

    task7_2_1 = EmptyOperator(task_id="task7_2_1")
    task7_2_2 = EmptyOperator(task_id="task7_2_2")
    task7_2_3 = EmptyOperator(task_id="task7_2_3")

    task7_3_1 = EmptyOperator(task_id="task7_3_1")
    task7_3_2 = EmptyOperator(task_id="task7_3_2")
    task7_3_3 = EmptyOperator(task_id="task7_3_3")

    start >> [task1_1, task1_2]

    [task1_1, task1_2] >> task2

    task3_1_1 >> task3_1_2 >> task3_1_3
    task3_2_1 >> task3_2_2 >> task3_2_3
    task3_3_1 >> task3_3_2 >> task3_3_3

    task2 >> [task3_1_1, task3_2_1, task3_3_1]

    [task3_1_3, task3_2_3, task3_3_3] >> task4_1 >> task4_2 >> task4_3

    task4_3 >> [task5_1, task5_2, task5_3] >> task6

    task6 >> [task7_1_1, task7_1_2, task7_1_3]
    task6 >> [task7_2_1, task7_2_2, task7_2_3]
    task6 >> [task7_3_1, task7_3_2, task7_3_3]

    [task7_1_1, task7_1_2, task7_1_3] >> end
    [task7_2_1, task7_2_2, task7_2_3] >> end
    [task7_3_1, task7_3_2, task7_3_3] >> end
