import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dag_manual_with_groups",
    start_date=datetime.datetime(2024, 8, 1),
    schedule=None
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("main_group") as main_group:

        with TaskGroup("group1") as group1:
            task1_1 = EmptyOperator(task_id="task1_1")
            task1_2 = EmptyOperator(task_id="task1_2")

        task2 = EmptyOperator(task_id="task2")

        with TaskGroup("group3_1") as group3_1:
            task3_1_1 = EmptyOperator(task_id="task3_1_1")
            task3_1_2 = EmptyOperator(task_id="task3_1_2")
            task3_1_3 = EmptyOperator(task_id="task3_1_3")
            task3_1_1 >> task3_1_2 >> task3_1_3

        with TaskGroup("group3_2") as group3_2:
            task3_2_1 = EmptyOperator(task_id="task3_2_1")
            task3_2_2 = EmptyOperator(task_id="task3_2_2")
            task3_2_3 = EmptyOperator(task_id="task3_2_3")
            task3_2_1 >> task3_2_2 >> task3_2_3

        with TaskGroup("group3_3") as group3_3:
            task3_3_1 = EmptyOperator(task_id="task3_3_1")
            task3_3_2 = EmptyOperator(task_id="task3_3_2")
            task3_3_3 = EmptyOperator(task_id="task3_3_3")
            task3_3_1 >> task3_3_2 >> task3_3_3

        with TaskGroup("group4") as group4:
            task4_1 = EmptyOperator(task_id="task4_1")
            task4_2 = EmptyOperator(task_id="task4_2")
            task4_3 = EmptyOperator(task_id="task4_3")
            task4_1 >> task4_2 >> task4_3

        with TaskGroup("group5") as group5:
            task5_1 = EmptyOperator(task_id="task5_1")
            task5_2 = EmptyOperator(task_id="task5_2")
            task5_3 = EmptyOperator(task_id="task5_3")

        task6 = EmptyOperator(task_id="task6")

        with TaskGroup("group7_1") as group7_1:
            task7_1_1 = EmptyOperator(task_id="task7_1_1")
            task7_1_2 = EmptyOperator(task_id="task7_1_2")
            task7_1_3 = EmptyOperator(task_id="task7_1_3")

        with TaskGroup("group7_2") as group7_2:
            task7_2_1 = EmptyOperator(task_id="task7_2_1")
            task7_2_2 = EmptyOperator(task_id="task7_2_2")
            task7_2_3 = EmptyOperator(task_id="task7_2_3")

        with TaskGroup("group7_3") as group7_3:
            task7_3_1 = EmptyOperator(task_id="task7_3_1")
            task7_3_2 = EmptyOperator(task_id="task7_3_2")
            task7_3_3 = EmptyOperator(task_id="task7_3_3")

        group1 >> task2

        task2 >> [group3_1, group3_2, group3_3]

        [group3_1, group3_2, group3_3] >> group4

        group4 >> group5 >> task6

        task6 >> [group7_1, group7_2, group7_3]

        start >> main_group >> end
