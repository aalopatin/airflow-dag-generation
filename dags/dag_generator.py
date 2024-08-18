from typing import Union, Any

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from generation.dags import generate_dag
from generation.fields import GraphFields


def task_generator(
        task_source: Union[dict, str],
        task_group: TaskGroup,
        **kwargs: Any
):

    if isinstance(task_source, dict):
        operator_name = task_source.get(GraphFields.operator)
        if operator_name == "SparkSubmitOperator":
            spark_conf_dag = kwargs.get("dag_extra").get("spark").get("conf")
            spark_conf_group = kwargs.get("graph_source").get("extra").get("spark").get("conf")
            return SparkSubmitOperator(
                task_id=task_source.get(GraphFields.id),
                task_group=task_group,
                conf={**spark_conf_dag, **spark_conf_group},
                **GraphFields.filter_dict(task_source)
            )
        elif operator_name == "CustomGroupGenerator":

            with TaskGroup(group_id=task_source.get(GraphFields.id), parent_group=task_group) as group:
                bash1 = BashOperator(task_id="bash1", bash_command="echo bash1 DONE!")
                bash2 = BashOperator(task_id="bash2", bash_command="echo bash2 DONE!")
                bash3 = BashOperator(task_id="bash3", bash_command="echo bash3 DONE!")
                [bash1, bash2] >> bash3

            return group

    return None


dags = Variable.get("dags", deserialize_json=True)

for dag in dags:
    generate_dag(dag, task_generator)
