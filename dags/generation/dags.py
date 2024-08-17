import functools
import json
from datetime import datetime
from typing import Union, List, Tuple, Callable

from airflow import DAG
from airflow.exceptions import *
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from generation.fields import GraphFields, DagFields
from generation.params import generate_params

graph_types = Union[
    TaskGroup, BaseOperator, List[Union[TaskGroup, BaseOperator]], Tuple[Union[TaskGroup, BaseOperator], ...]
]


def generate_task(
        task_source: Union[str, dict],
        task_group: TaskGroup,
        **kwargs: Any
):
    """
    Generates an Airflow task (EmptyOperator) based on the provided task source.

    :param task_source: The source of the task, which can be either a string (task ID) or a dictionary containing task properties.
    :param task_group: The TaskGroup to which this task will belong.
    :param kwargs: Additional arguments that may be passed to the task creation process.
    :return: An instance of EmptyOperator representing the generated task.
    :raises ValueError: If the task source is neither a string nor a dictionary.
    """
    if isinstance(task_source, dict):
        task_id = task_source.get(GraphFields.id)
    elif isinstance(task_source, str):
        task_id = task_source
    else:
        raise ValueError('Wrong definition of a task.')

    return EmptyOperator(
        task_id=task_id,
        task_group=task_group
    )


def generate_dag(
        source: Union[dict, str],
        task_generator: Callable = generate_task
):
    """
    This function generates a DAG based on the given description. The description of the DAG to be generated can be either a dictionary or a path to a JSON file that describes the DAG.

    :param source: The description of the DAG to be generated.
    :param task_generator: The function that generates the tasks for the DAG.
    """
    source = get_dict_from_source(source)
    with DAG(
            dag_id=source.get(DagFields.id),
            start_date=datetime.strptime(source.get(DagFields.start_date), '%Y-%m-%d'),
            params=generate_params(source.get(DagFields.params, None)),
            **DagFields.filter_dict(source)
    ):
        start = EmptyOperator(task_id="start")

        end = EmptyOperator(task_id="end")

        generate_and_construct_graph(source['graph'], start, end, task_generator, dag_extra=source.get("extra", {}))


def generate_and_construct_graph(
        source: Union[dict, str],
        start: Union[TaskGroup, BaseOperator],
        end: Union[TaskGroup, BaseOperator],
        task_generator: Callable = generate_task,
        **kwargs: Any
):
    """
    Generates a DAG graph based on the provided source and constructs it by linking the tasks between the start and end nodes.

    :param source: The source of the DAG graph, which can be either a dictionary or a JSON file path describing the graph structure.
    :param start: The starting task or TaskGroup for the DAG.
    :param end: The ending task or TaskGroup for the DAG.
    :param task_generator: A function to generate tasks in the graph. Defaults to `generate_task`.
    :param kwargs: Additional arguments that may be passed to the graph generation process.
    """
    construct_graph(generate_graph(source, task_generator, **kwargs), start, end)


def construct_graph(
        middle: graph_types,
        start: Union[TaskGroup, BaseOperator],
        end: Union[TaskGroup, BaseOperator]
):
    """
    Constructs a DAG graph by linking tasks or TaskGroups between the start and end nodes.

    :param middle: The middle portion of the graph, which can be a TaskGroup, a single task, a list of tasks, or a tuple of tasks.
    :param start: The starting task or TaskGroup for the DAG.
    :param end: The ending task or TaskGroup for the DAG.
    """
    if isinstance(middle, (TaskGroup, BaseOperator)):
        start >> middle >> end
    elif isinstance(middle, list):
        for task in middle:
            construct_graph(task, start, end)
    elif isinstance(middle, tuple):
        start >> middle[0]
        middle[1] >> end


def generate_graph(
        source: Union[dict, str],
        task_generator: Callable = generate_task,
        **kwargs: Any
) -> graph_types:
    """
    This function generates a DAG graph based on the given description. The description of the DAG graph to be generated can be either a dictionary or a path to a JSON file that describes the DAG.

    :param source: The description of the DAG graph to be generated.
    :param task_generator: The function that generates the tasks for the DAG.
    :return: Returns the result of the generate_group_and_task() function.
    """
    source = get_dict_from_source(source)

    return generate_groups_and_tasks(source, task_generator, **kwargs)


def generate_groups_and_tasks(
        graph: dict,
        task_generator: Callable = generate_task,
        parent_group: TaskGroup = None,
        **kwargs: Any
) -> graph_types:
    """
    Recursive generation of TaskGroups and Tasks based on the given graph description.

    :param graph: The description of the graph.
    :param task_generator: The function that generates the tasks for the DAG.
    :param parent_group: The parent group to which the elements generated in this function call belong.
    :param kwargs: Additional arguments.
    :return: The generated TaskGroup, list of tasks or Tuple of first and last tasks in the sequential group.
    """
    if GraphFields.id in graph:
        group = TaskGroup(
            group_id=graph.get(GraphFields.id),
            parent_group=parent_group,
            **GraphFields.filter_dict(graph)
        )
    else:
        group = parent_group

    kwargs = {**kwargs, **{'graph_source': graph}}

    items = []

    for item in graph.get(GraphFields.items):
        if is_group(item):
            items.append(generate_groups_and_tasks(item, task_generator, group, **kwargs))
        else:
            items.append(task_generator(task_source=item, task_group=group, **kwargs))

    def rshift_graph(current_item, next_item):
        if isinstance(next_item, list):
            return [rshift_graph(current_item, current_next_item) for current_next_item in next_item]
        elif isinstance(next_item, tuple):
            current_item >> next_item[0]
            return next_item[1]

        return current_item >> next_item

    if is_seq(graph) and len(items) > 1:
        functools.reduce(rshift_graph, items[1:], items[0])

    if GraphFields.id in graph:
        return group
    else:
        if is_seq(graph):
            return items[0], items[-1]
        else:
            return items


def get_dict_from_source(source: Union[dict, str]):
    """
   Converts a source into a dictionary, whether it is a JSON string, a file path to a JSON file, or already a dictionary.

   :param source: The source of the data, which can be a dictionary, a JSON string, or a file path to a JSON file.
   :return: A dictionary representation of the input source.
   :raises ValueError: If the input is not a dictionary, a JSON string, or a valid path to a JSON file.
   """
    if isinstance(source, str):
        try:
            return json.loads(source)
        except json.JSONDecodeError:
            with open(source, 'r') as f:
                return json.load(f)
    elif not isinstance(source, dict):
        raise ValueError("Input must be either a dictionary, a JSON string, or a valid path to a JSON file")
    else:
        return source


def is_seq(graph: dict) -> bool:
    return graph.get(GraphFields.type, 'seq') == 'seq'


def is_par(graph: dict) -> bool:
    return graph.get(GraphFields.type, 'seq') == 'par'


def is_group(graph: dict) -> bool:
    return isinstance(graph, dict) and GraphFields.items in graph
