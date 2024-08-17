from airflow.models import Variable

from generation.dags import generate_dag


dags = Variable.get("dags", deserialize_json=True)

for dag in dags:
    generate_dag(dag)
