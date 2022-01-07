from airflow.models import DagBag


def test_dags_load_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    assert dag_bag.import_errors == {}


def test_if_dags_exists(load_dag_names, get_dags_import_table):
    dag_bag = DagBag(include_examples=False)

    list_dags = []
    for name in load_dag_names:
        dag = dag_bag.get_dag(f'import_table_{name}')
        list_dags.append(dag)

    assert len(list_dags) == len(get_dags_import_table)


def test_if_dag_contains_tasks():
    """
    Every dag minimum contains: start and end tasks
    """
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag('import_table_tbl')

    assert len(dag.tasks) > 1
