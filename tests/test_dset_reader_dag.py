import os
import pytest
from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    os.environ["AIRFLOW_VAR_AVAILABLE_MOLLS"] = "fakemoll.somenergia.coop"
    return DagBag(dag_folder='dags',include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="dset_historic_reader_dag_v3")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

