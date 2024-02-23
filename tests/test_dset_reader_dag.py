import os
import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    os.environ["AIRFLOW_VAR_AVAILABLE_MOLLS"] = "fakemoll.somenergia.coop"
    os.environ["AIRFLOW_VAR_FAIL_EMAIL"] = "nobody@example.com"
    os.environ["AIRFLOW_VAR_REPO_SERVER_URL"] = "nowhere"
    os.environ["AIRFLOW_VAR_NOTIFIER_SMTP_URL"] = "nowhere"
    os.environ["AIRFLOW_VAR_NOTIFIER_SMTP_PORT"] = "1234"
    os.environ["AIRFLOW_VAR_NOTIFIER_SMTP_USER"] = "nobody"
    os.environ["AIRFLOW_VAR_NOTIFIER_SMTP_PASSWORD"] = "nothing"
    os.environ["AIRFLOW_VAR_DSET_TEST_QUERY_START_DATE"] = "2023-12-14T18:00:00"
    return DagBag(dag_folder="dags", include_examples=False)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="dset_historic_reader_dag_v3")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1
