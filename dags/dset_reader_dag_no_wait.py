import random
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount, DriverConfig
from datetime import datetime, timedelta
from airflow.models import Variable

my_email = Variable.get("fail_email")
addr = Variable.get("repo_server_url")

smtp = dict(
    host=Variable.get("notifier_smtp_url"),
    port=Variable.get("notifier_smtp_port"),
    user=Variable.get("notifier_smtp_user"),
    password=Variable.get("notifier_smtp_password"),
)

args = {
    "email": my_email,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

args_with_retries = {
    "email": my_email,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}


nfs_config = {
    "type": "nfs",
    "o": f"addr={addr},nfsvers=4",
    "device": ":/opt/airflow/repos",
}

driver_config = DriverConfig(name="local", options=nfs_config)
mount_nfs = Mount(
    source="local", target="/repos", type="volume", driver_config=driver_config
)


def get_random_moll():
    molls = Variable.get("available_molls").split()
    return random.choice(molls)


with DAG(
    dag_id="dset_historic_reader_dag_no_wait_v1",
    start_date=datetime(2023, 12, 14, 17, 0, 0),
    schedule="4-59/5 * * * *",
    catchup=False,
    tags=["Dades", "Plantmonitor", "Ingesta"],
    default_args=args_with_retries,
    max_active_runs=5,
) as dag:
    repo_name = "somenergia-plant-reader"

    # fixed data time interval
    _query_start_date = Variable.get("dset_test_query_start_date", "2023-12-14T18:00:00")
    query_start_date = datetime.fromisoformat(_query_start_date)
    query_end_date = query_start_date + timedelta(hours=1)

    sampled_moll = get_random_moll()

    # e.g. data_interval_start from airflow 2023-08-13T00:00:00+00:00
    # isoformat utc

    dset_reader_task = DockerOperator(
        api_version="auto",
        task_id="dset_plant_reader",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}",
            repo_name,
        ),
        working_dir=f"/repos/{repo_name}",
        command=(
            "python3 -m scripts.read_dset_api get-historic-readings"
            " --db-url {{ var.value.plantmonitor_db }}"
            " --api-base-url {{ var.value.dset_url }}"
            " --endpoint /api/data"
            " --api-key {{ var.value.dset_apikey }}"
            f" --from-date {query_start_date.strftime('%Y-%m-%dT%H:%M:%S')}"
            f" --to-date {query_end_date.strftime('%Y-%m-%dT%H:%M:%S')}"
            " --wait-min-before-request 0"
            " --dry-run"
            " --schema does_not_matter"
        ),
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove="force",
        retrieve_output=True,
        trigger_rule="none_failed",
        force_pull=True,
    )

if __name__ == "__main__":
    dag.test()
