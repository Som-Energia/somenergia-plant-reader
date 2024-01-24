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
    dag_id="dset_reader_dag_v2",
    start_date=datetime(2022, 12, 2),
    schedule="4-59/5 * * * *",
    catchup=False,
    tags=["dades", "jardiner", "dset"],
    default_args=args,
    max_active_runs=1,
) as dag:
    repo_name = "somenergia-plant-reader"

    sampled_moll = get_random_moll()

    dset_reader_task_alternative = DockerOperator(
        api_version="auto",
        task_id="dset_plant_reader_alternative",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command=(
            "python3 -m scripts.read_dset_api"
            " get-readings"
            " --db-url {{ var.value.plantmonitor_db }}"
            " --api-base-url {{ var.value.dset_url }}"
            " --api-key {{ var.value.dset_apikey }}"
            " --schema lake"
        ),


        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove="force",
        retrieve_output=True,
        trigger_rule="all_done",
        force_pull=True,
    )

    # INFO you need to manually create the table with python3 -m scripts.read_dset_api setupdb <dbapi> dset_readings


with DAG(
    dag_id="dset_historic_reader_dag_v3",
    start_date=datetime(2023, 8, 1),
    schedule="4-59/5 * * * *",
    catchup=False,
    tags=["Dades", "jardiner", "Ingesta", "dset"],
    default_args=args_with_retries,
    max_active_runs=5,
) as dag:
    repo_name = "somenergia-plant-reader"

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
            " --endpoint /api/data/ISO_FORMAT"
            " --api-key {{ var.value.dset_apikey }}"
            " --from-date {{ data_interval_start }}"
            " --to-date {{ data_interval_end }}"
            " --schema lake"
            " --sig-detail"
            " --apply-k-value"
            " --returnNullValues"
            " --query-timeout {{ var.value.get('dset_query_timeout_seconds', 40) | int }}"
        ),
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove="force",
        retrieve_output=True,
        trigger_rule="none_failed",
        force_pull=True,
    )


dset_daily_dag_doc = """
### Daily dag

Els comptadors arriben amb un dia de retard. Aquest és un dag diari que fa exactament el mateix que el
dag històric precedent, però amb un offset d'un dia.

Després tenim un camí en el nostre pipe que materialitza un dia més tard.
"""

with DAG(
    dag_id="dset_historic_reader_daily_dag",
    start_date=datetime(2023, 11, 1),
    schedule="0 5 * * *",
    catchup=True,
    tags=["dades", "jardiner", "ingesta", "dset"],
    default_args=args_with_retries,
    max_active_runs=5,
    doc_md=dset_daily_dag_doc
) as dag:
    repo_name = "somenergia-plant-reader"

    sampled_moll = get_random_moll()

    # e.g. data_interval_start from airflow 2023-08-13T00:00:00+00:00
    # isoformat utc

    dset_reader_task = DockerOperator(
        api_version="auto",
        task_id="dset_plant_reader_daily",
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
            " --endpoint /api/data/ISO_FORMAT"
            " --api-key {{ var.value.dset_apikey }}"
            " --from-date {{ data_interval_start }}"
            " --to-date {{ data_interval_end }}"
            " --schema lake"
            " --sig-detail"
            " --apply-k-value"
            " --returnNullValues"
            " --query-timeout {{ var.value.get('dset_query_timeout_seconds', 40) | int }}"
            " --request-time-offset-min 305"
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
