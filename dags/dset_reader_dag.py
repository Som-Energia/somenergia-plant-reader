import random
from airflow import DAG
from datetime import timedelta
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
    "retry_delay": timedelta(minutes=5),
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


# TODO should be 5 minutal when dset changes the frequency
with DAG(
    dag_id="dset_reader_dag",
    start_date=datetime(2022, 12, 2),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["Dades", "Plantmonitor"],
    default_args=args,
) as dag:
    repo_name = "somenergia-plant-reader"

    sampled_moll = get_random_moll()

    dset_reader_task = DockerOperator(
        api_version="auto",
        task_id="dset_plant_reader",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m scripts.read_dset_api get-readings "{{ var.value.plantlake_dbapi }}"\
                 "{{var.value.dset_url}}" "{{ var.value.dset_apikey}}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
        force_pull=True,
    )

    dset_reader_task_alternative = DockerOperator(
        api_version="auto",
        task_id="dset_plant_reader_alternative",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m scripts.read_dset_api get-readings "{{ var.value.plantmonitor_db }}"\
                 "{{var.value.dset_url}}" "{{ var.value.dset_apikey}}" --schema lake',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
        force_pull=True,
    )

    dset_reader_task >> dset_reader_task_alternative

    # INFO you need to manually create the table with python3 -m scripts.read_dset_api setupdb <dbapi> dset_readings
