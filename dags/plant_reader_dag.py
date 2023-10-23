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
    "retry_delay": timedelta(minutes=3),
}


nfs_config = {
    "type": "nfs",
    "o": f"addr={addr},nfsvers=4",
    "device": ":/opt/airflow/repos",
}


def get_random_moll():
    molls = Variable.get("available_molls").split()
    return random.choice(molls)


driver_config = DriverConfig(name="local", options=nfs_config)
mount_nfs = Mount(
    source="local", target="/repos", type="volume", driver_config=driver_config
)


with DAG(
    dag_id="plant_reader_dag",
    start_date=datetime(2022, 12, 2),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["Dades", "Plantmonitor"],
    default_args=args,
) as dag:
    repo_name = "somenergia-plant-reader"

    sampled_moll = get_random_moll()

    plant_reader_task_alternative = DockerOperator(
        api_version="auto",
        task_id="plant_reader_alternative",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m scripts.main get-readings "{{ var.value.plantmonitor_db }}"\
                 modbus_readings planta-asomada.somenergia.coop 1502 input 3:0:82 32:54:16 33:54:16 --schema lake',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove='force',
        retrieve_output=True,
        trigger_rule="none_failed",
        force_pull=True,
    )


with DAG(
    dag_id="plant_printer_dag",
    start_date=datetime(2023, 1, 2),
    schedule=None,
    catchup=False,
    tags=["Dades", "Plantmonitor"],
    default_args=args,
) as dag:
    repo_name = "somenergia-plant-reader"

    sampled_moll = get_random_moll()

    plant_printer_task = DockerOperator(
        api_version="auto",
        task_id="plant_printer",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command="python3 -m scripts.main print-multiple-readings planta-asomada.somenergia.coop 1502 input 120:11:10",
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove='force',
        retrieve_output=True,
        trigger_rule="none_failed",
    )
