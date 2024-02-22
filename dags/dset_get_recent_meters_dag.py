import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import DriverConfig, Mount

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


dag_doc = """
### DAG to read data for meters only

Els comptadors arriben amb un dia de retard. Aquest dag s'encarrega de llegir dades
nomès de comptadors, filtrant per la seva antiguitat.

Les consultes nomès es fan a nivell de signal_id, és a dir, fan servir un endpoint
diferent al utilitzat en el dag que llegeix la resta de aparells.
"""


with DAG(
    dag_id="dset_reader_meters_dag_v1",
    start_date=datetime(2022, 12, 2),
    schedule="4-59/5 * * * *",
    catchup=False,
    tags=["dades", "jardiner", "dset"],
    default_args=args,
    max_active_runs=1,
) as dag:
    repo_name = "somenergia-plant-reader"

    sampled_moll = get_random_moll()

    dset_read_meters = DockerOperator(
        api_version="auto",
        task_id="dset_plant_reader_alternative",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-app:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command=(
            "python3 -m scripts.read_dset_meters"
            " get-historic-readings-meters"
            " --db-url {{ var.value.plantmonitor_db }}"
            " --api-base-url {{ var.value.dset_url }}"
            " --api-key {{ var.value.dset_apikey }}"
            " --schema lake"
            " --apply-k-value"
        ),
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove="force",
        retrieve_output=True,
        trigger_rule="all_done",
        force_pull=True,
    )


if __name__ == "__main__":
    dag.test()
