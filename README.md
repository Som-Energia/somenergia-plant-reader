# somenergia-modbus-reader

A lightweight modbus reader for somenergia's plants

## overview

The `main` script can be run manually or by airflow using the dag

```bash
python -m scripts.main get-readings <dbapi> <table> <ip> <port> <registry_type>  <slave> <address> <count> ... <address> <count>
```

`registry_type` is the modbus registry type, essentially `holding` or `input`. `slave` is also known as unit and it refers to the modbus slave number with is typically 1. `<address> <count>` can be repeated as many address+count tuples you'll need to read from that ip.

Run `python scripts.main --help` for a longer description of each option.

The script will store the result in `table` of the dbapi which must exist beforehand with the following columns

`query_time ip port registry_address value create_date is_valid`

with one row for each registry read.

You can initialize the table running

```bash
python -m scripts.main create-table <dbapi> <table>
```

If the reading failed it will be logged with a NULL value and is_valid set to false

`<dbapi>` can also be `testing`, `pre` or `prod` if you have `.env.<target>` dot-files. Check .env.example for the expected syntax.

## install dependencies

```bash
pip install poetry
poetry install
```

alternativelly you can do

```bash
pip install -r requirements.txt
```

## testing

just run `pytest` on the root directory.  Or `pytest --dset` to run tests against dset production api.

## run

`typer` will tell you what arguments you need to run the notify_alarms script. You will have to provide a dbapi string or the placeholders `prod` or `pre` which will read `.env.prod` and `.env.pre` respectivelly

`python ./scripts/read_plant --help`

## deploy

deploy is in continuous delivery over main branch.

## update requirements

We use poetry to maintain the requirements, but we can update the requirements like so:

`poetry export --without=dev --without-hashes --format=requirements.txt --output requirements.txt`

## update documentation

We use mkdocs to serve extra documentation and adrs

`mkdocs serve`

## Passos per afegir ci/cd a un projecte

Airflow necessita tenir les imatges al harbor per a poder executar python en entorns limitats. Podeu pujar-ho manualment

Partint de somenergia-jardiner o somenergia-plant-reader (que no té dbt)

- [ ] crear carpeta containers/<service>
	- [ ] definir Dockerfiles per serveis del projecte
		- [ ] app
		- [ ] dbt-docs (opcional)
		- [ ] mkdocs
- [ ] crear .gitlab-ci.yml
- [ ] configurar variables d'entorn de cicd
- [ ] configurar mirall de gitlab cap a github

docker compose config will tell you if variables are missing
you'll need a login to our docker registry at harbour.somenergia.coop


