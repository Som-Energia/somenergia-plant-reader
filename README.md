# somenergia-modbus-reader

A lightweight modbus reader for somenergia's plants

## overview

The `main` script can be run manually or by airflow using the dag

```bash
python scripts.main --ip <ip> --port <port> --type <registry_type> --address <address> --count <count> --dbapi <dbapi> --table <table>
```

`registry_type` is the modbus registry type, essentially `holding` or `input`.

Run `python scripts.main --help` for a longer description of each option.

The script will store the result in `table` of the dbapi which must exist beforehand with the following columns

`query_time ip registry_address value create_date is_valid`

with one row for each registry read.

If the reading failed it will be logged with a NULL value and is_valid set to false

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

just run `pytest` on the root directory

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