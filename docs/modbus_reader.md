# Basic comments

The bandwidth of the modbus is limited, so if we must request more than 50 addresses it will have to be split into several calls.

Some of them might fail.

Check the options with `python -m scripts.main --help`

e.g.
```bash
python -m scripts.main print-multiple-readings 185.208.65.134 1502 input 33:55:2 32:55:20
```

the modbus tuples are of formate unit:register_address:count. So the previous example reads unit 33 and 32, two and twenty registers respectively both starting at address 55.

To store results in the database you can run

```bash
python3 -m scripts.main get-readings <dbapi> modbus_readings planta-asomada.somenergia.coop 1502 input 3:0:54 3:151:10 32:54:17 33:54:17'
```

Check the dag for an airflow example