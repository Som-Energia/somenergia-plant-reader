# The Dset Api reader

follows the same logic as the modbus reader but stores JSONB

[dset api docs](https://drive.google.com/file/d/1Yac3IIjauFo--ynyB4ccF1v9RnVRydWN/view?usp=sharing)

Particularities of our endpoints

with the queryparam `sig_detail` set to `true` we will additionally get

```
signal_device_id
signal_device_type
signal_sensor_id
```

The postman collection of the api calls can be found [here](https://documenter.getpostman.com/view/6601984/2s946cga7J)