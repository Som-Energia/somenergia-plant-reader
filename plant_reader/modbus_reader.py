import datetime
import logging
import typing as T

from pymodbus.client import ModbusTcpClient
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    column,
    insert,
    table,
)


class ModbusException(Exception):
    pass


def create_table(conn, table_name, schema: str = "public"):
    meta = MetaData(conn)
    dbtable = Table(
        table_name,
        meta,
        Column("query_time", DateTime(timezone=True)),
        Column("ip", String),
        Column("port", Integer),
        Column("register_address", Integer),
        Column("value", Integer),
        Column("create_date", DateTime(timezone=True)),
        Column("is_valid", Boolean),
        Column("unit", Integer),
        schema=schema,
    )

    dbtable.create(conn, checkfirst=True)


def get_table(table_name, schema: str = "public"):
    return table(
        table_name,
        column("query_time", DateTime(timezone=True)),
        column("ip", String),
        column("port", Integer),
        column("register_address", Integer),
        column("value", Integer),
        column("create_date", DateTime(timezone=True)),
        column("is_valid", Boolean),
        column("unit", Integer),
        schema=schema,
    )


def read_modbus(
    ip: str,
    port: int,
    type: str,
    register_address: int,
    count: int,
    slave: int,
    timeout: int = 20,
):
    client = ModbusTcpClient(
        ip, timeout=timeout, RetryOnEmpty=True, retries=3, port=port
    )
    logging.info("getting registers from inverter")
    if type == "holding":
        registries = client.read_holding_registers(
            register_address, count=count, slave=slave
        )
    elif type == "input":
        registries = client.read_input_registers(
            register_address, count=count, slave=slave
        )
    else:
        raise NotImplementedError("type {} is not implemented".format(type))

    if registries.isError():
        logging.error(registries)
        raise ModbusException(str(registries))

    return registries.registers


def main_read_store(
    conn: str,
    table: str,
    ip: str,
    port: int,
    type: str,
    modbus_tuples: T.List[T.Tuple[int, int, int]],
    schema: str,
):
    dbtable = get_table(table, schema=schema)

    for unit, register_address, count in modbus_tuples:
        try:
            registries = read_modbus(
                ip,
                port,
                type,
                register_address,
                count,
                unit,
            )
            query_time = datetime.datetime.now(datetime.timezone.utc)

            for offset, register in enumerate(registries):
                insert_statement = insert(dbtable).values(
                    query_time=query_time,
                    ip=ip,
                    port=port,
                    register_address=register_address + offset,
                    value=register,
                    create_date=query_time,
                    is_valid=True,
                    unit=unit,
                )

                conn.execute(insert_statement)
        except ModbusException as e:
            logging.error(e)

            insert_statement = insert(dbtable).values(
                query_time=query_time,
                ip=ip,
                port=port,
                register_address=register_address,
                value=None,
                create_date=query_time,
                is_valid=False,
                unit=unit,
            )

            conn.execute(insert_statement)

    return 0
