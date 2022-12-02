import logging
import datetime
from pymodbus.client import ModbusTcpClient

from sqlalchemy import table, column, insert, MetaData, Integer, DateTime, Boolean, Column, String, Table

class ModbusException(Exception):
    pass

def create_table(conn, table_name):

    meta = MetaData(conn)
    dbtable = Table(table_name, meta,
        Column("query_time", DateTime),
        Column("ip", String),
        Column("port", Integer),
        Column("register_address", String),
        Column("value", Integer),
        Column("create_date", DateTime),
        Column("is_valid", Boolean)
    )

    dbtable.create(conn, checkfirst=True)


def get_table(table_name):

    return table(
        table_name,
        column("query_time", DateTime),
        column("ip", String),
        column("port", Integer),
        column("register_address", String),
        column("value", Integer),
        column("create_date", DateTime),
        column("is_valid", Boolean)
    )

def read_modbus(ip, port, type, register_address, count, slave, timeout=20):
    client = ModbusTcpClient(ip, timeout=timeout,
                RetryOnEmpty=True,
                retries=3,
                port=port
            )
    logging.info("getting registers from inverter")
    if type == 'holding':
        registries = client.read_holding_registers(
            register_address,
            count=count,
            unit=slave
        )
    elif type == 'input':
        registries = client.read_input_registers(
            register_address,
            count=count,
            unit=slave
        )
    else:
        raise NotImplementedError("type {} is not implemented".format(type))

    if registries.isError():
        logging.error(registries)
        raise ModbusException(str(registries))

    return registries.registers


def main_read_store(conn, table, ip, port, type, register_address_count, slave):

    dbtable = get_table(table)

    start_count_tuples = list(zip(register_address_count[0::2],register_address_count[1::2]))

    for register_address, count in start_count_tuples:
        try:
            registries = read_modbus(ip, port, type, register_address, count, slave)
            query_time = datetime.datetime.now(datetime.timezone.utc)

            for offset,register in enumerate(registries):

                insert_statement = insert(dbtable).values(
                    query_time=query_time,
                    ip=ip,
                    port=port,
                    register_address=register_address+offset,
                    value=register,
                    create_date=query_time,
                    is_valid=True
                )

                result = conn.execute(insert_statement)
        except ModbusException as e:

            logging.error(e)

            insert_statement = insert(dbtable).values(
                query_time=query_time,
                ip=ip,
                port=port,
                register_address=register_address,
                value=None,
                create_date=query_time,
                is_valid=False
            )

            result = conn.execute(insert_statement)

    return 0