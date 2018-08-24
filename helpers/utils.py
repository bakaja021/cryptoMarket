import os
import logging
from dotenv import load_dotenv

load_dotenv(verbose=True)

log = logging.getLogger()
console = logging.StreamHandler()
log.addHandler(console)

config = {
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("HOST"),
    'port': os.getenv("PORT"),
    'database': os.getenv("DATABASE"),
    'raise_on_warnings': True,
}

_sql_write = (os.getenv("SQL_WRITE"))


def log_function(message: str):
    log.warning(message)


def write_db(cursor, cnx, **kwargs):
    sql = _sql_write
    val = (kwargs['exchange'], kwargs['pair'], kwargs['trade_id'], kwargs['unix_time'], kwargs['price'], kwargs['size_volume'], kwargs['created_at'], kwargs['vwap'])
    cursor.execute(sql, val)
    cnx.commit()
