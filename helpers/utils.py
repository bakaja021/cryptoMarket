import os
import logging
from multiprocessing import Process

from dotenv import load_dotenv

from helpers.symbols import SUPPORTED_EXCHANGES

load_dotenv(verbose=True)

log = logging.getLogger()
console = logging.StreamHandler()
log.addHandler(console)

config = {
    'user': os.getenv("USER_DB"),
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


def ohlcvt_write_db(cursor, cnx, current_unix, lower_unix, **kwargs):
    exchanges_str = str(SUPPORTED_EXCHANGES)[1:-1]
    sql_select = "select SUM(ABS(size_volume)), MAX(unix_time), MIN(unix_time), MAX(price), MIN(price) from market_coin "
    sql_where = "where pair='" + self.pair + "' and unix_time < " + str(
        current_unix) + " and unix_time >= " + str(lower_unix) + " and exchange IN (" + exchanges_str + ")"
    cursor.execute(sql_select + sql_where)
    result = cursor.fetchall()

    return result

def run_in_parallel(*fns):
    proc = []
    for fn in fns:
        p = Process(target=fn)
        p.start()
        proc.append(p)
    try:
        for p in proc:
            p.join()
    except KeyboardInterrupt:
        print('\nKeyboard received CTRL-C... Bye!!!')
        for p in proc:
            p.terminate()
            p.join()
