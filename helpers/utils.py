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
_sql_write_ohlcvt = (os.getenv("SQL_WRITE_OHLCVT"))


def log_function(message: str):
    log.warning(message)


def write_db(cursor, cnx, **kwargs):
    sql = _sql_write
    val = (
    kwargs['exchange'], kwargs['pair'], kwargs['trade_id'], kwargs['unix_time'], kwargs['price'], kwargs['size_volume'],
    kwargs['created_at'], kwargs['vwap'])
    cursor.execute(sql, val)
    cnx.commit()


def read_db_result_null(cursor, **kwargs):
    exchanges_str = str(SUPPORTED_EXCHANGES)[1:-1]
    sql_select = """select SUM(ABS(size_volume)), MAX(unix_time), MIN(unix_time), MAX(price), MIN(price) from market_coin """
    sql_where = "where pair='" + kwargs['pair'] + "' and unix_time < " + str(
        kwargs['current_unix']) + " and unix_time >= " + str(
        kwargs['lower_unix']) + " and exchange IN (" + exchanges_str + ")"
    cursor.execute(sql_select + sql_where)
    result = cursor.fetchall()

    return result


def write_db_result(cursor, cnx, **kwargs):
    sql = _sql_write_ohlcvt
    val = (kwargs['pair'], kwargs['unix_open'], kwargs['unix_close'], kwargs['open'], kwargs['high'], kwargs['low'],
           kwargs['close'], kwargs['volume'], kwargs['typical_price'], kwargs['created_at'])
    cursor.execute(sql, val)
    cnx.commit()


def read_db_result_not_null(cursor, **kwargs):
    exchanges_str = str(SUPPORTED_EXCHANGES)[1:-1]
    sql_close = "select price from market_coin where pair='" + kwargs['pair'] + "' and unix_time = " + str(
        kwargs['result'][0][1]) + " and exchange IN (" + exchanges_str + ")"
    sql_open = "select price from market_coin where pair='" + kwargs['pair'] + "' and unix_time = " + str(
        kwargs['result'][0][2]) + " and exchange IN (" + exchanges_str + ")"
    cursor.execute(sql_close)
    result_close = cursor.fetchall()
    cursor.execute(sql_open)
    result_open = cursor.fetchall()

    close_payload = sum((result_close[x][0] for x in range(len(result_close))))
    open_payload = sum((result_open[x][0]for x in range(len(result_open))))

    return open_payload, close_payload, result_open, result_close


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
