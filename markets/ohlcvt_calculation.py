import mysql.connector
import time
import threading
from datetime import datetime

from helpers.symbols import SUPPORTED_EXCHANGES, SUPPORTED_PAIRS
from helpers.utils import log_function, write_db


class Ohlcvt:

    def __init__(self, pair, **kwargs):
        self.pair = pair
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def run(self):
        while True:
            current_unix = int(time.time())

            while current_unix % 60 > 0:
                current_unix = int(time.time())
                time.sleep(0.5)

            lower_unix = current_unix - 60



            if result[0][0] is None:

                payload = {
                    'pair': self.pair,
                    'unix_open': lower_unix,
                    'unix_close': current_unix,
                    'open': 0,
                    'high': 0,
                    'low': 0,
                    'close': 0,
                    'volume': 0,
                    'typical_price': 0,
                    'created_at': datetime.utcnow()
                }

                sql = (
                    "INSERT INTO market_ohlcvt (pair, unix_open, unix_close, open, high, low, close, volume, typical_price, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                val = (payload['pair'], payload['unix_open'], payload['unix_close'], payload['open'], payload['high'],
                       payload['low'], payload['close'], payload['volume'], payload['typical_price'],
                       payload['created_at'])
                self.cursor.execute(sql, val)
                self.cnx.commit()

                print(payload)

            else:

                # Prepare open and close SQL queries.
                sql_close = "select price from market_coin where pair='" + self.pair + "' and unix_time = " + str(
                    result[0][1]) + " and exchange IN (" + exchanges_str + ")"
                sql_open = "select price from market_coin where pair='" + self.pair + "' and unix_time = " + str(
                    result[0][2]) + " and exchange IN (" + exchanges_str + ")"
                self.cursor.execute(sql_close)
                result_close = self.cursor.fetchall()
                self.cursor.execute(sql_open)
                result_open = self.cursor.fetchall()
                # print(self.pair + ", c, " + str(len(result_close)) + ", " + str(result_close))
                # print(self.pair + ", o, " + str(len(result_open)) + ", " + str(result_open))

                close_payload = 0
                open_payload = 0

                for x in range(len(result_close)):
                    close_payload += result_close[x][0]

                for y in range(len(result_open)):
                    open_payload += result_open[y][0]

                payload = {
                    'pair': self.pair,
                    'unix_open': lower_unix,
                    'unix_close': current_unix,
                    'open': round(open_payload / len(result_open), 2),
                    'high': round(result[0][3], 2),
                    'low': round(result[0][4], 2),
                    'close': round(close_payload / len(result_close), 2),
                    'volume': round(result[0][0], 4),
                    'typical_price': round((open_payload / len(result_open) + result[0][3] + result[0][
                        4] + close_payload / len(result_close)) / 4, 2),
                    'created_at': datetime.utcnow()
                }

                sql = (
                    "INSERT INTO market_ohlcvt (pair, unix_open, unix_close, open, high, low, close, volume, typical_price, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                val = (payload['pair'], payload['unix_open'], payload['unix_close'], payload['open'], payload['high'],
                       payload['low'],
                       payload['close'], payload['volume'], payload['typical_price'], payload['created_at'])
                self.cursor.execute(sql, val)
                self.cnx.commit()

                print(payload)

            # Reset cursor and wait. Reduce 50 to 45 if the above call takes longer than 10 seconds ... currently 4s
            self.cursor.close()
            time.sleep(50)


for i in supported_pairs:
    thread = ohlcv_feed(i, **config)
    thread.start()