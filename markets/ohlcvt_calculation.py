import mysql.connector
import time
import threading
from datetime import datetime

from helpers.utils import log_function, read_db_result_null, write_db_result, read_db_result_not_null


class Ohlcvt:
    """
    Calculating open, close, high, low, volume and typical_price price
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DASH-USD
    """

    def __init__(self, pair, **kwargs):
        self.pair = pair
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def run(self):
        while True:
            current_unix = int(time.time())

            while current_unix % 60 > 0:
                current_unix = int(time.time())
                time.sleep(1)

            lower_unix = current_unix - 60

            result = read_db_result_null(self.cursor, pair=self.pair, current_unix=current_unix, lower_unix=lower_unix)

            payload = {
                'pair': self.pair,
                'unix_open': lower_unix,
                'unix_close': current_unix,
                'created_at': datetime.utcnow()
            }
            if result[0][0] is None:
                payload.update({
                    'open': 0,
                    'high': 0,
                    'low': 0,
                    'close': 0,
                    'volume': 0,
                    'typical_price': 0,
                })

            else:
                open_payload, close_payload, result_open, result_close = read_db_result_not_null(self.cursor,
                                                                                                 pair=self.pair,
                                                                                                 result=result)

                open = round(open_payload / len(result_open), 2)
                high = round(result[0][3], 2)
                low = round(result[0][4], 2)
                close = round(close_payload / len(result_close), 2)
                volume = round(result[0][0], 4)
                typical_price = round((open_payload / len(result_open) + result[0][3] + result[0][
                    4] + close_payload / len(result_close)) / 4, 2)

                payload.update({
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'typical_price': typical_price,

                })

            write_db_result(self.cursor, self.cnx, **payload)

            log_function('OHLCVT saved! :)')
            time.sleep(50)
