import time
from datetime import datetime

import krakenex
import mysql.connector

from helpers.utils import log_function, write_db
from helpers.symbols import CRYPTO_SYMBOLS


class Kraken:
    """Kraken pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DASH-USD
    """
    def __init__(self, pair, **kwargs):
        self.exchange = 'Kraken'
        self.pair = pair
        self.unix_time_g_kraken = 0
        self.unix_time_kraken = 0
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def run(self):
        """
        Kraken does not have socket so Rest is implemented.
        """
        trade_id = '-'
        kraken_start = krakenex.API()
        while True:
            try:
                responses = kraken_start.query_public('Trades', {"pair": CRYPTO_SYMBOLS['kraken'][self.pair]})
                # log_function("Kreken received! :)")
                unix_time_s = responses['result']['last'][:10]
                unix_time_ms = responses['result']['last'][10:]
                unix_time_set = (unix_time_s, unix_time_ms)

                self.unix_time_g_kraken = round(float('.'.join(unix_time_set)), 4)
                for response in responses['result'][CRYPTO_SYMBOLS['kraken_sub'][self.pair]]:
                    res_time = round(float(response[2]), 4)
                    if res_time > self.unix_time_kraken:
                        unix_time = int(response[2])
                        price = float(response[0])
                        size_volume = float(response[1]) if response[3] == 'b' else -float(response[1])
                        created_at = datetime.utcnow()
                        vwap = price * abs(size_volume)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                                 unix_time=unix_time,
                                 price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)

                self.unix_time_kraken = self.unix_time_g_kraken
                time.sleep(45)

            except Exception as e:
                log_function("Kraken error! :(")
                log_function(str(e))
                time.sleep(45)
