import time
import requests
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db
from helpers.symbols import CRYPTO_SYMBOLS


class Kucoin:
    """Kucoin pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DASH-USD
    """
    def __init__(self, pair, **kwargs):
        self.exchange = 'Kucoin'
        self.pair = pair
        self.market = "https://api.kucoin.com/v1/open/deal-orders?limit=50&symbol="
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def run(self):
        unix_ms = 0
        while True:
            try:
                r = requests.get("{market}{pair}".format(market=self.market, pair=CRYPTO_SYMBOLS['kucoin'][self.pair])).json()
                log_function("Kucoin received! :)")
                for response in r['data']:
                    if response[0] > unix_ms:
                        trade_id = '-'
                        unix_time = int(response[0] / 1000)
                        price = float(response[2])
                        size_volume = float(response[3]) if response[1] == 'BUY' else -float(response[3])
                        created_at = datetime.utcnow()
                        vwap = price * abs(size_volume)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                                 unix_time=unix_time,
                                 price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)
                        unix_ms = response[0]
                time.sleep(5)

            except AttributeError:
                log_function("Kucoin error! :(")
