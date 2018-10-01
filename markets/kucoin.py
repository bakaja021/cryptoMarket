import gc
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
    6. BCH-USD
    7. EOS-USD
    8. TRX-USD
    9. VET-USD
    10. NEO-USD
    11. ADA-USD
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
            gc.collect()
            try:
                r = requests.get("{market}{pair}".format(market=self.market, pair=CRYPTO_SYMBOLS['kucoin'][self.pair])).json()
                # log_function("Kucoin received! :)")
                if 'data' in r:
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
                    time.sleep(45)

            except Exception as e:
                self.cnx.reconnect(attempts=1, delay=0)
                log_function("Kucoin error! :(")
                log_function(str(e))
