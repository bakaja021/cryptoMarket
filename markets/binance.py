import gc
import json
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db


class Binance:
    """Binance pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. ETC-USD
    4. LTC-USD
    5. BCH-USD
    6. EOS-USD
    7. TRX-USD
    8. VET-USD
    9. IOTA-USD
    10. TUSD-USD
    11. NEO-USD
    12. ADA-USD
    """

    def __init__(self, **kwargs):
        self.exchange = "Binance"
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            gc.collect()
            # log_function('Binance received! :)')
            response = json.loads(message)
            response = response['data']
            pair = "BCHUSD" if str(response['s'])[:-1] == "BCCUSD" else str(response['s'])[:-1]
            if pair == "BCCUSD":
                pair = "BCHUSD"
            trade_id = response['t']
            unix_time = int(response['T'] / 1000)
            price = float(response['p'])
            size_volume = float(response['q']) if response['m'] else -float(response['q'])
            created_at = datetime.utcnow()
            vwap = price * abs(size_volume)
            write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair, trade_id=trade_id, unix_time=unix_time,
                     price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)

        except Exception as e:
            log_function('Binance error! :(')
            log_function(str(e))
