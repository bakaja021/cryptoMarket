import gc
import json
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db


class Binance:
    """Binance pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    """

    def __init__(self, **kwargs):
        self.exchange = "Binance"
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            # log_function('Binance received! :)')
            gc.collect()
            response = json.loads(message)
            response = response['data']
            pair = "BCHUSD" if str(response['s'])[:-1] == "BCCUSD" else str(response['s'])[:-1]
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
