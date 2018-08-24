import os
import ast
from datetime import datetime

import pysher
import mysql.connector

from helpers.utils import log_function, write_db
from helpers.symbols import CRYPTO_SYMBOLS


class BitstampClient:
    """Bitstamp pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    """
    def __init__(self, pair, **kwargs):
        self.pusher = pysher.Pusher(os.getenv("BITSTAMP_KEY"))
        self.exchange = "Bitstamp"
        self.pair = pair
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def init_market(self, *args, **kwargs):
        try:
            response = ast.literal_eval(args[0])
            log_function('Bitstamp received! :)')
            trade_id = response['id']
            unix_time = int(response['timestamp'])
            price = float(response['price'])
            size_volume = float(response['amount']) if not response['type'] else -float(response['amount'])
            created_at = datetime.utcnow()
            vwap = price * abs(size_volume)
            write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id, unix_time=unix_time,
                     price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)

        except AttributeError:
            log_function("Bitstamp error! :(")

    def connect(self, data):
        channel = self.pusher.subscribe('live_trades{pair}'.format(pair=CRYPTO_SYMBOLS['bitstamp'][self.pair]))
        channel.bind('trade', self.init_market)
