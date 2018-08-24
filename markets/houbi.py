import ast
import json
import gzip
import random
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db
from helpers.symbols import CRYPTO_SYMBOLS


class Huobi:
    """Huobi pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DASH-USD
    """

    def __init__(self, pair, **kwargs):
        self.exchange = 'Huobi'
        self.pair = pair
        self.sub_id = random.randint(1000, 10000)
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            response = ast.literal_eval(gzip.decompress(message).decode('utf-8'))
            log_function('Houbi received! :)')
            trade_id = response["tick"]["data"][0]["id"]
            unix_time = int(response["tick"]["data"][0]["ts"] / 1000)
            price = float(response["tick"]["data"][0]["price"])
            size_volume = float(response["tick"]["data"][0]["amount"]) if response['tick']['data'][0][
                                                                              'direction'] == 'buy' else -float(
                response["tick"]["data"][0]["amount"])
            created_at = datetime.utcnow()
            vwap = price * abs(size_volume)
            write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                     unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)

        except AttributeError:
            log_function("Huobi error")

    def on_open(self, ws):
        subscription = {"sub": "market.{pair}.trade.detail".format(pair=CRYPTO_SYMBOLS['huobi'][self.pair]),
                        "id": self.sub_id}
        sub = json.dumps(subscription)
        ws.send(sub)
