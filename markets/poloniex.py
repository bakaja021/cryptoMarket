import ast
import json
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db
from helpers.symbols import CRYPTO_SYMBOLS


class Poloniex:
    """Poloniex pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DSH-USD
    6. BCH-USD
    7. EOS-USD
    8. XMR-USD
    9. ZEC-USD
    """
    def __init__(self, pair, **kwargs):
        self.exchange = "Poloniex"
        self.pair = pair
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            # log_function("Poloniex received! :)")
            responses = ast.literal_eval(message)
            if len(responses) > 1:
                for response in responses[2]:
                    if response[0] == 't':
                        trade_id = response[1]
                        unix_time = int(response[5])
                        price = float(response[3])
                        size_volume = float(response[4]) if response[2] else -float(response[4])
                        created_at = datetime.utcnow()
                        vwap = price * abs(size_volume)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                                 unix_time=unix_time,
                                 price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)
        except Exception as e:
            log_function("Poloniex error! :(")
            log_function(str(e))

    def on_open(self, ws):
        subscription = {"command": "subscribe", "channel": CRYPTO_SYMBOLS['poloniex'][self.pair]}
        sub = json.dumps(subscription)
        ws.send(sub)
