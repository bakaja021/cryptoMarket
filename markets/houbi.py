import gc
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
    6. BCH-USD
    7. EOS-USD
    8. TRX-USD
    9. VET-USD
    10. IOTA-USD
    11. ZEC-USD
    12. NEO-USD
    13. ADA-USD
    """

    def __init__(self, pair, **kwargs):
        self.exchange = 'Huobi'
        self.pair = pair
        self.sub_id = random.randint(1000, 10000)
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            gc.collect()
            response = ast.literal_eval(gzip.decompress(message).decode('utf-8'))
            # log_function('Houbi received! :)')
            if 'tick' in response:
                for transaction in response["tick"]["data"]:
                    trade_id = transaction["id"]
                    unix_time = int(transaction["ts"] / 1000)
                    price = float(transaction["price"])
                    size_volume = float(transaction["amount"]) if transaction['direction'] == 'buy' else -float(
                        transaction["amount"])
                    created_at = datetime.utcnow()
                    vwap = price * abs(size_volume)
                    write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                             unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                             vwap=vwap)

        except Exception as e:
            log_function("Huobi error! :(")
            log_function(str(e))

    def on_open(self, ws):
        subscription = {"sub": "market.{pair}.trade.detail".format(pair=CRYPTO_SYMBOLS['huobi'][self.pair]),
                        "id": self.sub_id}
        sub = json.dumps(subscription)
        ws.send(sub)
