import ast
import json
import random
import calendar
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db


class Hitbtc:
    """HitBTC pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DASH-USD
    """

    def __init__(self, pair, **kwargs):
        self.exchange = "HitBTC"
        self.pair = pair
        self.sub_id = random.randint(1000, 10000)
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            response = ast.literal_eval(message)
            log_function("Hitbtc received! :)")
            if response["method"] == "updateTrades":
                trade_id = response["params"]["data"][0]["id"]
                unix_time = int(calendar.timegm(datetime.strptime(response["params"]["data"][0]["timestamp"][:19],
                                                                  "%Y-%m-%dT%X").timetuple()))
                price = float(response["params"]["data"][0]["price"])
                size_volume = float(response["params"]["data"][0]["quantity"]) if response["params"]["data"][0][
                                                                                      "side"] == 'buy' else -float(
                    response["params"]["data"][0]["quantity"])
                created_at = datetime.utcnow()
                vwap = price * abs(size_volume)
                write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                         unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)

        except AttributeError:
            log_function("Hitbtc error! :(")

    def on_open(self, ws):
        subscription = {"method": "subscribeTrades", "params": {"symbol": self.pair}, "id": self.sub_id}
        sub = json.dumps(subscription)
        ws.send(sub)
