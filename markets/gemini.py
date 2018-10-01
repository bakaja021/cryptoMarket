import gc
import ast
from datetime import datetime

import mysql.connector

from helpers.utils import log_function, write_db


class Gemini:
    """Gemini pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. ZEC-USD
    """

    def __init__(self, pair, **kwargs):
        self.exchange = 'Gemini'
        self.pair = pair
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

    def on_message(self, ws, message):
        try:
            gc.collect()
            response = ast.literal_eval(message)
            if 'eventId' in response and 'timestamp' in response and 'events' in response:
                # log_function('Gemini received! :)')
                trade_id = response['eventId']
                unix_time = int(response['timestamp'])
                price = float(response['events'][0]['price'])
                size_volume = float(response['events'][0]['amount']) if response['events'][0][
                                                                            'makerSide'] == 'ask' else -float(
                    response['events'][0]['amount'])
                created_at = datetime.utcnow()
                vwap = price * abs(size_volume)
                write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair, trade_id=trade_id,
                         unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)

        except Exception as e:
            log_function("Gemini error! :(")
            log_function(str(e))
