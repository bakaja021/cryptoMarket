import time
from datetime import datetime

from copra.websocket import Client
import mysql.connector

from helpers.utils import log_function, write_db


class Coinbase(Client):
    """Coinbase pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    """

    def __init__(self, loop, channels, **kwargs):
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()
        super().__init__(loop, channels)

    def on_message(self, message):
        try:
            if 'product_id' in message and 'trade_id' in message and 'time' in message and 'price' in message and 'side' in message:
                log_function('Coinbase received! :)')
                exchange = 'Coinbase'
                pair = message['product_id'].replace('-', '')
                trade_id = message['trade_id'] if 'trade_id' in message else "-"
                unix_time = int(
                    time.mktime(datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())) - 25200
                price = float(message['price'])
                size_volume = float(message['last_size']) if message['side'] == 'buy' else -float(message['last_size'])
                created_at = datetime.utcnow()
                vwap = price * abs(size_volume)
                write_db(self.cursor, self.cnx, exchange=exchange, pair=pair, trade_id=trade_id, unix_time=unix_time,
                         price=price, size_volume=size_volume, created_at=created_at, vwap=vwap)
            else:
                pass
        except AttributeError:
            log_function("Coinbase error! :(")
