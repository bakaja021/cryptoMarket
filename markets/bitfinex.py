import time
from datetime import datetime

from btfxwss import BtfxWss
import mysql.connector

from helpers.utils import log_function, write_db


class Bitfinex:
    """Bitfinex pairs (cryptocurrency against usd) that are available are:
    1. BTC-USD
    2. ETH-USD
    3. LTC-USD
    4. ETC-USD
    5. DASH-USD
    """

    def __init__(self, **kwargs):
        self.exchange = 'Bitfinex'
        self.pair_btc = 'BTCUSD'
        self.pair_eth = 'ETHUSD'
        self.pair_etc = 'ETCUSD'
        self.pair_ltc = 'LTCUSD'
        self.pair_dsh = 'DSHUSD'
        self.cnx = mysql.connector.connect(**kwargs)
        self.cursor = self.cnx.cursor()

        wss = BtfxWss()
        wss.start()

        while not wss.conn.connected.is_set():
            time.sleep(1)

        wss.subscribe_to_trades(self.pair_btc)
        wss.subscribe_to_trades(self.pair_eth)
        wss.subscribe_to_trades(self.pair_etc)
        wss.subscribe_to_trades(self.pair_ltc)
        wss.subscribe_to_trades(self.pair_dsh)

        t = time.time()
        while time.time() - t < 10:
            pass

        self.ticker_btc = wss.trades(self.pair_btc)
        self.ticker_eth = wss.trades(self.pair_eth)
        self.ticker_etc = wss.trades(self.pair_etc)
        self.ticker_ltc = wss.trades(self.pair_ltc)
        self.ticker_dsh = wss.trades(self.pair_dsh)

    @staticmethod
    def _extract_data(tu):
        trade_id = tu[0][1][0]
        unix_time = int(tu[1])
        price = float(tu[0][1][3])
        size_volume = float(tu[0][1][2])
        created_at = datetime.utcnow()
        vwap = price * abs(size_volume)

        return trade_id, unix_time, price, size_volume, created_at, vwap

    def run(self):
        """
        hb - heart beat is returned when there is no action on market
        te - message which mimics the current behavior, use it if speed is important to you
        tu - message which will be delayed by 1-2 seconds and include the tradeId,
             use if the tradeId is important to you
        """

        while True:
            try:
                log_function('Bitfinex received! :)')
                try:
                    tu_btc = self.ticker_btc.get()
                    if tu_btc[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_btc)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_btc, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except AttributeError:
                    pass

                try:
                    tu_eth = self.ticker_eth.get()
                    if tu_eth[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_eth)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_eth, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except AttributeError:
                    pass

                try:
                    tu_etc = self.ticker_etc.get()
                    if tu_etc[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_etc)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_etc, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except AttributeError:
                    pass

                try:
                    tu_ltc = self.ticker_ltc.get()
                    if tu_ltc[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_ltc)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_ltc, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except AttributeError:
                    pass

                try:
                    tu_dsh = self.ticker_dsh.get()
                    if tu_dsh[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_dsh)
                        pair_dsh = 'DASHUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_dsh, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except AttributeError:
                    pass

            except AttributeError:
                log_function("Bitfinex error")
