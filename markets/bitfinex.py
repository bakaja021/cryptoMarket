import gc
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
        self.pair_bch = 'BCHUSD'
        self.pair_btg = 'BTGUSD'
        self.pair_eos = 'EOSUSD'
        self.pair_trx = 'TRXUSD'
        self.pair_xmr = 'XMRUSD'
        self.pair_vet = 'VETUSD'
        self.pair_iot = 'IOTUSD'
        self.pair_zec = 'ZECUSD'
        self.pair_neo = 'NEOUSD'
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
        wss.subscribe_to_trades(self.pair_bch)
        wss.subscribe_to_trades(self.pair_btg)
        wss.subscribe_to_trades(self.pair_eos)
        wss.subscribe_to_trades(self.pair_trx)
        wss.subscribe_to_trades(self.pair_xmr)
        wss.subscribe_to_trades(self.pair_vet)
        wss.subscribe_to_trades(self.pair_iot)
        wss.subscribe_to_trades(self.pair_zec)
        wss.subscribe_to_trades(self.pair_neo)

        t = time.time()
        while time.time() - t < 10:
            pass

        self.ticker_btc = wss.trades(self.pair_btc)
        self.ticker_eth = wss.trades(self.pair_eth)
        self.ticker_etc = wss.trades(self.pair_etc)
        self.ticker_ltc = wss.trades(self.pair_ltc)
        self.ticker_dsh = wss.trades(self.pair_dsh)
        self.ticker_bch = wss.trades(self.pair_bch)
        self.ticker_btg = wss.trades(self.pair_btg)
        self.ticker_eos = wss.trades(self.pair_eos)
        self.ticker_trx = wss.trades(self.pair_trx)
        self.ticker_xmr = wss.trades(self.pair_xmr)
        self.ticker_vet = wss.trades(self.pair_vet)
        self.ticker_iot = wss.trades(self.pair_iot)
        self.ticker_zec = wss.trades(self.pair_zec)
        self.ticker_neo = wss.trades(self.pair_neo)

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
            gc.collect()
            try:
                # log_function('Bitfinex received! :)')
                try:
                    tu_btc = self.ticker_btc.get()
                    if tu_btc[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_btc)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_btc, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_eth = self.ticker_eth.get()
                    if tu_eth[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_eth)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_eth, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_etc = self.ticker_etc.get()
                    if tu_etc[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_etc)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_etc, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_ltc = self.ticker_ltc.get()
                    if tu_ltc[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_ltc)
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=self.pair_ltc, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_dsh = self.ticker_dsh.get()
                    if tu_dsh[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_dsh)
                        pair_dsh = 'DASHUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_dsh, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_bch = self.ticker_bch.get()
                    if tu_bch[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_bch)
                        pair_bch = 'BCHUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_bch, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_btg = self.ticker_btg.get()
                    if tu_btg[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_btg)
                        pair_btg = 'BTGUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_btg, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_eos = self.ticker_eos.get()
                    if tu_eos[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_eos)
                        pair_eos = 'EOSUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_eos, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_trx = self.ticker_trx.get()
                    if tu_trx[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_trx)
                        pair_trx = 'TRXUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_trx, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_xmr = self.ticker_xmr.get()
                    if tu_xmr[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_xmr)
                        pair_xmr = 'XMRUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_xmr, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_vet = self.ticker_vet.get()
                    if tu_vet[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_vet)
                        pair_vet = 'VETUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_vet, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_iot = self.ticker_iot.get()
                    if tu_iot[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_iot)
                        pair_iot = 'IOTAUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_iot, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_zec = self.ticker_zec.get()
                    if tu_zec[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_zec)
                        pair_zec = 'ZECUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_zec, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

                try:
                    tu_neo = self.ticker_neo.get()
                    if tu_neo[0][0] == "tu":
                        trade_id, unix_time, price, size_volume, created_at, vwap = self._extract_data(tu_neo)
                        pair_neo = 'NEOUSD'
                        write_db(self.cursor, self.cnx, exchange=self.exchange, pair=pair_neo, trade_id=trade_id,
                                 unix_time=unix_time, price=price, size_volume=size_volume, created_at=created_at,
                                 vwap=vwap)
                except Exception as e:
                    pass

            except Exception as e:
                log_function("Bitfinex error")
                log_function(str(e))
