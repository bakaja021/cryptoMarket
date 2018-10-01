import time
import asyncio

import websocket
from copra.websocket import Channel

from markets.binance import Binance
from markets.bitfinex import Bitfinex
from markets.bitstamp import BitstampClient
from markets.coinbase import Coinbase
from markets.gemini import Gemini
from markets.hitbtc import Hitbtc
from markets.houbi import Huobi
from markets.kraken import Kraken
from markets.kucoin import Kucoin
from markets.poloniex import Poloniex
from markets.ohlcvt_calculation import Ohlcvt

from helpers.utils import config, run_in_parallel


def binance():
    market = Binance(**config)
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/ltcusdt@trade/etcusdt@trade/bccusdt@trade/eosusdt@trade/trxusdt@trade/vetusdt@trade/iotausdt@trade/tusdusdt@trade/neousdt@trade/adausdt@trade/",
        on_message=market.on_message)
    ws.run_forever(ping_timeout=5)


def bitfinex():
    market = Bitfinex(**config)
    market.run()


def bitstamp_btc():
    bitstamp = BitstampClient("BTCUSD", **config)
    bitstamp.pusher.connection.bind('pusher:connection_established', bitstamp.connect)
    bitstamp.pusher.connect()
    while True:
        time.sleep(1)


def bitstamp_eth():
    bitstamp = BitstampClient("ETHUSD", **config)
    bitstamp.pusher.connection.bind('pusher:connection_established', bitstamp.connect)
    bitstamp.pusher.connect()
    while True:
        time.sleep(1)


def bitstamp_ltc():
    bitstamp = BitstampClient("LTCUSD", **config)
    bitstamp.pusher.connection.bind('pusher:connection_established', bitstamp.connect)
    bitstamp.pusher.connect()
    while True:
        time.sleep(1)


def bitstamp_bch():
    bitstamp = BitstampClient("BCHUSD", **config)
    bitstamp.pusher.connection.bind('pusher:connection_established', bitstamp.connect)
    bitstamp.pusher.connect()
    while True:
        time.sleep(1)


def coinbase():
    loop = asyncio.get_event_loop()
    ws = Coinbase(loop, Channel('ticker', ['BTC-USD', 'ETH-USD', 'LTC-USD', 'ETC-USD', 'BCH-USD']), **config)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(ws.close())
        loop.close()


def gemini_btc():
    market = Gemini('BTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.gemini.com/v1/marketdata/btcusd?trades=true",
                                on_message=market.on_message)
    while True:
        ws.run_forever()
        time.sleep(1)


def gemini_eth():
    market = Gemini('ETHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.gemini.com/v1/marketdata/ethusd?trades=true",
                                on_message=market.on_message)
    while True:
        ws.run_forever()
        time.sleep(1)


def gemini_zec():
    market = Gemini('ZECUSD', **config)
    ws = websocket.WebSocketApp("wss://api.gemini.com/v1/marketdata/zecusd?trades=true",
                                on_message=market.on_message)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_btc():
    market = Hitbtc('BTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_eth():
    market = Hitbtc('ETHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_etc():
    market = Hitbtc('ETCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_ltc():
    market = Hitbtc('LTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_dash():
    market = Hitbtc('DASHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_bch():
    market = Hitbtc('BCHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_btg():
    market = Hitbtc('BTGUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_eos():
    market = Hitbtc('EOSUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_trx():
    market = Hitbtc('TRXUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_xmr():
    market = Hitbtc('XMRUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_iota():
    market = Hitbtc('IOTAUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_zec():
    market = Hitbtc('ZECUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_neo():
    market = Hitbtc('NEOUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def hitbtc_ada():
    market = Hitbtc('ADAUSD', **config)
    ws = websocket.WebSocketApp("wss://api.hitbtc.com/api/2/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_btc():
    market = Huobi('BTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_eth():
    market = Huobi('ETHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_etc():
    market = Huobi('ETCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_ltc():
    market = Huobi('LTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_dash():
    market = Huobi('DASHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_bch():
    market = Huobi('BCHUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_eos():
    market = Huobi('EOSUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_trx():
    market = Huobi('TRXUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_vet():
    market = Huobi('VETUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_iota():
    market = Huobi('IOTAUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_zec():
    market = Huobi('ZECUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_neo():
    market = Huobi('NEOUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def huobi_ada():
    market = Huobi('ADAUSD', **config)
    ws = websocket.WebSocketApp("wss://api.huobi.pro/ws", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def kraken_ada():
    market = Kraken('ADAUSD', **config)
    market.run()


def kraken_qtum():
    market = Kraken('QTUMUSD', **config)
    market.run()


def kraken_btc():
    market = Kraken('BTCUSD', **config)
    market.run()


def kraken_eth():
    market = Kraken('ETHUSD', **config)
    market.run()


def kraken_etc():
    market = Kraken('ETCUSD', **config)
    market.run()


def kraken_ltc():
    market = Kraken('LTCUSD', **config)
    market.run()


def kraken_dash():
    market = Kraken('DASHUSD', **config)
    market.run()


def kraken_bch():
    market = Kraken('BCHUSD', **config)
    market.run()


def kraken_eos():
    market = Kraken('EOSUSD', **config)
    market.run()


def kraken_xmr():
    market = Kraken('XMRUSD', **config)
    market.run()


def kraken_zec():
    market = Kraken('ZECUSD', **config)
    market.run()


def kucoin_btc():
    market = Kucoin('BTCUSD', **config)
    market.run()


def kucoin_eth():
    market = Kucoin('ETHUSD', **config)
    market.run()


def kucoin_etc():
    market = Kucoin('ETCUSD', **config)
    market.run()


def kucoin_ltc():
    market = Kucoin('LTCUSD', **config)
    market.run()


def kucoin_dash():
    market = Kucoin('DASHUSD', **config)
    market.run()


def kucoin_bch():
    market = Kucoin('BCHUSD', **config)
    market.run()


def kucoin_eos():
    market = Kucoin('EOSUSD', **config)
    market.run()


def kucoin_trx():
    market = Kucoin('TRXUSD', **config)
    market.run()


def kucoin_vet():
    market = Kucoin('VETUSD', **config)
    market.run()


def kucoin_neo():
    market = Kucoin('NEOUSD', **config)
    market.run()


def poloniex_btc():
    market = Poloniex('BTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_eth():
    market = Poloniex('ETHUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_etc():
    market = Poloniex('ETCUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_ltc():
    market = Poloniex('LTCUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_dash():
    market = Poloniex('DASHUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_bch():
    market = Poloniex('BCHUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_eos():
    market = Poloniex('EOSUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_xmr():
    market = Poloniex('XMRUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def poloniex_zec():
    market = Poloniex('ZECUSD', **config)
    ws = websocket.WebSocketApp("wss://api2.poloniex.com", on_message=market.on_message, on_open=market.on_open)
    while True:
        ws.run_forever()
        time.sleep(1)


def ohlcvt_btc():
    ohlcvt = Ohlcvt("BTCUSD", **config)
    ohlcvt.run()


def ohlcvt_etc():
    ohlcvt = Ohlcvt("ETCUSD", **config)
    ohlcvt.run()


def ohlcvt_ltc():
    ohlcvt = Ohlcvt("LTCUSD", **config)
    ohlcvt.run()


def ohlcvt_eth():
    ohlcvt = Ohlcvt("ETHUSD", **config)
    ohlcvt.run()


def ohlcvt_dash():
    ohlcvt = Ohlcvt("DASHUSD", **config)
    ohlcvt.run()


def ohlcvt_bch():
    ohlcvt = Ohlcvt("BCHUSD", **config)
    ohlcvt.run()


def ohlcvt_btg():
    ohlcvt = Ohlcvt("BTGUSD", **config)
    ohlcvt.run()


def ohlcvt_omg():
    ohlcvt = Ohlcvt("OMGUSD", **config)
    ohlcvt.run()


def ohlcvt_eos():
    ohlcvt = Ohlcvt("EOSUSD", **config)
    ohlcvt.run()


def ohlcvt_trx():
    ohlcvt = Ohlcvt("TRXUSD", **config)
    ohlcvt.run()


def ohlcvt_xmr():
    ohlcvt = Ohlcvt("XMRUSD", **config)
    ohlcvt.run()


def ohlcvt_vet():
    ohlcvt = Ohlcvt("VETUSD", **config)
    ohlcvt.run()


def ohlcvt_iota():
    ohlcvt = Ohlcvt("IOTAUSD", **config)
    ohlcvt.run()


def ohlcvt_zec():
    ohlcvt = Ohlcvt("ZECUSD", **config)
    ohlcvt.run()


def ohlcvt_tusd():
    ohlcvt = Ohlcvt("TUSDUSD", **config)
    ohlcvt.run()


def ohlcvt_neo():
    ohlcvt = Ohlcvt("NEOUSD", **config)
    ohlcvt.run()


def ohlcvt_ada():
    ohlcvt = Ohlcvt("ADAUSD", **config)
    ohlcvt.run()


def main():
    start = time.time()
    run_in_parallel(binance,
                    bitfinex,
                    bitstamp_btc, bitstamp_eth, bitstamp_ltc, bitstamp_bch,
                    coinbase,
                    gemini_btc, gemini_eth, gemini_zec,
                    hitbtc_btc, hitbtc_eth, hitbtc_etc, hitbtc_ltc, hitbtc_dash, hitbtc_bch, hitbtc_btg, hitbtc_eos, hitbtc_trx, hitbtc_xmr, hitbtc_iota, hitbtc_zec, hitbtc_neo, hitbtc_ada,
                    huobi_btc, huobi_eth, huobi_etc, huobi_ltc, huobi_dash, huobi_bch, huobi_eos, huobi_trx, huobi_vet, huobi_iota, huobi_zec, huobi_neo, huobi_ada,
                    kraken_btc, kraken_eth, kraken_etc, kraken_ltc, kraken_dash, kraken_zec, kraken_xmr, kraken_bch, kraken_eos, kraken_ada, kraken_qtum,
                    kucoin_btc, kucoin_eth, kucoin_etc, kucoin_ltc, kucoin_dash, kucoin_bch, kucoin_eos, kucoin_trx, kucoin_vet, kucoin_neo,
                    poloniex_btc, poloniex_dash, poloniex_etc, poloniex_eth, poloniex_ltc, poloniex_bch, poloniex_eos, poloniex_xmr, poloniex_zec,
                    ohlcvt_btc, ohlcvt_etc, ohlcvt_ltc, ohlcvt_dash, ohlcvt_eth, ohlcvt_bch, ohlcvt_btg, ohlcvt_omg, ohlcvt_eos, ohlcvt_trx, ohlcvt_xmr, ohlcvt_vet, ohlcvt_iota, ohlcvt_zec, ohlcvt_tusd, ohlcvt_neo, ohlcvt_ada)
    stop = time.time()
    print("It took: {total} seconds.".format(total=stop-start))


if __name__ == "__main__":
    main()
