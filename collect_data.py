import time
import asyncio
from multiprocessing import Process
import websocket
from copra.websocket import Channel

from markets.binance import Binance
from markets.bitfinex import Bitfinex
from markets.bitstamp import BitstampClient
from markets.coinbase import Coinbase
from markets.gemini import Gemini
from markets.hitbtc import Hitbtc
from markets.houbi import Huobi    # DON'T WORK
from markets.kraken import Kraken
from markets.kucoin import Kucoin
from markets.poloniex import Poloniex

from helpers.utils import config


def binance():
    market = Binance(**config)
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/ltcusdt@trade/etcusdt@trade",
        on_message=market.on_message)
    while True:
        ws.run_forever()
        time.sleep(1)


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


def coinbase():
    loop = asyncio.get_event_loop()
    ws = Coinbase(loop, Channel('ticker', ['BTC-USD', 'ETH-USD', 'LTC-USD']), **config)
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


def run_in_parallel(*fns):
    proc = []
    for fn in fns:
        p = Process(target=fn)
        p.start()
        proc.append(p)
    try:
        for p in proc:
            p.join()
    except KeyboardInterrupt:
        print('\nKeyboard received CTRL-C... Bye!!!')
        for p in proc:
            p.terminate()
            p.join()


def main():
    start = time.time()
    run_in_parallel(binance, bitfinex, bitstamp_btc, bitstamp_eth, bitstamp_ltc, coinbase, gemini_btc, gemini_eth, hitbtc_btc, hitbtc_eth, hitbtc_etc,
                    hitbtc_ltc, hitbtc_dash, huobi_btc, huobi_eth, huobi_etc, huobi_ltc, huobi_dash, kraken_btc,
                    kraken_eth, kraken_etc, kraken_ltc, kraken_dash, kucoin_btc, kucoin_eth, kucoin_etc, kucoin_ltc,
                    kucoin_dash, poloniex_btc, poloniex_dash, poloniex_etc, poloniex_eth, poloniex_ltc)
    stop = time.time()
    print("It took: {total} seconds.".format(total=stop-start))


if __name__ == "__main__":
    main()
