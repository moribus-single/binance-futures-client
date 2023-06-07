import asyncio
import json
import websockets
import requests


# URIs for getting real-time trades
BTC_URI = "wss://stream.binance.com:9443/ws/btcusdt@trade"
ETH_URI = "wss://stream.binance.com:9443/ws/ethusdt@trade"

# Binance API endpoints
SERVER_TIME = "https://fapi.binance.com/fapi/v1/time"
AGG_TRADES = "https://fapi.binance.com/fapi/v1/aggTrades"

# Minimum amount of prices for calculating Pearson correlation
COMP_AMOUNT = 20

# Border value for determining relation in prices
BORDER_VALUE = 0.3

# Price change parameters
PRICE_CHANGE_TIME = 60
PRICE_CHANGE_PERCENT = 1


def pearson_correlation(x, y):
    """
    Calculates Pearson correlation coefficient
    Borders: -1 <= coeff <= +1

    If:
        0 < abs(coeff) < 0.3 --> weak relation
        0.3 <= coeff < 0.5 --> average relation
        0.5 <= coeff < 1 --> strong relation

    """
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)

    x_x = [num * num for num in x]
    y_y = [num * num for num in y]
    x_y = [a * b for a, b in zip(x, y)]

    mean_x_y = sum(x_y) / len(x_y)
    mean_x_x = sum(x_x) / len(x_x)
    mean_y_y = sum(y_y) / len(y_y)

    x_deviation = pow(mean_x_x - mean_x * mean_x, 0.5)
    y_deviation = pow(mean_y_y - mean_y * mean_y, 0.5)

    if x_deviation == 0 or y_deviation == 0:
        return 0

    return (mean_x_y - mean_x * mean_y) / (x_deviation * y_deviation)


def get_server_time():
    """Returns current server time"""
    return requests.get(SERVER_TIME, timeout=5).json()['serverTime'] // 1000


def get_eth_price_change_percents():
    """Returns ETH price change percent from old_time to new_time"""
    current_time = get_server_time()

    old_price_data = {
        "symbol": "ETHUSDT",
        "startTime": (current_time - PRICE_CHANGE_TIME) * 1000,
        "limit": 1
    }
    new_price_data = {
        **old_price_data,
        "startTime": current_time * 1000,
    }

    old_price = float(requests.get(AGG_TRADES, params=old_price_data,  timeout=5).json()[0]['p'])
    new_price = float(requests.get(AGG_TRADES, params=new_price_data,  timeout=5).json()[0]['p'])

    return abs(old_price - new_price) * 100 // old_price


async def main():
    async with websockets.connect(BTC_URI, ping_timeout=None) as btc_ws, \
            websockets.connect(ETH_URI, ping_timeout=None) as eth_ws:
        channel = asyncio.Queue()

        async def unpack(ws, source):
            """Unpacking asset prices from received data"""
            while True:
                data = await ws.recv()
                json_data = json.loads(data)

                # writing data to channel
                await channel.put((source, float(json_data['p'])))

        # asynchronous execution
        asyncio.create_task(unpack(btc_ws, 'BTC'))
        asyncio.create_task(unpack(eth_ws, 'ETH'))

        btc_prices = []
        eth_prices = []

        start_time = get_server_time()
        while True:
            # check if price change time has passed
            current_time = get_server_time()
            if current_time - start_time >= PRICE_CHANGE_TIME:
                # check price change percent
                eth_price_change = get_eth_price_change_percents()
                start_time = current_time

                if eth_price_change > PRICE_CHANGE_PERCENT:
                    # if price changes more than PRICE_CHANGE_PERCENT
                    print(f"[TIME={PRICE_CHANGE_TIME}sec] PRICE CHANGE = {eth_price_change}%")

            # if first asset prices >= COMP_AMOUNT, then waiting for
            # the second asset prices amount is reaching COMP_AMOUNT
            if len(eth_prices) >= COMP_AMOUNT and len(btc_prices) >= COMP_AMOUNT:
                # calculating Pearson correlation, delete old prices and log to terminal
                coeff = pearson_correlation(btc_prices[:COMP_AMOUNT], eth_prices[:COMP_AMOUNT])

                btc_prices = []
                eth_prices = []

                if coeff >= BORDER_VALUE:
                    # updating start time, from which price change will be calculated
                    start_time = get_server_time()
                    print(f"Correlation: {coeff}\nBTC price affects ETH price!\n")

            # if both arrays length less than 200, just continue receiving data
            else:
                ticker, data = await channel.get()

                if ticker == 'BTC':
                    btc_prices.append(data)
                elif ticker == 'ETH':
                    eth_prices.append(data)


asyncio.get_event_loop().run_until_complete(main())
