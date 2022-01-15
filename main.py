import json
import requests_cache as rc
from concurrent import futures
import pandas as pd

api_key = {
    "api_key": "a740b2dde762ba1a9b13b7da718e299c3cb56fa14266404090ef9a69d61579f9",
    "extraParams": "Research by Korchmar brothers"
}

session = rc.CachedSession(
    allowable_codes=(200,),
    cache_name="cryptocompare",
    backend='filesystem',
    ignored_parameters=["api_key"]  # Should not affect result
)

date = 1640995200000  # 01 January 2022, in ms since 1970


def get_data(url, params=None):
    params = params or dict()
    params.update(api_key)
    ask = session.get(url, params=params)
    json_content = json.loads(ask.text)

    if ask.status_code != 200 or not json_content["Data"]:
        raise ValueError(f"Response was malformed! url: {ask.url}")
    return json_content["Data"]


if __name__ == '__main__':
    # 1. Determine which coins have a market cap above the treshold:
    MIN_MARKET_CAP = 1e5  # not used for now
    print(f"1. Listing symbols with market cap >= {MIN_MARKET_CAP} USD;")
    
    # 1.1 Find total volumes from blockchain entries. First, get list of all coins that have this available:
    raw_coins = get_data(url="https://min-api.cryptocompare.com/data/blockchain/list")
    coins_on_blockchain = list(raw_coins.keys())
    print(f"1.1. Found blockchain info for {len(coins_on_blockchain)} symbols")
    
    # 1.2. Iterate over individual coins to get volume:
    total_volumes = {}
    unknown_volume = []
    with futures.ThreadPoolExecutor() as executor:
    
        def get_volume(symbol):
            coin_params = {"fsym": symbol, "toTs": date, "limit": 1}
            coin_stats = get_data("https://min-api.cryptocompare.com/data/blockchain/histo/day", coin_params)
            return coin_stats["Data"][0]["current_supply"]
    
        working_total_volumes = {coin: executor.submit(get_volume, coin) for coin in coins_on_blockchain}
        futures.wait(working_total_volumes.values())
    
        # Fill result dict:
        for coin, future in working_total_volumes.items():
            if future.exception() is None and future.result() > 0:
                total_volumes[coin] = future.result()
            else:
                unknown_volume.append(coin)
    
    print(f"1.2. Retrieved volume for {len(total_volumes)} symbols, {len(unknown_volume)} return empty payload;")
    
    # 1.3. Find current price at that time to get market capitalization
    closing_prices = {}
    unknown_price = []
    with futures.ThreadPoolExecutor() as executor:
    
        def get_price(symbol):
            coin_params = {"fsym": symbol, "tsym": "USD", "toTs": date, "limit": 1}
            coin_stats = get_data("https://min-api.cryptocompare.com/data/v2/histoday", coin_params)
            return coin_stats["Data"][0]["close"]
    
        working_closing_prices = {coin: executor.submit(get_price, coin) for coin in coins_on_blockchain}
        futures.wait(working_closing_prices.values())
    
        # Fill result dict:
        for coin, future in working_closing_prices.items():
            if future.exception() is None and future.result() > 0:
                closing_prices[coin] = future.result()
            else:
                unknown_price.append(coin)
    
    print(f"1.3. Retrieved closing price for {len(closing_prices)} symbols, {len(unknown_price)} return empty payload;")
    
    # 1.4. Produce a dataframe
    rows = []
    columns = ["symbol", "volume", "closing price", "market cap"]
    for coin in coins_on_blockchain:
        data = [coin, total_volumes.get(coin, 0), closing_prices.get(coin, 0)]
        data.append(data[1] * data[2])
        rows.append(data)
    market_capitalization = pd.DataFrame(data=rows, columns=columns)
    market_capitalization.to_csv("market_cap.csv")
    print(f"1.4. Exporting CSV for market capitalization.")
