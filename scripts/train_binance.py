import pandas as pd
from binance.client import Client
import datetime
import os
from ta import add_all_ta_features

client = Client()  # Nếu muốn, truyền API_KEY, SECRET qua .env

def fetch_data(symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_1MINUTE, lookback='1000'):
    klines = client.futures_klines(symbol=symbol, interval=interval, limit=int(lookback))
    df = pd.DataFrame(klines, columns=['open_time','open','high','low','close','volume',
                                       'close_time','quote_asset_volume','number_of_trades',
                                       'taker_buy_base_asset_volume','taker_buy_quote_asset_volume','ignore'])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    return df[['open_time', 'open', 'high', 'low', 'close', 'volume']]

def enrich_ta(df):
    # Dùng thư viện ta để tính các chỉ báo thông dụng
    df = df.copy()
    df['sma5'] = df['close'].rolling(window=5).mean()
    df['sma10'] = df['close'].rolling(window=10).mean()
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    # RSI
    import ta
    df['rsi14'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    # MACD
    macd = ta.trend.MACD(df['close'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    # Bollinger Bands
    bb = ta.volatility.BollingerBands(df['close'])
    df['bb_high'] = bb.bollinger_hband()
    df['bb_low'] = bb.bollinger_lband()
    return df

def add_signal(df):
    df['signal'] = 0
    df.loc[df['sma5'] > df['sma10'], 'signal'] = 1    # Buy
    df.loc[df['sma5'] < df['sma10'], 'signal'] = -1   # Sell
    return df

def main():
    df = fetch_data()
    df = enrich_ta(df)
    df = add_signal(df)
    now = datetime.datetime.utcnow()
    save_path = 'train_results'
    os.makedirs(save_path, exist_ok=True)
    file_name = f"{save_path}/btc_train_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(file_name, index=False)
    print(df.tail())

if __name__ == "__main__":
    main()
