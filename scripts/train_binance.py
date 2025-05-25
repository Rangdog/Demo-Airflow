import pandas as pd
import requests
import datetime
import os

def fetch_data_cc(symbol='BTC', to_symbol='USDT', limit=2000):
    url = "https://min-api.cryptocompare.com/data/v2/histominute"
    params = {
        'fsym': symbol,
        'tsym': to_symbol,
        'limit': limit  # tối đa 2000 phút/lần (~33 giờ)
    }
    r = requests.get(url, params=params)
    data = r.json()
    if 'Data' not in data or 'Data' not in data['Data']:
        print("API trả về lỗi:")
        print(data)
        raise Exception("Không lấy được dữ liệu từ CryptoCompare!")
    data_list = data['Data']['Data']
    df = pd.DataFrame(data_list)
    df['open_time'] = pd.to_datetime(df['time'], unit='s')
    df = df.rename(columns={'volumeto': 'volume'})
    return df[['open_time', 'open', 'high', 'low', 'close', 'volume']]

def enrich_ta(df):
    import ta
    df['sma5'] = df['close'].rolling(window=5).mean()
    df['sma10'] = df['close'].rolling(window=10).mean()
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['rsi14'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    macd = ta.trend.MACD(df['close'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
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
    df = fetch_data_cc()
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
