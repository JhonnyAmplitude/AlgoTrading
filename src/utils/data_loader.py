# src/utils/data_loader.py

from dotenv import load_dotenv
import os
import requests
import pandas as pd
from requests.exceptions import RequestException

load_dotenv()
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")


class BybitClient:
    def __init__(self,
                 api_key: str = API_KEY,
                 api_secret: str = SECRET_KEY,
                 base_url: str = BYBIT_BASE_URL):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url

    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """
        Возвращает DataFrame OHLCV через V5 GET /v5/market/kline.
        """
        url = f"{self.base_url}/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("result", {}).get("list", [])
        except (RequestException, ValueError) as e:
            print(f"[BybitClient] Error fetching klines for {symbol}: {e}")
            return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

        if not data:
            return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume"])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df

    def get_top10_symbols(self) -> list[str]:
        """
        Возвращает список топ-10 USDT-пар по объёму за 24 часа через V5 GET /v5/market/tickers.
        """
        url = f"{self.base_url}/v5/market/tickers"
        params = {"category": "spot"}
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            tickers = payload.get("result", {}).get("list", [])
        except (RequestException, ValueError) as e:
            print(f"[BybitClient] Error fetching tickers: {e}")
            return []

        usdt_pairs = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
        sorted_usdt = sorted(
            usdt_pairs,
            key=lambda x: float(x.get("volume24h", 0.0)),
            reverse=True
        )
        return [t["symbol"] for t in sorted_usdt[:10]]


def load_data(symbol: str,
              interval: str,
              limit: int = 200,
              source: str = "bybit") -> pd.DataFrame:
    if source.lower() == "bybit":
        client = BybitClient()
        return client.get_klines(symbol, interval, limit)
    raise ValueError(f"Unknown data source: {source}")


if __name__ == "__main__":
    client = BybitClient()
    print("Base URL:", client.base_url)
    print("Top 10 symbols:", client.get_top10_symbols())
    df = client.get_klines("BTCUSDT", "1", 100)
    print(df.head())
