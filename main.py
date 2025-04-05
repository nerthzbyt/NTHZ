import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Any, Optional, Union

import aiohttp
import ntplib
import numpy as np
import pandas as pd
import uvicorn
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI
from psycopg2 import OperationalError
from pybit.unified_trading import HTTP
from sqlalchemy.orm import sessionmaker, Session
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

# Importar modelos desde models.py
from models import (
    Base, MarketData, Orderbook, MarketTicker, Trade, Position, init_db, PositionSide,
)

# region Setup Inicial
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("NertzMetalEngine")

BASE_URL = "https://api.bybit.com" if not os.getenv("USE_TESTNET",
                                                    "True").lower() == "true" else "https://api-testnet.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear" if not os.getenv("USE_TESTNET",
                                                                    "True").lower() == "true" else "wss://stream-testnet.bybit.com/v5/public/linear"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:angel_amely@localhost:5432/nertzhul")
# Usar el motor de base de datos de models.py
engine = init_db(DATABASE_URL)  
engine.echo = True  # Activar logs SQL para desarrollo

for attempt in range(3):
    try:
        with engine.connect() as connection:
            logger.info("✅ Conexión a la base de datos establecida.")
        break
    except OperationalError as e:
        logger.error(f"❌ Error al conectar (intento {attempt + 1}/3): {e}")
        if attempt < 2:
            time.sleep(2)
        else:
            sys.exit(1)

# Usar la misma sesión que en models.py
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# endregion

# region Configuración de la base de datos
# Las definiciones de modelos ahora se importan desde models.py
# endregion

Base.metadata.create_all(bind=engine)


# endregion

# region Configuración
class Config:
    def __init__(self):
        self.USE_TESTNET = os.getenv("USE_TESTNET", "True").lower() == "true"
        self.SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
        self.TIMEFRAME = os.getenv("TIMEFRAME", "1m")
        self.CAPITAL_USDT = float(os.getenv("CAPITAL_USDT", "1030.02"))
        self.RISK_FACTOR = float(os.getenv("RISK_FACTOR", "0.005"))
        self.MIN_TRADE_SIZE = float(os.getenv("MIN_TRADE_SIZE", "0.001"))
        self.MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "0.005"))
        self.FEE_RATE = float(os.getenv("FEE_RATE", "0.00055"))
        self.TP_PERCENTAGE = float(os.getenv("TP_PERCENTAGE", "1.5"))
        self.SL_PERCENTAGE = float(os.getenv("SL_PERCENTAGE", "1.5"))
        self.EGM_BUY_THRESHOLD = float(os.getenv("EGM_BUY_THRESHOLD", "0.1"))
        self.EGM_SELL_THRESHOLD = float(os.getenv("EGM_SELL_THRESHOLD", "-0.1"))
        self.RSI_UPPER_THRESHOLD = float(os.getenv("RSI_UPPER_THRESHOLD", "65.0"))  # Ajustado a valores más sensibles
        self.RSI_LOWER_THRESHOLD = float(os.getenv("RSI_LOWER_THRESHOLD", "35.0"))  # Ajustado a valores más sensibles
        self.RECV_WINDOW = int(os.getenv("RECV_WINDOW", "5000"))
        self.MAX_OPEN_ORDERS = int(os.getenv("MAX_OPEN_ORDERS", "1"))
        self.MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "1000"))
        self.DEFAULT_SLEEP_TIME = int(os.getenv("DEFAULT_SLEEP_TIME", "5"))
        self.MAX_DRAWDOWN = float(os.getenv("MAX_DRAWDOWN", "0.1"))
        self.SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "300"))
        self.MIN_VOLATILITY = float(os.getenv("MIN_VOLATILITY", "0.01"))
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
        self.RETRY_DELAY_FACTOR = int(os.getenv("RETRY_DELAY_FACTOR", "2"))
        self.BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
        self.BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")


config = Config()


# endregion

# region Funciones auxiliares
def get_ntp_time() -> int:
    try:
        client = ntplib.NTPClient()
        response = client.request('pool.ntp.org', timeout=2)
        logger.info("✅ Tiempo NTP obtenido correctamente")
        return int(response.tx_time * 1000)
    except Exception as e:
        logger.error(f"❌ Error al obtener NTP: {e}")
        return int(time.time() * 1000)


async def fetch_data(session: aiohttp.ClientSession, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                logger.info(f"✅ Datos obtenidos de {url}: {data}")
                return data
            logger.error(f"❌ Error en {url}: {response.status}")
            return None
    except Exception as e:
        logger.error(f"❌ Excepción al obtener datos de {url}: {e}")
        return None


def timestamp_to_datetime(timestamp: Union[int, str]) -> datetime:
    try:
        return datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
    except Exception as e:
        logger.error(f"❌ Error al convertir timestamp {timestamp}: {e}")
        return datetime.now(timezone.utc)


def calculate_trade_volume(trade_data: List[Dict], time_window: int = 300) -> float:
    try:
        current_time = int(time.time() * 1000)
        recent_trades = [trade for trade in trade_data if current_time - int(trade["timestamp"]) <= time_window * 1000]
        volume = sum(float(trade.get("size", 0)) for trade in recent_trades)
        logger.info(f"📊 Volumen de trades calculado: {volume}")
        return volume
    except Exception as e:
        logger.error(f"Error en calculate_trade_volume: {e}")
        return 0.0


async def get_price_limits(session: aiohttp.ClientSession, symbol: str) -> Tuple[float, float]:
    url = f"{BASE_URL}/v5/market/instruments-info"
    params = {"category": "linear", "symbol": symbol}
    data = await fetch_data(session, url, params)
    if data and "result" in data and data["result"]["list"]:
        price_filter = data["result"]["list"][0]["priceFilter"]
        return float(price_filter["minPrice"]), float(price_filter["maxPrice"])
    logger.warning(f"⚠️ No se pudieron obtener límites de precio para {symbol}")
    return 0.0, float("inf")


async def get_quantity_precision(session: aiohttp.ClientSession, symbol: str) -> float:
    url = f"{BASE_URL}/v5/market/instruments-info"
    params = {"category": "linear", "symbol": symbol}
    data = await fetch_data(session, url, params)
    if data and "result" in data and data["result"]["list"]:
        return float(data["result"]["list"][0]["lotSizeFilter"]["qtyStep"])
    logger.warning(f"⚠️ No se pudo obtener precisión de cantidad para {symbol}")
    return 0.001


async def calculate_metrics(candle_data: List[Dict], orderbook_data: Dict, ticker_data: Dict, trade_data: List[Dict],
                            symbol: str, depth: int = 5) -> Dict[str, float]:
    if not all([candle_data, len(candle_data) >= 2, orderbook_data.get("bids"), orderbook_data.get("asks"),
                ticker_data.get('last_price')]):
        logger.warning("⚠️ Datos insuficientes para calcular métricas")
        return {"combined": 0.0, "ild": 0.0, "egm": 0.0, "pio": 0.0, "volatility": config.MIN_VOLATILITY, "atr": 0.0}

    try:
        async with aiohttp.ClientSession() as session:
            min_price, max_price = await get_price_limits(session, symbol)
        last_price = float(ticker_data["last_price"])
        if not (min_price <= last_price <= max_price):
            closes = np.array([float(c["close"]) for c in candle_data[-5:]], dtype=np.float64)
            last_price = float(np.mean(closes)) if len(closes) >= 5 else (min_price + max_price) / 2

        closes = np.array([float(c["close"]) for c in candle_data[-14:]], dtype=np.float64)
        highs = np.array([float(c.get("high", last_price)) for c in candle_data[-14:]], dtype=np.float64)
        lows = np.array([float(c.get("low", last_price)) for c in candle_data[-14:]], dtype=np.float64)
        volumes = np.array([float(c.get("volume", 0)) for c in candle_data[-5:]], dtype=np.float64)

        volatility = (float(ticker_data.get("high_24h", highs.max())) - float(
            ticker_data.get("low_24h", lows.min()))) / last_price
        volatility = max(volatility, config.MIN_VOLATILITY)

        atr = AverageTrueRange(high=pd.Series(highs), low=pd.Series(lows), close=pd.Series(closes),
                               window=14).average_true_range()
        atr_value = float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else volatility * last_price

        bids = orderbook_data["bids"][:depth]
        asks = orderbook_data["asks"][:depth]
        bid_volume = sum(float(bid[1]) for bid in bids)
        ask_volume = sum(float(ask[1]) for ask in asks)
        total_volume = bid_volume + ask_volume + 1e-6
        ild = np.clip((bid_volume - ask_volume) / total_volume, -1.0, 1.0)

        closes_short = closes[-5:]
        avg_price = float(np.mean(closes_short))
        price_range = float(max(highs[-5:].max() - lows[-5:].min(), 1e-8))
        egm = np.clip((last_price - avg_price) / price_range, -1.0, 1.0)

        pio = 0.0
        if trade_data:
            recent_volume = calculate_trade_volume(trade_data)
            avg_volume = float(ticker_data.get("volume_24h", 1e-6)) / (24 * 60 * 60 / 300)
            pio = np.clip((recent_volume - avg_volume) / avg_volume, -1.0, 1.0) if avg_volume > 0 else 0.0

        combined = np.clip((0.7 * egm + 0.3 * ild) * 10, -10.0, 10.0)
        logger.info(
            f"📊 Métricas calculadas para {symbol}: combined={combined}, ild={ild}, egm={egm}, pio={pio}, volatility={volatility}, atr={atr_value}")
        return {"combined": combined, "ild": ild, "egm": egm, "pio": pio, "volatility": volatility, "atr": atr_value}
    except Exception as e:
        logger.error(f"Error en calculate_metrics: {e}")
        return {"combined": 0.0, "ild": 0.0, "egm": 0.0, "pio": 0.0, "volatility": config.MIN_VOLATILITY, "atr": 0.0}


def calculate_tp_sl(price: float, volatility: float, atr: float, action: str, capital: float,
                    risk_per_trade: float = 0.01) -> Tuple[float, float]:
    if action.lower() not in ["buy", "sell"] or price <= 0 or volatility < 0:
        logger.warning(
            f"⚠️ Parámetros inválidos para calculate_tp_sl: action={action}, price={price}, volatility={volatility}")
        return 0.0, 0.0

    tp_distance = price * (config.TP_PERCENTAGE / 100)
    sl_distance = price * (config.SL_PERCENTAGE / 100)
    logger.info(
        f"📈 Calculando TP/SL: price={price}, TP_PERCENTAGE={config.TP_PERCENTAGE}, SL_PERCENTAGE={config.SL_PERCENTAGE}, tp_distance={tp_distance}, sl_distance={sl_distance}")

    if action.lower() == "buy":
        take_profit = price + tp_distance
        stop_loss = price - sl_distance
    else:
        take_profit = price - tp_distance
        stop_loss = price + sl_distance

    take_profit = round(take_profit, 2)
    stop_loss = round(stop_loss, 2)
    logger.info(f"📈 TP/SL calculados: action={action}, take_profit={take_profit}, stop_loss={stop_loss}")
    return take_profit, stop_loss


def calculate_rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        logger.warning(f"⚠️ Insuficientes precios para calcular RSI: {len(prices)}/{period + 1}")
        return 50.0
    series = pd.Series(prices)
    rsi = RSIIndicator(close=series, window=period).rsi()
    rsi_value = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0
    logger.info(f"📊 RSI calculado: {rsi_value}")
    return rsi_value


def calculate_ema(prices: List[float], window: int) -> float:
    if not prices or len(prices) < window:
        logger.warning(f"⚠️ Insuficientes precios para calcular EMA: {len(prices)}/{window}")
        return float(np.mean(prices)) if prices else 0.0
    series = pd.Series(prices)
    ema = EMAIndicator(close=series, window=window).ema_indicator()
    ema_value = float(ema.iloc[-1]) if not pd.isna(ema.iloc[-1]) else float(np.mean(prices))
    logger.info(f"📊 EMA calculada (ventana={window}): {ema_value}")
    return ema_value


def evaluate_trend(short_ema: float, mid_ema: float, long_ema: float) -> Tuple[str, str]:
    if short_ema > mid_ema > long_ema:
        return "BUY", "Cruzamiento alcista de Triple EMA"
    elif short_ema < mid_ema < long_ema:
        return "SELL", "Cruzamiento bajista de Triple EMA"
    return "HOLD", "No hay confirmación clara"


# endregion

# region Estrategia
class TpslStrategy:
    def __init__(self):
        self.short_window = 3
        self.mid_window = 6
        self.long_window = 12
        self.capital = config.CAPITAL_USDT
        self.signal_counts = {"BUY": 0, "SELL": 0, "HOLD": 0}

    def generate_signal(self, market_data: Dict[str, List[float]], metrics: Dict[str, float], current_price: float) -> \
            Dict[str, Any]:
        if not market_data.get("close_prices") or len(market_data["close_prices"]) < self.long_window:
            logger.warning(
                f"⚠️ Datos insuficientes para generar señal: {len(market_data.get('close_prices', []))}/{self.long_window}")
            self.signal_counts["HOLD"] += 1
            return {"action": "HOLD", "confidence": 0.0, "take_profit": 0.0, "stop_loss": 0.0,
                    "reason": "Datos insuficientes"}

        closing_prices = market_data["close_prices"]
        rsi = calculate_rsi(closing_prices)
        volatility = metrics.get("volatility", config.MIN_VOLATILITY)
        atr = metrics.get("atr", volatility * closing_prices[-1])
        ild = metrics.get("ild", 0.0)  # Imbalance Liquidity Delta
        egm = metrics.get("egm", 0.0)  # Exponential Growth Momentum
        pio = metrics.get("pio", 0.0)  # Price Impact of Orders
        combined = metrics.get("combined", 0.0)  # Métrica combinada

        logger.info(f"📈 RSI calculado: {rsi:.2f}, Volatilidad: {volatility:.4f}, ATR: {atr:.2f}")
        logger.info(f"📊 Métricas adicionales: ILD={ild:.4f}, EGM={egm:.4f}, PIO={pio:.4f}, Combined={combined:.4f}")

        # Decisión basada en RSI
        if rsi > config.RSI_UPPER_THRESHOLD:
            action, reason = "SELL", f"Sobrecompra (RSI={rsi:.2f})"
            self.signal_counts["SELL"] += 1
        elif rsi < config.RSI_LOWER_THRESHOLD:
            action, reason = "BUY", f"Sobreventa (RSI={rsi:.2f})"
            self.signal_counts["BUY"] += 1
        # Decisión basada en métricas combinadas
        elif combined > config.EGM_BUY_THRESHOLD and egm > 0:
            action, reason = "BUY", f"Señal alcista (Combined={combined:.2f}, EGM={egm:.2f})"
            self.signal_counts["BUY"] += 1
        elif combined < config.EGM_SELL_THRESHOLD and egm < 0:
            action, reason = "SELL", f"Señal bajista (Combined={combined:.2f}, EGM={egm:.2f})"
            self.signal_counts["SELL"] += 1
        # Decisión basada en desequilibrio de liquidez
        elif ild > 0.5:  # Fuerte desequilibrio hacia compras
            action, reason = "BUY", f"Desequilibrio de liquidez alcista (ILD={ild:.2f})"
            self.signal_counts["BUY"] += 1
        elif ild < -0.5:  # Fuerte desequilibrio hacia ventas
            action, reason = "SELL", f"Desequilibrio de liquidez bajista (ILD={ild:.2f})"
            self.signal_counts["SELL"] += 1
        else:
            # Si no hay señales claras de las métricas, usar EMAs
            short_ema = calculate_ema(closing_prices, self.short_window)
            mid_ema = calculate_ema(closing_prices, self.mid_window)
            long_ema = calculate_ema(closing_prices, self.long_window)
            logger.info(f"📈 EMAs calculadas: Short={short_ema:.2f}, Mid={mid_ema:.2f}, Long={long_ema:.2f}")
            action, reason = evaluate_trend(short_ema, mid_ema, long_ema)
            self.signal_counts[action] += 1

        take_profit, stop_loss = (calculate_tp_sl(current_price, volatility, atr, action, self.capital)
                                  if action in ["BUY", "SELL"] else (0.0, 0.0))

        return {
            "action": action,
            "confidence": 0.9 if action in ["BUY", "SELL"] else 0.5,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "reason": reason,
            "metrics": metrics,
            "rsi": rsi
        }


# endregion

# region Clase Principal
class NertzMetalEngine:
    def __init__(self) -> None:
        self.timeframe = config.TIMEFRAME
        self.symbols = config.SYMBOL.split(",")
        self.capital = self.initial_capital = config.CAPITAL_USDT
        self.positions = {symbol: [] for symbol in self.symbols}
        self.trade_data = {symbol: [] for symbol in self.symbols}
        self.iterations = 0
        self.ws = None
        self.running = True
        self.orderbook_data = {symbol: {"bids": [], "asks": []} for symbol in self.symbols}
        self.ticker_data = {symbol: {"last_price": 0.0, "volume_24h": 0.0, "high_24h": 0.0, "low_24h": 0.0} for symbol
                            in self.symbols}
        self.candles = {symbol: [] for symbol in self.symbols}
        self.errors = []  # Initialize errors list before calling methods that might use it
        self.trade_id_counter = self._load_initial_trade_id()
        self.session_start = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        self.trade_buffer = {symbol: [] for symbol in self.symbols}
        self.buffer_size = 10
        self.strategy = TpslStrategy()
        self.last_sync_time = self.last_orderbook_log = 0
        self.last_trade_time = {symbol: datetime.min.replace(tzinfo=timezone.utc) for symbol in self.symbols}
        self.last_kline_time = {symbol: 0 for symbol in self.symbols}
        self.connection_attempts = 0
        self.max_connection_attempts = 5
        self.max_candles = 100
        self.max_trades = 1000
        self.last_failed_order = {symbol: None for symbol in self.symbols}
        self.last_failed_order_time = {symbol: datetime.min.replace(tzinfo=timezone.utc) for symbol in self.symbols}
        self._initialize_capital()
        self._load_positions()
        # Sincronizar posiciones abiertas con Bybit al inicio
        self._sync_positions_with_bybit()

    def _sync_positions_with_bybit(self) -> None:
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        for symbol in self.symbols:
            try:
                response = session.get_positions(category="linear", symbol=symbol)
                if response.get("retCode") == 0:
                    for pos in response["result"]["list"]:
                        if float(pos["size"]) > 0:
                            # Verificar si la posición ya está en self.positions
                            if not any(p["order_id"] == pos["positionIdx"] for p in self.positions[symbol]):
                                self.positions[symbol].append({
                                    "trade_id": f"pos-{pos['positionIdx']}",
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "symbol": symbol,
                                    "action": "buy" if pos["side"] == "Buy" else "sell",
                                    "entry_price": float(pos["avgPrice"]),
                                    "quantity": float(pos["size"]),
                                    "status": "open",
                                    "order_id": pos["positionIdx"],
                                    "sl": float(pos.get("stopLoss", 0.0)),
                                    "tp": float(pos.get("takeProfit", 0.0))
                                })
                                logger.info(
                                    f"✅ Sincronizada posición abierta desde Bybit para {symbol}: {pos['side']} {pos['size']} @ {pos['avgPrice']}")
                    logger.info(f"✅ {symbol}: {len(self.positions[symbol])} posiciones sincronizadas")
                else:
                    error_msg = f"❌ Error al sincronizar posiciones para {symbol}: {response.get('retMsg')}"
                    logger.error(error_msg)
                    self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
            except Exception as e:
                error_msg = f"❌ Error al sincronizar posiciones abiertas desde Bybit para {symbol}: {e}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

    def _initialize_capital(self) -> None:
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        try:
            wallet_response = session.get_wallet_balance(accountType="UNIFIED")
            if wallet_response.get("retCode") == 0:
                wallet = wallet_response["result"]["list"][0]["coin"]
                self.capital = self.initial_capital = next(
                    (float(c["walletBalance"]) for c in wallet if c["coin"] == "USDT"), config.CAPITAL_USDT)
                logger.info(f"✅ Capital inicial: {self.capital:.2f} USDT")
            else:
                error_msg = f"❌ Error al obtener saldo: {wallet_response.get('retMsg')}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
        except Exception as e:
            error_msg = f"❌ Error al inicializar capital: {e}"
            logger.error(error_msg)
            self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

    def _load_initial_trade_id(self) -> int:
        with SessionLocal() as db:
            try:
                last_trade = db.query(Trade.trade_id).order_by(Trade.trade_id.desc()).first()
                return last_trade[0] + 1 if last_trade else 1
            except Exception as e:
                logger.warning(f"⚠ No se pudo cargar trade_id: {e}")
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(),
                                    "message": f"No se pudo cargar trade_id: {e}"})
                return 1

    def _load_positions(self) -> None:
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        for symbol in self.symbols:
            try:
                response = session.get_open_orders(category="linear", symbol=symbol)
                if response.get("retCode") == 0:
                    self.positions[symbol] = []  # Reiniciar posiciones para este símbolo
                    for order in response["result"]["list"]:
                        try:
                            # Manejar campos potencialmente vacíos con valores predeterminados
                            price = float(order.get("price", "0.0") or "0.0")
                            qty = float(order.get("qty", "0.0") or "0.0")
                            stop_loss = float(order.get("stopLoss", "0.0") or "0.0")
                            take_profit = float(order.get("takeProfit", "0.0") or "0.0")

                            self.positions[symbol].append({
                                "trade_id": f"existing-{order['orderId']}",
                                "timestamp": timestamp_to_datetime(int(order["createdTime"])).isoformat(),
                                "symbol": symbol,
                                "action": order["side"].lower(),
                                "entry_price": price,
                                "quantity": qty,
                                "status": "open",
                                "order_id": order["orderId"],
                                "sl": stop_loss,
                                "tp": take_profit
                            })
                        except ValueError as ve:
                            logger.error(
                                f"❌ Error al procesar orden {order.get('orderId', 'desconocido')} para {symbol}: "
                                f"Conversión a float falló - Datos: {order}, Error: {ve}")
                            self.errors.append({
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "message": f"Error al convertir datos de orden {order.get('orderId')} para {symbol}: {ve}"
                            })
                        except Exception as e:
                            logger.error(
                                f"❌ Error inesperado al procesar orden {order.get('orderId', 'desconocido')} para {symbol}: {e}")
                            self.errors.append({
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "message": f"Error inesperado al procesar orden para {symbol}: {e}"
                            })
                    logger.info(f"✅ {symbol}: {len(self.positions[symbol])} posiciones abiertas cargadas")
                else:
                    error_msg = f"❌ Error al cargar posiciones para {symbol}: {response.get('retMsg')}"
                    logger.error(error_msg)
                    self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
            except Exception as e:
                error_msg = f"❌ Error al cargar posiciones para {symbol}: {e}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

    async def fetch_initial_data(self) -> None:
        async with aiohttp.ClientSession() as session:
            for symbol in self.symbols:
                # Asegurarse de que el interval tenga el formato correcto (por ejemplo, "5" para la API)
                interval = self.timeframe.replace("m", "")  # "5m" -> "5"
                params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": "200"}
                kline_response = await fetch_data(session, f"{BASE_URL}/v5/market/kline", params)
                if kline_response and "result" in kline_response:
                    with SessionLocal() as db:
                        try:
                            for k in kline_response["result"]["list"]:
                                candle = MarketData(
                                    timestamp=timestamp_to_datetime(int(k[0])),
                                    symbol=symbol,
                                    open=float(k[1]),
                                    high=float(k[2]),
                                    low=float(k[3]),
                                    close=float(k[4]),
                                    volume=float(k[5]) or 0.001,
                                    interval=self.timeframe  # Asignar el intervalo (por ejemplo, "5m")
                                )
                                # Verificar si el dato ya existe para evitar duplicados
                                if not db.query(MarketData).filter_by(
                                        timestamp=candle.timestamp,
                                        symbol=symbol,
                                        interval=candle.interval
                                ).first():
                                    db.add(candle)
                            db.commit()
                            # Actualizar el estado interno del bot
                            self.candles[symbol] = [
                                {"timestamp": c[0], "open": float(c[1]), "high": float(c[2]), "low": float(c[3]),
                                 "close": float(c[4]), "volume": float(c[5])} for c in kline_response["result"]["list"]
                            ]
                            if self.candles[symbol]:
                                self.last_kline_time[symbol] = int(kline_response["result"]["list"][0][0])
                            logger.info(f"📈 {symbol}: {len(self.candles[symbol])} velas iniciales cargadas")
                        except Exception as e:
                            logger.error(f"❌ Error al guardar velas iniciales para {symbol}: {e}")
                            db.rollback()
                            self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": str(e)})
                else:
                    error_msg = f"❌ Fallo al cargar datos iniciales para {symbol}"
                    logger.error(error_msg)
                    self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

    async def start_async(self) -> None:
        logger.info(f"🔥 Iniciando bot para {self.symbols}")
        await self.fetch_initial_data()
        asyncio.create_task(self._periodic_sync())
        asyncio.create_task(self._periodic_kline_fetch())
        # Opcional: Tarea separada para actualizar órdenes más frecuentemente
        asyncio.create_task(self._periodic_order_update())
        await self._connect_websocket_async()

    # Método auxiliar para actualización periódica de órdenes (opcional)
    async def _periodic_order_update(self) -> None:
        while self.running:
            with SessionLocal() as db:
                await self._update_pending_orders(db)
            await asyncio.sleep(30)  # Actualiza cada 30 segundos

    async def _periodic_sync(self) -> None:
        while self.running:
            if time.time() - self.last_sync_time >= config.SYNC_INTERVAL:
                self._initialize_capital()
                self._sync_positions()
                with SessionLocal() as db:
                    await self._update_pending_orders(db)  # Actualizar órdenes pendientes
                    await self._save_results("BTCUSDT")
                self.last_sync_time = time.time()
            await asyncio.sleep(60)

    async def _periodic_kline_fetch(self) -> None:
        while self.running:
            await asyncio.sleep(60)
            for symbol in self.symbols:
                current_time = time.time() * 1000
                last_kline = self.last_kline_time.get(symbol, 0)
                if current_time - last_kline > 60000 or last_kline == 0:
                    logger.warning(f"⚠️ No se han recibido klines para {symbol} en 60 segundos, recargando datos...")
                    await self.fetch_initial_data()
                    with SessionLocal() as db:
                        await self._execute_trade(symbol, db)

    def _sync_positions(self) -> None:
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        for symbol in self.symbols:
            try:
                response = session.get_positions(category="linear", symbol=symbol)
                if response.get("retCode") == 0:
                    self.positions[symbol] = []
                    for pos in response["result"]["list"]:
                        if float(pos["size"]) > 0:
                            self.positions[symbol].append({
                                "trade_id": f"pos-{pos['positionIdx']}",
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "symbol": symbol,
                                "action": "buy" if pos["side"] == "Buy" else "sell",
                                "entry_price": float(pos["avgPrice"]),
                                "quantity": float(pos["size"]),
                                "status": "open",
                                "order_id": None,
                                "sl": float(pos.get("stopLoss", 0.0)),
                                "tp": float(pos.get("takeProfit", 0.0))
                            })
                    logger.info(f"✅ {symbol}: {len(self.positions[symbol])} posiciones sincronizadas")
                else:
                    error_msg = f"❌ Error al sincronizar posiciones para {symbol}: {response.get('retMsg')}"
                    logger.error(error_msg)
                    self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
            except Exception as e:
                error_msg = f"❌ Error al sincronizar posiciones para {symbol}: {e}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

    async def _connect_websocket_async(self) -> None:
        while self.running and self.connection_attempts < self.max_connection_attempts:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    self.connection_attempts = 0
                    logger.info("🌐 WebSocket conectado")
                    await self._resubscribe_async()
                    async for message in ws:
                        data = json.loads(message)
                        if "retCode" in data and data["retCode"] != 0:
                            error_msg = f"❌ Error en WebSocket: {data['retMsg']} (retCode: {data['retCode']})"
                            logger.error(error_msg)
                            self.errors.append(
                                {"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
                            if data["retCode"] == 10001:  # Rate limit exceeded
                                await asyncio.sleep(60)
                                break
                        await self._on_message(ws, message)
            except websockets.ConnectionClosed as e:
                # Usando la API actualizada de websockets
                close_code = getattr(e, "code", None)
                close_reason = getattr(e, "reason", None)
                error_msg = f"⚠ WebSocket cerrado: {close_code} - {close_reason}"
                logger.warning(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
                self.connection_attempts += 1
                await asyncio.sleep(min(5 * self.connection_attempts, 60))
            except Exception as e:
                error_msg = f"❌ Error inesperado en WebSocket: {e}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
                self.connection_attempts += 1
                await asyncio.sleep(5)
        if self.connection_attempts >= self.max_connection_attempts:
            logger.critical("❌ Máximo de intentos de reconexión alcanzado")
            self.stop()

    async def _resubscribe_async(self) -> None:
        interval = self.timeframe.replace("m", "")
        for symbol in self.symbols:
            subscription = {
                "op": "subscribe",
                "args": [f"publicTrade.{symbol}", f"kline.{interval}.{symbol}", f"orderbook.50.{symbol}",
                         f"tickers.{symbol}"]
            }
            await self.ws.send(json.dumps(subscription))
            logger.info(f"📡 Suscrito a {symbol}: {subscription['args']}")

    async def _on_message(self, ws: Any, message: str) -> None:
        with SessionLocal() as db:
            try:
                data = json.loads(message)
                if "topic" not in data:
                    if data.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong", "ts": get_ntp_time()}))
                    return

                # Extraer el símbolo del topic
                topic_parts = data["topic"].split(".")
                symbol = topic_parts[-1]
                if symbol not in self.symbols:
                    return

                try:
                    if "kline" in data["topic"] and "data" in data and len(data["data"]) > 0:
                        await self._handle_kline(symbol, data["data"][0], db)
                    elif "orderbook" in data["topic"] and "data" in data:
                        await self._handle_orderbook(symbol, data, db)
                    elif "tickers" in data["topic"] and "data" in data:
                        await self._handle_ticker(symbol, data["data"], db)
                        await self._execute_trade_on_ticker(symbol, db)
                    elif "publicTrade" in data["topic"] and "data" in data:
                        self.trade_data[symbol].extend(
                            [{"timestamp": t.get("T"), "size": t.get("v"), "price": t.get("p"), "side": t.get("S")}
                             for t in data["data"] if all(k in t for k in ["T", "v", "p", "S"])])
                        self.trade_data[symbol] = self.trade_data[symbol][-self.max_trades:]
                except Exception as e:
                    error_msg = f"❌ Error al procesar mensaje para {symbol}: {str(e)}"
                    logger.error(error_msg)
                    self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
            except Exception as e:
                error_msg = f"❌ Error en mensaje: {str(e)}, Mensaje: {message}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

    async def _handle_kline(self, symbol: str, kline: Dict, db: Session) -> None:
        timestamp = timestamp_to_datetime(int(kline["start"]))
        candle = MarketData(
            timestamp=timestamp,
            symbol=symbol,
            open=float(kline["open"]),
            high=float(kline["high"]),
            low=float(kline["low"]),
            close=float(kline["close"]),
            volume=float(kline["volume"]) or 0.001,
            interval=self.timeframe  # Añadir el intervalo que es obligatorio
        )
        existing_candle = db.query(MarketData).filter_by(timestamp=candle.timestamp, symbol=symbol, interval=candle.interval).first()
        if not existing_candle:
            db.add(candle)
            db.commit()
            self.candles[symbol].append({
                "timestamp": timestamp.isoformat(),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume
            })
            self.candles[symbol] = self.candles[symbol][-self.max_candles:]
            self.last_kline_time[symbol] = int(kline["start"])
            logger.info(f"✅ Nueva vela añadida para {symbol}: close={candle.close}")
            await self._execute_trade(symbol, db)

    async def _handle_orderbook(self, symbol: str, data: Dict, db: Session) -> None:
        try:
            if data["type"] == "snapshot":
                self.orderbook_data[symbol] = {"bids": data["data"]["b"], "asks": data["data"]["a"]}
            elif data["type"] == "delta":
                bid_dict = {float(b[0]): float(b[1]) for b in self.orderbook_data[symbol]["bids"]}
                ask_dict = {float(a[0]): float(a[1]) for a in self.orderbook_data[symbol]["asks"]}
                for price, qty in data["data"]["b"]:
                    price, qty = float(price), float(qty)
                    if qty > 0:
                        bid_dict[price] = qty
                    elif price in bid_dict:
                        del bid_dict[price]
                for price, qty in data["data"]["a"]:
                    price, qty = float(price), float(qty)
                    if qty > 0:
                        ask_dict[price] = qty
                    elif price in ask_dict:
                        del ask_dict[price]
                self.orderbook_data[symbol] = {
                    "bids": [[str(p), str(q)] for p, q in sorted(bid_dict.items(), reverse=True)][:50],
                    "asks": [[str(p), str(q)] for p, q in sorted(ask_dict.items())][:50]
                }

            # Asegurarse de que los datos estén en formato JSON válido
            bids_json = json.dumps(self.orderbook_data[symbol]["bids"])
            asks_json = json.dumps(self.orderbook_data[symbol]["asks"])

            orderbook = Orderbook(
                timestamp=datetime.now(timezone.utc),
                symbol=symbol,
                bids=bids_json,
                asks=asks_json
            )
            db.add(orderbook)
            db.commit()
        except Exception as e:
            logger.error(f"❌ Error al procesar orderbook para {symbol}: {e}")
            db.rollback()

    async def _handle_ticker(self, symbol: str, ticker: Dict, db: Session) -> None:
        try:
            if "lastPrice" in ticker:
                last_price = float(ticker["lastPrice"])
                self.ticker_data[symbol]["last_price"] = last_price
            if "volume24h" in ticker:
                self.ticker_data[symbol]["volume_24h"] = float(ticker["volume24h"])
            if "highPrice24h" in ticker:
                self.ticker_data[symbol]["high_24h"] = float(ticker["highPrice24h"])
            if "lowPrice24h" in ticker:
                self.ticker_data[symbol]["low_24h"] = float(ticker["lowPrice24h"])

            # Create a safe dictionary with all required fields explicitly set
            ticker_data = {
                "timestamp": datetime.now(timezone.utc),
                "symbol": symbol,
                "last_price": self.ticker_data[symbol]["last_price"],
                "volume_24h": self.ticker_data[symbol]["volume_24h"],
                "high_24h": self.ticker_data[symbol]["high_24h"],
                "low_24h": self.ticker_data[symbol]["low_24h"],
                # Explicitly set to None to prevent errors
                "bid_price": None,
                "ask_price": None
            }

            # Create and save the market ticker with safe values
            market_ticker = MarketTicker(**ticker_data)
            db.add(market_ticker)
            try:
                db.commit()
                logger.info(f"📊 Ticker actualizado para {symbol}: last_price={self.ticker_data[symbol]['last_price']}")
            except Exception as commit_error:
                db.rollback()
                logger.error(f"❌ Error al guardar ticker en la base de datos: {commit_error}")
                # Try to retry with minimal data if specific columns are causing problems
                try:
                    # Create a minimal version with only essential columns
                    minimal_ticker = MarketTicker(
                        timestamp=datetime.now(timezone.utc),
                        symbol=symbol,
                        last_price=self.ticker_data[symbol]["last_price"],
                        volume_24h=self.ticker_data[symbol]["volume_24h"],
                        high_24h=self.ticker_data[symbol]["high_24h"],
                        low_24h=self.ticker_data[symbol]["low_24h"]
                    )
                    db.add(minimal_ticker)
                    db.commit()
                    logger.info(f"📊 Ticker actualizado con datos mínimos para {symbol}")
                except Exception as retry_error:
                    db.rollback()
                    logger.error(f"❌ Error en segundo intento de guardar ticker: {retry_error}")
        except Exception as e:
            logger.error(f"❌ Error al procesar ticker para {symbol}: {e}")
            db.rollback()

    async def _close_position(self, symbol: str, position: Dict, db: Session) -> bool:
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        try:
            # Verificar si hay posiciones mixtas (largas y cortas)
            has_long = any(p["action"] == "buy" and p["status"] == "open" for p in self.positions[symbol])
            has_short = any(p["action"] == "sell" and p["status"] == "open" for p in self.positions[symbol])
            mixed_positions = has_long and has_short

            close_side = "Sell" if position["action"] == "buy" else "Buy"
            close_params = {
                "category": "linear",
                "symbol": symbol,
                "side": close_side,
                "orderType": "Market",
                "qty": f"{position['quantity']:.3f}",
                "timeInForce": "GTC",
            }

            # Solo usar reduceOnly si no hay posiciones mixtas
            if not mixed_positions:
                close_params["reduceOnly"] = True

            logger.info(f"🔄 Enviando orden de cierre para {symbol}: {close_side} {position['quantity']} @ Market")
            response = session.place_order(**close_params)

            if response.get("retCode") == 0:
                # Obtener el ID de la orden de cierre para rastrearla
                close_order_id = response["result"]["orderId"]
                logger.info(f"✅ Orden de cierre enviada: {close_order_id}")
                
                # Esperar a que la orden se ejecute y obtener el precio real de ejecución
                max_retries = 5
                for retry in range(max_retries):
                    try:
                        # Consultar el estado de la orden de cierre
                        order_response = session.get_order_history(
                            category="linear",
                            symbol=symbol,
                            orderId=close_order_id
                        )
                        
                        if order_response.get("retCode") == 0 and order_response["result"]["list"]:
                            close_order = order_response["result"]["list"][0]
                            order_status = close_order["orderStatus"]
                            
                            if order_status == "Filled":
                                # Obtener el precio real de ejecución
                                execution_price = float(close_order["avgPrice"])
                                logger.info(f"✅ Orden de cierre ejecutada a precio: {execution_price}")
                                
                                try:
                                    # Actualizar la BD y el estado con el precio real de ejecución
                                    trade = db.query(Trade).filter_by(order_id=position["order_id"]).first()
                                    if trade:
                                        # Asegurarse de que el estado se actualice a "closed"
                                        trade.status = "closed"
                                        
                                        # Actualizar objeto Position - CRÍTICO para cerrar posiciones con pérdida
                                        position_obj = db.query(Position).filter_by(order_id=position["order_id"]).first()
                                        if position_obj:
                                            # Forzar el cierre completo de la posición
                                            position_obj.status = "closed"
                                            position_obj.last_update = datetime.now(timezone.utc)
                                            position_obj.filled_quantity = position_obj.quantity  # Asegurar que se marca como completamente llenada
                                            position_obj.remaining_quantity = 0.0  # Asegurar que no queda cantidad pendiente
                                            position_obj.execution_status = "filled"  # Marcar como completamente ejecutada
                                            position_obj.close_time = datetime.now(timezone.utc)  # Registrar tiempo de cierre
                                            logger.info(f"✅ Posición actualizada en la base de datos: {position['order_id']}")

                                        # Calcular P/L con el precio real de ejecución
                                        trade.exit_price = execution_price  # Usar precio real de ejecución en lugar del ticker
                                        
                                        # Cálculo correcto de P/L según la dirección de la operación
                                        if position["action"] == "buy":
                                            # Para posiciones largas: (precio_salida - precio_entrada) * cantidad
                                            profit_loss = (execution_price - trade.entry_price) * position["quantity"]
                                        else:
                                            # Para posiciones cortas: (precio_entrada - precio_salida) * cantidad
                                            profit_loss = (trade.entry_price - execution_price) * position["quantity"]
                                        
                                        # Aplicar comisiones
                                        trade.profit_loss = profit_loss * (1 - config.FEE_RATE)
                                        
                                        # Actualizar campos adicionales para asegurar cierre completo
                                        trade.updated_at = datetime.now(timezone.utc)
                                        
                                        # Guardar cambios en la base de datos
                                        try:
                                            db.commit()
                                            logger.info(f"✅ Cambios guardados en la base de datos para la orden {position['order_id']}")
                                        except Exception as commit_error:
                                            logger.error(f"❌ Error al hacer commit para la orden {position['order_id']}: {commit_error}")
                                            db.rollback()
                                            # Intentar nuevamente con una nueva sesión
                                            with SessionLocal() as new_db:
                                                try:
                                                    # Volver a obtener los objetos con la nueva sesión
                                                    new_trade = new_db.query(Trade).filter_by(order_id=position["order_id"]).first()
                                                    new_position_obj = new_db.query(Position).filter_by(order_id=position["order_id"]).first()
                                                    
                                                    if new_trade and new_position_obj:
                                                        # Actualizar nuevamente
                                                        new_trade.status = "closed"
                                                        new_trade.exit_price = execution_price
                                                        new_trade.profit_loss = profit_loss * (1 - config.FEE_RATE)
                                                        new_trade.updated_at = datetime.now(timezone.utc)
                                                        
                                                        new_position_obj.status = "closed"
                                                        new_position_obj.last_update = datetime.now(timezone.utc)
                                                        new_position_obj.filled_quantity = new_position_obj.quantity
                                                        new_position_obj.remaining_quantity = 0.0
                                                        new_position_obj.execution_status = "filled"
                                                        new_position_obj.close_time = datetime.now(timezone.utc)
                                                        
                                                        new_db.commit()
                                                        logger.info(f"✅ Cambios guardados en la base de datos con nueva sesión para la orden {position['order_id']}")
                                                        
                                                        # Actualizar el diccionario de posición en memoria con los valores de la nueva sesión
                                                        position["status"] = "closed"
                                                        position["exit_price"] = new_trade.exit_price
                                                        position["profit_loss"] = new_trade.profit_loss
                                                        self.capital += new_trade.profit_loss
                                                        
                                                        # Registrar resultado en logs
                                                        result_type = "ganancia" if new_trade.profit_loss > 0 else "pérdida"
                                                        logger.info(f"✅ Posición cerrada para {symbol}: {position['action']} {position['quantity']} @ {new_trade.exit_price}, P/L={new_trade.profit_loss:.2f} ({result_type})")
                                                        await self._save_results(symbol, new_trade)
                                                except Exception as new_commit_error:
                                                    logger.error(f"❌ Error al hacer commit con nueva sesión para la orden {position['order_id']}: {new_commit_error}")
                                                    new_db.rollback()
                                        
                                        # Actualizar el diccionario de posición en memoria
                                        position["status"] = "closed"
                                        position["exit_price"] = trade.exit_price
                                        position["profit_loss"] = trade.profit_loss
                                        self.capital += trade.profit_loss
                                        
                                        # Registrar resultado en logs
                                        result_type = "ganancia" if trade.profit_loss > 0 else "pérdida"
                                        logger.info(f"✅ Posición cerrada para {symbol}: {position['action']} {position['quantity']} @ {trade.exit_price}, P/L={trade.profit_loss:.2f} ({result_type})")
                                        await self._save_results(symbol, trade)
                                    return True
                                except Exception as e:
                                    logger.error(f"❌ Error al procesar cierre de posición: {e}")
                                    return False
                            elif order_status == "Rejected" or order_status == "Cancelled":
                                logger.error(f"❌ Orden de cierre rechazada o cancelada: {order_status}")
                                break
                            else:
                                # La orden aún está en proceso, esperar y reintentar
                                logger.info(f"⏳ Orden de cierre en estado: {order_status}, esperando... (intento {retry+1}/{max_retries})")
                                await asyncio.sleep(1)  # Esperar 1 segundo antes de verificar nuevamente
                        else:
                            logger.error(f"❌ Error al consultar orden de cierre: {order_response.get('retMsg', 'Error desconocido')}")
                            await asyncio.sleep(1)  # Esperar antes de reintentar
                    except Exception as e:
                        logger.error(f"❌ Error al verificar estado de orden de cierre: {str(e)}")
                        await asyncio.sleep(1)  # Esperar antes de reintentar
                
                # Si llegamos aquí sin retornar, significa que no pudimos confirmar que la orden se ejecutó
                logger.warning(f"⚠️ No se pudo confirmar la ejecución de la orden de cierre después de {max_retries} intentos")
                return False
            else:
                error_msg = f"❌ Fallo al cerrar posición para {symbol}: {response.get('retMsg')}"
                logger.error(error_msg)
                self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})

                # Si el error es por posiciones mixtas, reintentamos sin reduceOnly
                if response.get("retCode") == 110017 and "reduceOnly" in close_params:
                    logger.info(f"🔄 Reintentando cerrar posición sin reduceOnly para {symbol}")
                    del close_params["reduceOnly"]
                    retry_response = session.place_order(**close_params)
                    if retry_response.get("retCode") == 0:
                        # Obtener el ID de la orden de cierre para rastrearla
                        close_order_id = retry_response["result"]["orderId"]
                        logger.info(f"✅ Orden de cierre enviada en segundo intento: {close_order_id}")
                        
                        # Esperar a que la orden se ejecute y obtener el precio real de ejecución
                        max_retries = 5
                        for retry in range(max_retries):
                            try:
                                # Consultar el estado de la orden de cierre
                                order_response = session.get_order_history(
                                    category="linear",
                                    symbol=symbol,
                                    orderId=close_order_id
                                )
                                
                                if order_response.get("retCode") == 0 and order_response["result"]["list"]:
                                    close_order = order_response["result"]["list"][0]
                                    order_status = close_order["orderStatus"]
                                    
                                    if order_status == "Filled":
                                        # Obtener el precio real de ejecución
                                        execution_price = float(close_order["avgPrice"])
                                        logger.info(f"✅ Orden de cierre ejecutada en segundo intento a precio: {execution_price}")
                                        
                                        # Actualizar la BD y el estado con el precio real de ejecución
                                        trade = db.query(Trade).filter_by(order_id=position["order_id"]).first()
                                        if trade:
                                            trade.status = "closed"
                                            position_obj = db.query(Position).filter_by(order_id=position["order_id"]).first()
                                            if position_obj:
                                                position_obj.status = "closed"
                                                position_obj.last_update = datetime.now(timezone.utc)
                                                position_obj.filled_quantity = position_obj.quantity
                                                position_obj.remaining_quantity = 0.0
                                                position_obj.execution_status = "filled"
                                                position_obj.close_time = datetime.now(timezone.utc)
                                            
                                            trade.exit_price = execution_price  # Usar precio real de ejecución
                                            profit_loss = (trade.exit_price - trade.entry_price) * position["quantity"] if position["action"] == "buy" else \
                                                          (trade.entry_price - trade.exit_price) * position["quantity"]
                                            trade.profit_loss = profit_loss * (1 - config.FEE_RATE)
                                            db.commit()
                                            position["status"] = "closed"
                                            position["exit_price"] = trade.exit_price
                                            position["profit_loss"] = trade.profit_loss
                                            self.capital += trade.profit_loss
                                            result_type = "ganancia" if trade.profit_loss > 0 else "pérdida"
                                            logger.info(f"✅ Posición cerrada en segundo intento para {symbol}: P/L={trade.profit_loss:.2f} ({result_type})")
                                            await self._save_results(symbol, trade)
                                        return True
                                    elif order_status == "Rejected" or order_status == "Cancelled":
                                        logger.error(f"❌ Orden de cierre en segundo intento rechazada o cancelada: {order_status}")
                                        break
                                    else:
                                        # La orden aún está en proceso, esperar y reintentar
                                        logger.info(f"⏳ Orden de cierre en segundo intento en estado: {order_status}, esperando... (intento {retry+1}/{max_retries})")
                                        await asyncio.sleep(1)  # Esperar 1 segundo antes de verificar nuevamente
                                else:
                                    logger.error(f"❌ Error al consultar orden de cierre en segundo intento: {order_response.get('retMsg', 'Error desconocido')}")
                                    await asyncio.sleep(1)  # Esperar antes de reintentar
                            except Exception as e:
                                logger.error(f"❌ Error al verificar estado de orden de cierre en segundo intento: {str(e)}")
                                await asyncio.sleep(1)  # Esperar antes de reintentar
                        
                        # Si llegamos aquí sin retornar, significa que no pudimos confirmar que la orden se ejecutó
                        logger.warning(f"⚠️ No se pudo confirmar la ejecución de la orden de cierre en segundo intento después de {max_retries} intentos")
                        return False

                return False
        except Exception as e:
            error_msg = f"❌ Error al cerrar posición para {symbol}: {str(e)}"
            logger.error(error_msg)
            self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
            return False

    async def _execute_trade(self, symbol: str, db: Session) -> None:
        logger.info(f"🔍 Verificando trade para {symbol}: {len(self.candles[symbol])} velas disponibles")
        if len(self.candles[symbol]) < 14:
            logger.warning(f"⚠️ Insuficientes velas para {symbol}: {len(self.candles[symbol])}/14")
            await self._save_results(symbol)
            return

        if not self._check_drawdown():
            logger.warning(f"⚠️ Drawdown máximo alcanzado para {symbol}")
            await self._save_results(symbol)
            return

        current_time = datetime.now(timezone.utc)
        if current_time < self.last_trade_time[symbol] + timedelta(seconds=config.DEFAULT_SLEEP_TIME):
            logger.info(
                f"⏳ Cooldown activo para {symbol}, próxima ejecución en {(self.last_trade_time[symbol] + timedelta(seconds=config.DEFAULT_SLEEP_TIME) - current_time).total_seconds():.2f} segundos")
            await self._save_results(symbol)
            return

        metrics = await calculate_metrics(self.candles[symbol], self.orderbook_data[symbol], self.ticker_data[symbol],
                                          self.trade_data[symbol], symbol)
        last_price = self.ticker_data[symbol]["last_price"]
        if last_price <= 0:
            logger.warning(f"⚠️ Precio inválido para {symbol}: {last_price}, omitiendo operación")
            await self._save_results(symbol)
            return

        signal = self.strategy.generate_signal({"close_prices": [c["close"] for c in self.candles[symbol]]}, metrics,
                                               last_price)
        logger.info(f"📊 Señal para {symbol}: {signal['action']} - {signal['reason']}")

        if signal["action"] == "HOLD":
            logger.info(f"⏳ Señal HOLD, no se ejecutará ninguna acción para {symbol}")
            await self._save_results(symbol)
            return

        open_positions = [pos for pos in self.positions[symbol] if pos["status"] == "open"]
        for position in open_positions:
            if (signal["action"] == "SELL" and position["action"] == "buy") or (
                    signal["action"] == "BUY" and position["action"] == "sell"):
                logger.info(
                    f"📉 Cerrando posición {position['action']} para {symbol} debido a señal opuesta: {signal['action']}")
                await self._close_position(symbol, position, db)
            elif position["action"] == "buy" and last_price <= position["sl"]:
                logger.info(
                    f"📉 Cerrando posición {position['action']} para {symbol} porque el precio ({last_price}) cayó por debajo del stop_loss ({position['sl']})")
                await self._close_position(symbol, position, db)
            elif position["action"] == "sell" and last_price >= position["sl"]:
                logger.info(
                    f"📉 Cerrando posición {position['action']} para {symbol} porque el precio ({last_price}) superó el stop_loss ({position['sl']})")
                await self._close_position(symbol, position, db)
            elif position["action"] == "buy" and signal["rsi"] >= config.RSI_UPPER_THRESHOLD:
                logger.info(
                    f"📉 Cerrando posición {position['action']} para {symbol} porque RSI indica sobrecompra (RSI={signal['rsi']})")
                await self._close_position(symbol, position, db)
            elif position["action"] == "sell" and signal["rsi"] <= config.RSI_LOWER_THRESHOLD:
                logger.info(
                    f"📉 Cerrando posición {position['action']} para {symbol} porque RSI indica sobreventa (RSI={signal['rsi']})")
                await self._close_position(symbol, position, db)

        self.positions[symbol] = [pos for pos in self.positions[symbol] if pos["status"] == "open"]

        if len(open_positions) >= config.MAX_OPEN_ORDERS:
            logger.info(
                f"⏳ Máximo de posiciones abiertas ({config.MAX_OPEN_ORDERS}) alcanzado para {symbol}, omitiendo nueva orden")
            await self._save_results(symbol)
            return

        async with aiohttp.ClientSession() as session:
            qty_step = await get_quantity_precision(session, symbol)
        quantity = max(min((self.capital * config.RISK_FACTOR) / metrics["atr"], config.MAX_TRADE_SIZE),
                       config.MIN_TRADE_SIZE)
        quantity = round(quantity / qty_step) * qty_step

        required_capital = quantity * last_price
        if required_capital > self.capital:
            logger.warning(
                f"⚠️ Capital insuficiente para {symbol}: Requiere {required_capital:.2f} USDT, disponible {self.capital:.2f} USDT")
            await self._save_results(symbol)
            return

        take_profit, stop_loss = signal["take_profit"], signal["stop_loss"]

        if signal["action"].lower() == "buy":
            if stop_loss >= last_price:
                logger.error(
                    f"❌ Stop Loss inválido para BUY: SL={stop_loss} debe ser menor que el precio={last_price}")
                await self._save_results(symbol)
                return
            if take_profit <= last_price:
                logger.error(
                    f"❌ Take Profit inválido para BUY: TP={take_profit} debe ser mayor que el precio={last_price}")
                await self._save_results(symbol)
                return
        else:  # SELL
            if stop_loss <= last_price:
                logger.error(
                    f"❌ Stop Loss inválido para SELL: SL={stop_loss} debe ser mayor que el precio={last_price}")
                await self._save_results(symbol)
                return
            if take_profit >= last_price:
                logger.error(
                    f"❌ Take Profit inválido para SELL: TP={take_profit} debe ser menor que el precio={last_price}")
                await self._save_results(symbol)
                return

        order_key = (signal["action"], last_price, take_profit, stop_loss)
        last_failed = self.last_failed_order.get(symbol)
        time_since_last_failure = (
                current_time - self.last_failed_order_time[symbol]).total_seconds() if last_failed else float(
            'inf')

        if last_failed == order_key and time_since_last_failure < 300:
            logger.info(
                f"⏳ Orden repetitiva evitada para {symbol}: {signal['action']} {quantity} @ {last_price}, TP={take_profit}, SL={stop_loss}")
            await self._save_results(symbol)
            return
        elif last_failed and time_since_last_failure >= 300:
            logger.info(f"🔄 Reintentando orden previamente fallida para {symbol} tras {time_since_last_failure:.0f}s")
            self.last_failed_order[symbol] = None
            self.last_failed_order_time[symbol] = datetime.min.replace(tzinfo=timezone.utc)

        logger.info(
            f"📝 Preparando orden para {symbol}: {signal['action']} {quantity} @ {last_price}, TP={take_profit}, SL={stop_loss}")
        order_result = await self._place_order(symbol, signal["action"].lower(), quantity, last_price, take_profit,
                                               stop_loss)
        if order_result["success"]:
            trade = Trade(
                trade_id=self.trade_id_counter,
                timestamp=current_time,
                symbol=symbol,
                action=signal["action"].lower(),
                entry_price=last_price,
                quantity=quantity,
                decision=signal["action"].lower(),
                combined=float(metrics["combined"]) if hasattr(metrics['combined'], 'item') else float(
                    metrics['combined']),
                order_id=order_result["order_id"],
                tp=take_profit,
                sl=stop_loss,
                status="open",
                # Añadir el campo side (convertir a mayúsculas para seguir la convención)
                side=signal["action"].upper(),
                # Almacenar métricas calculadas
                ild=float(metrics["ild"]) if hasattr(metrics['ild'], 'item') else float(metrics['ild']),
                egm=float(metrics["egm"]) if hasattr(metrics['egm'], 'item') else float(metrics['egm']),
                pio=float(metrics["pio"]) if hasattr(metrics['pio'], 'item') else float(metrics['pio']),
                volatility=float(metrics["volatility"]) if hasattr(metrics['volatility'], 'item') else float(
                    metrics['volatility']),
                atr=float(metrics["atr"]) if hasattr(metrics['atr'], 'item') else float(metrics['atr']),
                rsi=float(signal["rsi"]) if hasattr(signal['rsi'], 'item') else float(signal['rsi'])
            )
            db.add(trade)
            # Crear y guardar objeto Position con position_id generado automáticamente
            position = Position(
                position_id=f"pos-{uuid.uuid4()}",  # Generar un ID único para la posición
                order_id=trade.order_id,
                symbol=symbol,
                side=PositionSide.LONG if trade.action == 'buy' else PositionSide.SHORT,  # Asignar el lado correcto
                action=trade.action,
                entry_price=float(trade.entry_price),
                quantity=float(trade.quantity),
                timestamp=trade.timestamp,
                tp=float(trade.tp) if trade.tp is not None else None,
                sl=float(trade.sl) if trade.sl is not None else None,
                status='open',
                last_update=datetime.now(timezone.utc),
                remaining_quantity=float(trade.quantity),
                filled_quantity=0.0,
                execution_status='pending',
                retry_count=0
            )
            db.add(position)
            db.commit()
            logger.info(f"✅ Posición guardada en la base de datos: {trade.order_id}")

            # Actualizar inmediatamente después de colocar la orden
            await self._update_pending_orders(db)

            self.positions[symbol].append({
                "trade_id": trade.trade_id,
                "timestamp": trade.timestamp.isoformat(),
                "symbol": symbol,
                "action": trade.action,
                "entry_price": trade.entry_price,
                "quantity": trade.quantity,
                "status": "open",
                "order_id": trade.order_id,
                "sl": trade.sl,
                "tp": trade.tp
            })
            self.last_trade_time[symbol] = current_time
            self.trade_id_counter += 1
            self.last_failed_order[symbol] = None
            self.last_failed_order_time[symbol] = datetime.min.replace(tzinfo=timezone.utc)
            logger.info(
                f"✅ Orden colocada para {symbol}: {trade.action} {trade.quantity} @ {trade.entry_price}, OrderID={trade.order_id}")
            asyncio.create_task(
                self._monitor_tp_sl(symbol, quantity, take_profit, stop_loss, signal["action"].lower(),
                                    trade.order_id))
            await self._save_results(symbol, trade)
        else:
            logger.error(f"❌ Fallo al colocar orden para {symbol}: {order_result['message']}")
            self.last_failed_order[symbol] = order_key
            self.last_failed_order_time[symbol] = current_time
            await self._save_results(symbol)

    async def _execute_trade_on_ticker(self, symbol: str, db: Session) -> None:
        if time.time() * 1000 - self.last_kline_time.get(symbol, 0) > 60000:
            logger.warning(f"⚠️ No se han recibido klines para {symbol} en 60 segundos, recargando datos...")
            await self.fetch_initial_data()
        await self._execute_trade(symbol, db)

    async def _place_order(self, symbol: str, action: str, quantity: float, price: float, tp: float, sl: float) -> Dict[
        str, Any]:
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        try:
            order_params = {
                "category": "linear",
                "symbol": symbol,
                "side": "Buy" if action == "buy" else "Sell",
                "orderType": "Market",
                "qty": f"{quantity:.3f}",
                "timeInForce": "GTC",
                "takeProfit": f"{tp:.2f}",
                "stopLoss": f"{sl:.2f}"
            }
            response = session.place_order(**{k: v for k, v in order_params.items() if v is not None})
            if response.get("retCode") == 0:
                logger.info(f"✅ Orden colocada exitosamente: {order_params}")
                return {"success": True, "order_id": response["result"]["orderId"], "message": "Orden exitosa"}
            logger.error(f"❌ Error al colocar orden: {response.get('retMsg', 'Error desconocido')}")
            return {"success": False, "order_id": None, "message": response.get("retMsg", "Error desconocido")}
        except Exception as e:
            logger.error(f"❌ Excepción al colocar orden: {str(e)}")
            return {"success": False, "order_id": None, "message": str(e)}

    async def _monitor_tp_sl(self, symbol: str, quantity: float, tp: float, sl: float, side: str, order_id: str):
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        with SessionLocal() as db:
            trade = db.query(Trade).filter_by(order_id=order_id).first()
            position_obj = db.query(Position).filter_by(order_id=order_id).first() if trade else None
            max_retries = 10  # Aumentar el número de reintentos para TP/SL
            retry_count = 0
            
            while trade and trade.status == "open" and retry_count < max_retries:
                try:
                    response = session.get_order_history(category="linear", symbol=symbol, orderId=order_id)
                    if response.get("retCode") == 0 and response["result"]["list"]:
                        order = response["result"]["list"][0]
                        order_status = order["orderStatus"]
                        
                        if order_status in ["Filled", "PartiallyFilled"]:
                            # Obtener el precio real de ejecución
                            execution_price = float(order["avgPrice"])
                            logger.info(f"✅ Orden TP/SL ejecutada a precio: {execution_price}")
                            
                            # Calcular P/L con el precio real de ejecución - corregido para manejar correctamente posiciones largas y cortas
                            if side == "buy":
                                # Para posiciones largas: (precio_salida - precio_entrada) * cantidad
                                profit_loss = (execution_price - trade.entry_price) * quantity
                            else:
                                # Para posiciones cortas: (precio_entrada - precio_salida) * cantidad
                                profit_loss = (trade.entry_price - execution_price) * quantity
                            
                            # Actualizar trade
                            trade.exit_price = execution_price
                            trade.profit_loss = profit_loss * (1 - config.FEE_RATE)
                            trade.status = "closed"
                            trade.updated_at = datetime.now(timezone.utc)
                            
                            # Actualizar position si existe
                            if position_obj:
                                position_obj.status = "closed"
                                position_obj.last_update = datetime.now(timezone.utc)
                                position_obj.filled_quantity = position_obj.quantity
                                position_obj.remaining_quantity = 0.0
                                position_obj.execution_status = "filled"
                                position_obj.close_time = datetime.now(timezone.utc)
                            
                            # Guardar cambios en la base de datos con manejo de errores
                            try:
                                db.commit()
                                logger.info(f"✅ Cambios guardados en la base de datos para la orden TP/SL {order_id}")
                                
                                # Actualizar el diccionario de posición en memoria
                                for pos in self.positions[symbol]:
                                    if pos["order_id"] == order_id:
                                        pos.update({
                                            "exit_price": execution_price, 
                                            "profit_loss": trade.profit_loss,
                                            "status": "closed"
                                        })
                                
                                # Actualizar capital
                                self.capital += trade.profit_loss
                                
                                # Registrar resultado en logs
                                result_type = "ganancia" if trade.profit_loss > 0 else "pérdida"
                                logger.info(
                                    f"✅ Posición cerrada por TP/SL para {symbol}: {side} @ {execution_price}, P/L={trade.profit_loss:.2f} ({result_type})")
                                
                                # Guardar resultados
                                await self._save_results(symbol, trade)
                            except Exception as commit_error:
                                logger.error(f"❌ Error al hacer commit para la orden TP/SL {order_id}: {commit_error}")
                                db.rollback()
                                # Intentar nuevamente con una nueva sesión
                                with SessionLocal() as new_db:
                                    try:
                                        # Volver a obtener los objetos con la nueva sesión
                                        new_trade = new_db.query(Trade).filter_by(order_id=order_id).first()
                                        new_position_obj = new_db.query(Position).filter_by(order_id=order_id).first()
                                        
                                        if new_trade and new_position_obj:
                                            # Actualizar nuevamente
                                            new_trade.status = "closed"
                                            new_trade.exit_price = execution_price
                                            new_trade.profit_loss = profit_loss * (1 - config.FEE_RATE)
                                            new_trade.updated_at = datetime.now(timezone.utc)
                                            
                                            new_position_obj.status = "closed"
                                            new_position_obj.last_update = datetime.now(timezone.utc)
                                            new_position_obj.filled_quantity = new_position_obj.quantity
                                            new_position_obj.remaining_quantity = 0.0
                                            new_position_obj.execution_status = "filled"
                                            new_position_obj.close_time = datetime.now(timezone.utc)
                                            
                                            new_db.commit()
                                            logger.info(f"✅ Cambios guardados en la base de datos con nueva sesión para la orden TP/SL {order_id}")
                                            
                                            # Actualizar el diccionario de posición en memoria con los valores de la nueva sesión
                                            for pos in self.positions[symbol]:
                                                if pos["order_id"] == order_id:
                                                    pos.update({
                                                        "exit_price": execution_price, 
                                                        "profit_loss": new_trade.profit_loss,
                                                        "status": "closed"
                                                    })
                                            
                                            # Actualizar capital
                                            self.capital += new_trade.profit_loss
                                            
                                            # Registrar resultado en logs
                                            result_type = "ganancia" if new_trade.profit_loss > 0 else "pérdida"
                                            logger.info(
                                                f"✅ Posición cerrada por TP/SL para {symbol}: {side} @ {execution_price}, P/L={new_trade.profit_loss:.2f} ({result_type})")
                                            
                                            # Guardar resultados
                                            await self._save_results(symbol, new_trade)
                                    except Exception as new_commit_error:
                                        logger.error(f"❌ Error al hacer commit con nueva sesión para la orden TP/SL {order_id}: {new_commit_error}")
                                        new_db.rollback()
                            return  # Salir del bucle después de procesar correctamente
                        elif order_status in ["Rejected", "Cancelled"]:
                            logger.warning(f"⚠️ Orden TP/SL {order_id} fue {order_status}")
                            return  # Salir si la orden fue rechazada o cancelada
                    
                    # Si llegamos aquí, la orden aún está pendiente o en proceso
                    retry_count += 1
                    logger.info(f"⏳ Monitoreando orden TP/SL {order_id}, estado: {order.get('orderStatus', 'desconocido')} (intento {retry_count}/{max_retries})")
                    
                except Exception as e:
                    error_msg = f"❌ Error al monitorear orden {order_id}: {e}"
                    logger.error(error_msg)
                    self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": error_msg})
                    retry_count += 1
                
                await asyncio.sleep(2)  # Aumentar el tiempo de espera entre verificaciones
            
            if retry_count >= max_retries:
                logger.warning(f"⚠️ Se alcanzó el máximo de reintentos ({max_retries}) al monitorear la orden TP/SL {order_id}")


    def _check_drawdown(self) -> bool:
        drawdown = (self.initial_capital - self.capital) / self.initial_capital
        if drawdown > config.MAX_DRAWDOWN:
            logger.critical(f"❌ Drawdown máximo alcanzado: {drawdown:.2%}")
            return False
        return True

    async def _save_results(self, symbol: str, trade_result: Optional[Trade] = None) -> None:
        unrealized_pnl = {}
        for sym in self.symbols:
            unrealized_pnl[sym] = 0.0
            for pos in self.positions[sym]:
                if pos["status"] == "open":
                    last_price = self.ticker_data[sym]["last_price"]
                    if pos["action"] == "buy":
                        unrealized_pnl[sym] += (last_price - pos["entry_price"]) * pos["quantity"]
                    else:
                        unrealized_pnl[sym] += (pos["entry_price"] - last_price) * pos["quantity"]

        results = {
            "metadata": {
                "session_start": self.session_start,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "capital_inicial": self.initial_capital,
                "capital_actual": self.capital,
                "total_trades": sum(1 for s in self.symbols for t in self.positions[s] if t["status"] == "closed"),
                "iterations": self.iterations,
                "running": self.running,
                "signal_counts": self.strategy.signal_counts,
                "errors": self.errors[-10:]
            },
            "summary": {
                "total_loss": sum(t["profit_loss"] for s in self.symbols for t in self.positions[s] if
                                  t["status"] == "closed" and t["profit_loss"] < 0),
                "total_profit": sum(t["profit_loss"] for s in self.symbols for t in self.positions[s] if
                                    t["status"] == "closed" and t["profit_loss"] > 0),
                "profit_per_trade": sum(
                    t["profit_loss"] for s in self.symbols for t in self.positions[s] if t["status"] == "closed") / (
                                            sum(1 for s in self.symbols for t in self.positions[s] if
                                                t["status"] == "closed") or 1)
            },
            "symbols": {
                sym: {
                    "profit": sum(t["profit_loss"] for t in self.positions[sym] if
                                  t["status"] == "closed" and t["profit_loss"] > 0),
                    "loss": sum(t["profit_loss"] for t in self.positions[sym] if
                                t["status"] == "closed" and t["profit_loss"] < 0),
                    "net_profit": sum(t["profit_loss"] for t in self.positions[sym] if t["status"] == "closed"),
                    "unrealized_pnl": unrealized_pnl[sym]
                } for sym in self.symbols
            },
            "trades": {sym: self.positions[sym] for sym in self.symbols}
        }
        if trade_result:
            results["last_trade"] = {
                "trade_id": trade_result.trade_id,
                "timestamp": trade_result.timestamp.isoformat(),
                "symbol": trade_result.symbol,
                "action": trade_result.action,
                "entry_price": float(trade_result.entry_price),
                "exit_price": float(trade_result.exit_price) if trade_result.exit_price else None,
                "quantity": float(trade_result.quantity),
                "profit_loss": float(trade_result.profit_loss) if trade_result.profit_loss else None,
                # Incluir métricas calculadas
                "metrics": {
                    "ild": float(trade_result.ild) if trade_result.ild is not None else None,
                    "egm": float(trade_result.egm) if trade_result.egm is not None else None,
                    "pio": float(trade_result.pio) if trade_result.pio is not None else None,
                    "volatility": float(trade_result.volatility) if trade_result.volatility is not None else None,
                    "atr": float(trade_result.atr) if trade_result.atr is not None else None,
                    "rsi": float(trade_result.rsi) if trade_result.rsi is not None else None,
                    "combined": float(trade_result.combined) if trade_result.combined is not None else None
                }
            }
        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, "session_results.json"), "w", encoding="utf-8") as file:
            json.dump(results, file, indent=4)
        logger.info(f"📄 Resultados guardados en: {log_dir}/session_results.json")

    async def _update_pending_orders(self, db: Session) -> None:
        """
        Actualiza el estado de las órdenes pendientes en la tabla positions usando la API de Bybit.
        """
        global now
        session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)

        # Obtener órdenes pendientes de la base de datos
        pending_positions = db.query(Position).filter_by(execution_status="pending").all()
        
        # Verificar y corregir posiciones sin position_id
        for pos in pending_positions:
            if not pos.position_id:
                pos.position_id = f"pos-{uuid.uuid4()}"
                logger.info(f"✅ Generado position_id para posición pendiente: {pos.order_id}")
                db.commit()
                
        # Actualizar la consulta después de las correcciones
        pending_positions = db.query(Position).filter_by(execution_status="pending").all()
        if not pending_positions:
            logger.info("✅ No hay órdenes pendientes para actualizar.")
            return

        for pos in pending_positions:
            try:
                # Consultar estado de la orden en Bybit usando order_id
                response = session.get_order_history(
                    category="linear",  # Cambia a "spot" si usas spot
                    orderId=pos.order_id
                )
                if response.get("retCode") == 0 and response["result"]["list"]:
                    order = response["result"]["list"][0]
                    status = order["orderStatus"]
                    now = datetime.now(timezone.utc)

                    # Actualizar según el estado de Bybit
                    if status == "Filled":
                        pos.execution_status = "filled"
                        pos.filled_quantity = float(order["cumExecQty"])
                        pos.remaining_quantity = float(order["qty"]) - float(order["cumExecQty"])
                        pos.last_update = now
                        logger.info(
                            f"✅ Orden {pos.order_id} actualizada a 'filled': {pos.filled_quantity}/{pos.quantity}")
                    elif status == "Cancelled" or status == "Rejected":
                        pos.execution_status = "cancelled"
                        pos.last_update = now
                        logger.info(f"✅ Orden {pos.order_id} actualizada a 'cancelled'")
                    elif status == "PartiallyFilled":
                        pos.execution_status = "partially_filled"
                        pos.filled_quantity = float(order["cumExecQty"])
                        pos.remaining_quantity = float(order["qty"]) - float(order["cumExecQty"])
                        pos.last_update = now
                        logger.info(
                            f"✅ Orden {pos.order_id} actualizada a 'partially_filled': {pos.filled_quantity}/{pos.quantity}")
                    else:
                        # Si sigue pendiente, solo actualiza retry_count si es necesario
                        pos.retry_count += 1
                        pos.last_update = now
                        if pos.retry_count > config.MAX_RETRIES:
                            pos.execution_status = "timeout"
                            logger.warning(f"⚠️ Orden {pos.order_id} agotó reintentos, marcada como 'timeout'")

                    db.commit()

                    # Si la orden se llenó completamente, sincronizar con posiciones abiertas
                    if pos.execution_status == "filled" and pos.filled_quantity == pos.quantity:
                        # Usar pos.symbol para obtener el símbolo correcto
                        symbol = pos.symbol
                        # Verificar si ya está en self.positions
                        if not any(p["order_id"] == pos.order_id for p in self.positions[symbol]):
                            self.positions[symbol].append({
                                "trade_id": f"pos-{pos.order_id}",
                                "timestamp": pos.timestamp.isoformat(),
                                "symbol": symbol,
                                "action": pos.action,
                                "entry_price": pos.entry_price,
                                "quantity": pos.quantity,
                                "status": "open",
                                "order_id": pos.order_id,
                                "sl": pos.sl,
                                "tp": pos.tp
                            })
                            logger.info(f"✅ Posición abierta sincronizada para {symbol}: {pos.action} {pos.quantity}")
                else:
                    logger.error(
                        f"❌ Error al consultar orden {pos.order_id}: {response.get('retMsg', 'Respuesta vacía')}")
                    pos.retry_count += 1
                    if pos.retry_count > config.MAX_RETRIES:
                        pos.execution_status = "timeout"
                        pos.last_update = now
                        logger.warning(f"⚠️ Orden {pos.order_id} agotó reintentos, marcada como 'timeout'")
                    db.commit()
            except Exception as e:
                logger.error(f"❌ Excepción al actualizar orden {pos.order_id}: {e}")
                pos.retry_count += 1
                if pos.retry_count > config.MAX_RETRIES:
                    pos.execution_status = "timeout"
                    pos.last_update = now
                    logger.warning(f"⚠️ Orden {pos.order_id} agotó reintentos, marcada como 'timeout'")
                db.commit()

    def reset_trades(self) -> None:
        self.positions = {symbol: [] for symbol in self.symbols}
        self.trade_id_counter = self._load_initial_trade_id()
        self.trade_buffer = {symbol: [] for symbol in self.symbols}
        self.last_trade_time = {symbol: datetime.min.replace(tzinfo=timezone.utc) for symbol in self.symbols}
        self.last_failed_order = {symbol: None for symbol in self.symbols}
        self.last_failed_order_time = {symbol: datetime.min.replace(tzinfo=timezone.utc) for symbol in self.symbols}
        with SessionLocal() as db:
            try:
                db.query(Trade).delete()
                db.commit()
                logger.info("🧹 Trades eliminados de la base de datos.")
            except Exception as e:
                logger.warning(f"⚠ No se pudo eliminar trades: {e}")
                db.rollback()
        logger.info("🧹 Estado de trades reseteado.")

    def stop(self) -> None:
        self.running = False
        if self.ws:
            asyncio.create_task(self.ws.close())


# endregion

# region FastAPI
app = FastAPI()
bot = NertzMetalEngine()


@app.get("/status")
async def get_status() -> Dict:
    return {"running": bot.running, "iterations": bot.iterations, "symbols": bot.symbols,
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/start")
async def start_bot() -> Dict:
    if not bot.running:
        bot.running = True
        asyncio.create_task(bot.start_async())
        return {"message": "✅ Bot iniciado", "timestamp": datetime.now(timezone.utc).isoformat()}
    return {"message": "⚠️ Bot ya está corriendo", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/stop")
async def stop_bot() -> Dict:
    if bot.running:
        bot.stop()
        return {"message": "🛑 Bot detenido", "timestamp": datetime.now(timezone.utc).isoformat()}
    return {"message": "⚠️ Bot ya está detenido", "timestamp": datetime.now(timezone.utc).isoformat()}


async def main():
    try:
        bot.reset_trades()
        logger.info("🚀 Iniciando bot y servidor API...")
        await asyncio.gather(bot.start_async(),
                             uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8087)).serve())
    except KeyboardInterrupt:
        logger.info("🛑 Bot detenido por el usuario (KeyboardInterrupt)")
        bot.stop()
        await bot._save_results("BTCUSDT")
    except Exception as e:
        logger.error(f"❌ Error inesperado en el bot: {e}")
        bot.stop()
        await bot._save_results("BTCUSDT")


if __name__ == "__main__":
    asyncio.run(main())
