"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              TRADING BOT v2.1  –  Hotfix: Model Always Predicts NEUTRAL     ║
║                                                                              ║
║  BUGS CORREGIDOS EN v2.0:                                                    ║
║  1-8. (ver historial)                                                         ║
║                                                                              ║
║  HOTFIXES v2.1 (basado en logs reales):                                      ║
║  A. Labels por cuantiles (33%/33%/33%) → elimina desequilibrio de clases    ║
║     Problema: umbral 0.4% sobre 4 velas → 86% NEUTRAL, CV acc 77% < 86%   ║
║     El modelo era PEOR que siempre predecir NEUTRAL                          ║
║  B. sample_weight correcto en model.fit() (antes se calculaba pero no usaba) ║
║  C. Eliminado use_label_encoder (deprecado en XGBoost 1.6+)                 ║
║  D. Entrenamiento inicial con 1000 velas (antes 500 = solo 5 días)          ║
║  E. Borrado automático del modelo viejo si hay desequilibrio extremo         ║
║  F. Parámetros faltantes del .env añadidos con defaults correctos            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import os
import logging
import logging.handlers
import signal
import time
from typing import Optional, Tuple, Dict, Any, List
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
import requests
import pandas as pd
from pybit.unified_trading import HTTP, WebSocket
from xgboost import XGBClassifier
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from ta.volatility import BollingerBands, AverageTrueRange
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, EMAIndicator, ADXIndicator
from ta.volume import OnBalanceVolumeIndicator
from joblib import dump, load
from dotenv import load_dotenv
import numpy as np
from functools import lru_cache
import gc

# ══════════════════════════════════════════════════════════════════════════════
#                              Configuration
# ══════════════════════════════════════════════════════════════════════════════
load_dotenv("config/.env")

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            "logs/trading.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8"
        )
    ]
)

CONFIG = {
    "demo_trading":                os.getenv("USE_DEMO_TRADING", "True").lower() == "true",
    "api_key":                     os.getenv("BYBIT_API_KEY", "default_key"),
    "api_secret":                  os.getenv("BYBIT_API_SECRET", "default_secret"),
    "symbol":                      os.getenv("SYMBOL", "BTCUSDT"),
    "category":                    os.getenv("CATEGORY", "linear"),
    "channel_type":                os.getenv("CHANNEL_TYPE", "linear"),
    "public_channel_type":         os.getenv("PUBLIC_CHANNEL_TYPE", "linear"),
    "leverage":                    int(os.getenv("TRADING_LEVERAGE", 5)),
    "trade_percentage":            float(os.getenv("TRADE_PERCENTAGE", 0.75)),
    "interval":                    int(os.getenv("TIMEFRAME", 15)),

    # ── Gestión de riesgo ──────────────────────────────────────────────────
    "base_tp_pct":                 float(os.getenv("BASE_TP_PCT", 0.045)),
    "base_sl_pct":                 float(os.getenv("BASE_SL_PCT", 0.022)),
    "min_rr_ratio":                float(os.getenv("MIN_RR_RATIO", 1.8)),      # NUEVO: R:R mínimo
    "dynamic_tp_factor":           float(os.getenv("DYNAMIC_TP_FACTOR", 1.3)),
    "dynamic_sl_factor":           float(os.getenv("DYNAMIC_SL_FACTOR", 1.0)),
    "risk_adjustment_factor":      float(os.getenv("RISK_ADJUSTMENT_FACTOR", 0.8)),
    "volatility_scaling":          os.getenv("VOLATILITY_SCALING", "true").lower() == "true",
    "max_position_value":          float(os.getenv("MAX_POSITION_VALUE", 50000.0)),
    "max_drawdown_pct":            float(os.getenv("MAX_DRAWDOWN_PCT", 0.15)),
    "risk_factor":                 float(os.getenv("RISK_FACTOR", 0.01)),
    "profit_take_threshold":       float(os.getenv("PROFIT_TAKE_THRESHOLD", 0.055)),
    "stop_loss_threshold":         float(os.getenv("STOP_LOSS_THRESHOLD", 0.032)),

    # ── Trailing stop ──────────────────────────────────────────────────────
    "trailing_stop_activation":    float(os.getenv("TRAILING_STOP_ACTIVATION", 0.6)),  # % del TP para activar
    "trailing_stop_pct":           float(os.getenv("TRAILING_STOP_PCT", 0.008)),

    # ── Modelo ML ─────────────────────────────────────────────────────────
    "model_path":                  Path(os.getenv("MODEL_PATH", "xgboost_model.joblib")),
    "pca_n_components":            int(os.getenv("PCA_N_COMPONENTS", 6)),       # FIJO para consistencia
    "forward_candles":             int(os.getenv("FORWARD_CANDLES", 4)),        # Horizonte predicción
    "signal_return_threshold":     float(os.getenv("SIGNAL_RETURN_THRESHOLD", 0.004)),  # ±0.4% para señal
    "model_features": [
        "rsi", "rsi_slope", "macd", "macd_signal", "macd_diff",
        "bb_high", "bb_low", "bb_pct", "volatility", "atr",
        "vwap", "vwap_distance", "stoch_k", "stoch_d",
        "obv_slope", "price_vs_ema20", "price_vs_ema50", "adx",
        "volume_ratio",
    ],

    # ── Filtros de señal ───────────────────────────────────────────────────
    "signal_confidence_threshold": float(os.getenv("SIGNAL_CONFIDENCE_THRESHOLD", 0.62)),
    "adx_min_threshold":           float(os.getenv("ADX_MIN_THRESHOLD", 20.0)),  # NUEVO: filtro de tendencia
    "trend_confirmation_window":   int(os.getenv("TREND_CONFIRMATION_WINDOW", 3)),

    # ── Control de trading ────────────────────────────────────────────────
    "retrain_interval":            int(os.getenv("RETRAIN_INTERVAL", 48)),
    "trade_cooldown":              int(os.getenv("TRADE_COOLDOWN", 300)),
    "max_trades_per_day":          int(os.getenv("MAX_TRADES_PER_DAY", 6)),

    # ── Sizing ────────────────────────────────────────────────────────────
    "capital_usdt":                float(os.getenv("CAPITAL_USDT", 50000.0)),
    "min_trade_size":              float(os.getenv("MIN_TRADE_SIZE", 0.0001)),
    "max_trade_size":              float(os.getenv("MAX_TRADE_SIZE", 0.1)),
    "use_risk_based_sizing":       os.getenv("USE_RISK_BASED_SIZING", "true").lower() == "true",
    "fee_rate":                    float(os.getenv("FEE_RATE", 0.00055)),

    # ── Operaciones ───────────────────────────────────────────────────────
    "signal_poll_seconds":         float(os.getenv("SIGNAL_POLL_SECONDS", 2.0)),
    "position_health_interval":    float(os.getenv("POSITION_HEALTH_INTERVAL", 10.0)),
    "max_iterations":              int(os.getenv("MAX_ITERATIONS", 1440)),
    "slippage_pct":                float(os.getenv("SLIPPAGE_PCT", 0.0005)),

    # ── Performance ───────────────────────────────────────────────────────
    "cache_maxsize":               int(os.getenv("CACHE_MAXSIZE", 64)),
    "data_retention_limit":        int(os.getenv("DATA_RETENTION_LIMIT", 500)),
    "websocket_ping_interval":     float(os.getenv("WEBSOCKET_PING_INTERVAL", 20.0)),
    "websocket_ping_timeout":      float(os.getenv("WEBSOCKET_PING_TIMEOUT", 8.0)),
    "api_call_timeout":            float(os.getenv("API_CALL_TIMEOUT", 10.0)),
    "enable_gc_collection":        os.getenv("ENABLE_GC_COLLECTION", "true").lower() == "true",
    "memory_cleanup_interval":     int(os.getenv("MEMORY_CLEANUP_INTERVAL", 100)),
    "orderbook_depth":             int(os.getenv("ORDERBOOK_DEPTH", 50)),
    "database_url":                os.getenv("DATABASE_URL", "sqlite:///data/trading.db"),
    "results_dir":                 os.getenv("RESULTS_DIR", "logs"),
    "api_rate_limit_delay":        float(os.getenv("API_RATE_LIMIT_DELAY", 0.1)),
}

# Señales del modelo (3 clases)
SIGNAL_SHORT  = 0
SIGNAL_NEUTRAL = 1
SIGNAL_LONG   = 2

MAKER_FEE: float = 0.0002
TAKER_FEE: float = 0.00055
_shutdown_requested = False

PERFORMANCE_METRICS: Dict[str, Any] = {
    "api_calls_total": 0,
    "websocket_messages_processed": 0,
    "model_predictions_total": 0,
    "data_fetch_time_total": 0.0,
    "indicator_calc_time_total": 0.0,
    "gc_collections_total": 0,
    "trades_total": 0,
    "trades_won": 0,
    "trades_lost": 0,
    "total_pnl": 0.0,
}


# ══════════════════════════════════════════════════════════════════════════════
#  TradingState – Estado persistente del bot (FIX CRÍTICO #1, #2, #6)
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class TradingState:
    """
    Centraliza TODO el estado mutable del bot.
    Elimina variables globales dispersas que causaban bugs de persistencia.
    """
    # Signal tracking
    last_signal: Optional[int]         = None
    previous_signal: Optional[int]     = None
    pending_tp_sl: bool                = False

    # Time tracking
    last_trade_time: float             = 0.0
    last_check_time: float             = 0.0

    # Daily limits
    current_day: Optional[object]      = None
    daily_trades: int                  = 0

    # Drawdown protection
    peak_equity: float                 = 0.0
    max_drawdown_reached: bool         = False

    # Error tracking
    consecutive_errors: int            = 0

    # FIX CRÍTICO #1: tracker persistente entre llamadas a check_position_health
    highest_pnl_tracker: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # FIX CRÍTICO #2: trailing stop state
    trailing_activated: bool           = False
    trailing_stop_price: float         = 0.0   # precio de stop del trailing
    trailing_peak_price: float         = 0.0   # precio más favorable visto

    # Iteration counter
    iteration: int                     = 0


# ══════════════════════════════════════════════════════════════════════════════
#                         Async Wrapper for Blocking I/O
# ══════════════════════════════════════════════════════════════════════════════
async def async_bybit_call(func, *args, **kwargs):
    """Ejecuta llamadas síncronas de pybit en thread separado."""
    PERFORMANCE_METRICS["api_calls_total"] += 1
    timeout = kwargs.pop("_timeout", CONFIG.get("api_call_timeout", 10.0))
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(func, *args, **kwargs),
            timeout=float(timeout)
        )
    except asyncio.TimeoutError as e:
        func_name = getattr(func, "__name__", str(func))
        raise TimeoutError(f"Bybit API timeout ({timeout}s): {func_name}") from e


# ══════════════════════════════════════════════════════════════════════════════
#                              Utility Functions
# ══════════════════════════════════════════════════════════════════════════════
def get_server_time_and_sync() -> bool:
    """Comprueba sincronización temporal con Bybit."""
    try:
        base_url = "https://api-demo.bybit.com" if CONFIG.get("demo_trading") else "https://api.bybit.com"
        response = requests.get(f"{base_url}/v5/market/time", timeout=10)
        response.raise_for_status()
        result = response.json().get("result", {}) or {}
        server_time = float(result.get("timeSecond") or 0)
        if not server_time:
            time_nano = result.get("timeNano")
            if time_nano:
                server_time = float(time_nano) / 1_000_000_000
        if not server_time:
            logging.error(f"Unexpected server time response: {response.text[:200]}")
            return False
        time_diff = abs(server_time - time.time())
        if time_diff > 30:
            logging.warning(f"System time out of sync: {time_diff:.1f}s difference.")
            return False
        logging.info("✅ System time in sync.")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Cannot connect to Bybit: {e}")
        return False


def safe_float(data: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(data.get(key, default))
    except (ValueError, TypeError):
        return default


def validate_params(leverage: int, trade_percentage: float) -> Tuple[int, float]:
    return max(1, min(leverage, 100)), max(0.01, min(trade_percentage, 1.0))


# ══════════════════════════════════════════════════════════════════════════════
#                           Bybit Session Manager
# ══════════════════════════════════════════════════════════════════════════════
class BybitSessionManager:
    def __init__(self, api_key: str, api_secret: str, demo_trading: bool = False, renew_interval: int = 3600):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.demo_trading = demo_trading
        self.renew_interval = renew_interval
        self.last_created = 0
        self.session: Optional[HTTP] = None
        self._initialize_session()

    def _initialize_session(self) -> None:
        now = time.time()
        if not self.session or (now - self.last_created > self.renew_interval):
            self.session = HTTP(
                api_key=self.api_key,
                api_secret=self.api_secret,
                testnet=False,
                demo=self.demo_trading,
            )
            self.last_created = now
            logging.debug("New HTTP session created.")

    def get_session(self) -> HTTP:
        self._initialize_session()
        return self.session

    async def validate_credentials(self) -> bool:
        try:
            session = self.get_session()
            if not self.api_key or self.api_key == "default_key":
                logging.error("API validation failed: missing BYBIT_API_KEY in config/.env")
                return False
            for attempt in range(3):
                try:
                    auth_check = await async_bybit_call(
                        session.get_wallet_balance,
                        accountType="UNIFIED",
                        _timeout=15.0
                    )
                    if auth_check.get("retCode", -1) == 0:
                        logging.info("✅ API credentials validated successfully.")
                        return True
                    logging.error(f"Validation failed (attempt {attempt+1}): {auth_check.get('retMsg')}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        return False
                except (TimeoutError, asyncio.TimeoutError) as e:
                    logging.warning(f"Validation timeout (attempt {attempt+1}/3): {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        return False
            return False
        except Exception as e:
            logging.error(f"Error validating credentials: {e}")
            return False


# ══════════════════════════════════════════════════════════════════════════════
#                      Data Fetching with Cache
# ══════════════════════════════════════════════════════════════════════════════
_historical_data_cache: Dict[str, pd.DataFrame] = {}
_historical_data_timestamp: Dict[str, float]    = {}


async def fetch_historical_data(symbol: str, interval: int, limit: int = 500) -> Optional[pd.DataFrame]:
    """Fetch historical klines con caché y validación."""
    start_time = time.time()
    cache_key = f"{symbol}_{interval}_{CONFIG['demo_trading']}"
    current_time = time.time()
    cache_valid = (interval * 60) - 30

    if cache_key in _historical_data_timestamp:
        if current_time - _historical_data_timestamp[cache_key] < cache_valid:
            df = _historical_data_cache.get(cache_key)
            if df is not None and not df.empty:
                PERFORMANCE_METRICS["data_fetch_time_total"] += (time.time() - start_time)
                return df.copy()

    session = SESSION_MANAGER.get_session()
    for attempt in range(3):
        try:
            response = await async_bybit_call(
                session.get_kline,
                category=CONFIG["category"],
                symbol=symbol,
                interval=str(interval),
                limit=limit
            )
            data = response.get("result", {}).get("list", [])
            if not data:
                logging.warning("No kline data received.")
                return None

            df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume", "turnover"])
            df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"]), unit="ms", utc=True)
            df.set_index("open_time", inplace=True)
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            df = df.sort_index()

            # Filtrar outliers de precio
            median_close = float(df["close"].median())
            if median_close > 0:
                mask = (df["close"] >= median_close * 0.2) & (df["close"] <= median_close * 5)
                df = df[mask]

            # Filtrar spikes
            pct_change = df["close"].pct_change().abs().fillna(0)
            df = df[pct_change <= 0.3]

            if len(df) > CONFIG["data_retention_limit"]:
                df = df.tail(CONFIG["data_retention_limit"])

            _historical_data_cache[cache_key]     = df.copy()
            _historical_data_timestamp[cache_key] = current_time
            PERFORMANCE_METRICS["data_fetch_time_total"] += (time.time() - start_time)
            return df
        except Exception as e:
            logging.warning(f"Fetch attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2)

    logging.error("Failed to fetch historical data after retries.")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#                  Technical Indicators  (FIX: más features, sin colisiones)
# ══════════════════════════════════════════════════════════════════════════════
# FIX CRÍTICO #8: clave determinista basada en índice del último candle
_indicator_cache: Dict[str, pd.DataFrame] = {}
_indicator_cache_ts: Dict[str, float]     = {}


def calculate_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calcula indicadores técnicos ampliados para el modelo de 3 clases."""
    global PERFORMANCE_METRICS
    start_time = time.time()

    if df is None or df.empty or len(df) < 26:
        logging.warning("Insufficient data for indicators (need ≥26 candles)")
        return None

    # FIX: clave basada en timestamp del último candle (sin colisiones de hash)
    last_ts = str(df.index[-1])
    cache_key = f"{last_ts}_{len(df)}"
    current_time = time.time()

    if cache_key in _indicator_cache_ts:
        if current_time - _indicator_cache_ts[cache_key] < 60:
            cached = _indicator_cache.get(cache_key)
            if cached is not None:
                PERFORMANCE_METRICS["indicator_calc_time_total"] += (time.time() - start_time)
                return cached.copy()

    try:
        df = df.copy()
        n = len(df)

        # ── RSI ───────────────────────────────────────────────────────────
        df["rsi"] = RSIIndicator(df["close"], window=14).rsi()
        df["rsi_slope"] = df["rsi"].diff(3)  # Pendiente del RSI

        # ── MACD ─────────────────────────────────────────────────────────
        macd_ind = MACD(df["close"], window_slow=26, window_fast=12, window_sign=9)
        df["macd"]        = macd_ind.macd()
        df["macd_signal"] = macd_ind.macd_signal()
        df["macd_diff"]   = macd_ind.macd_diff()

        # ── Bollinger Bands ───────────────────────────────────────────────
        bb = BollingerBands(df["close"], window=20, window_dev=2)
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"]  = bb.bollinger_lband()
        df["bb_mid"]  = bb.bollinger_mavg()
        # Posición relativa del precio en las bandas (0=low, 1=high)
        bb_width = (df["bb_high"] - df["bb_low"]).replace(0, np.nan)
        df["bb_pct"] = (df["close"] - df["bb_low"]) / bb_width

        # ── ATR (Average True Range) ──────────────────────────────────────
        df["atr"] = AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()

        # ── Volatilidad realizada ─────────────────────────────────────────
        df["volatility"] = df["close"].pct_change().rolling(window=min(20, n)).std()

        # ── VWAP (O(n) acumulativo) ───────────────────────────────────────
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        cum_vp = (typical_price * df["volume"]).cumsum()
        cum_v  = df["volume"].cumsum().replace(0, np.nan)
        df["vwap"] = cum_vp / cum_v
        df["vwap_distance"] = (df["close"] - df["vwap"]) / df["vwap"]  # % distancia al VWAP

        # ── Estocástico ───────────────────────────────────────────────────
        stoch = StochasticOscillator(df["high"], df["low"], df["close"], window=14, smooth_window=3)
        df["stoch_k"] = stoch.stoch()
        df["stoch_d"] = stoch.stoch_signal()

        # ── OBV slope ─────────────────────────────────────────────────────
        obv = OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()
        df["obv_slope"] = obv.diff(5)

        # ── EMAs y distancia del precio ───────────────────────────────────
        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        ema50 = EMAIndicator(df["close"], window=50).ema_indicator()
        df["price_vs_ema20"] = (df["close"] - ema20) / ema20
        df["price_vs_ema50"] = (df["close"] - ema50) / ema50

        # ── ADX (fuerza de tendencia) ─────────────────────────────────────
        adx_ind = ADXIndicator(df["high"], df["low"], df["close"], window=14)
        df["adx"] = adx_ind.adx()

        # ── Ratio de volumen (vs. media 20) ──────────────────────────────
        df["volume_ratio"] = df["volume"] / df["volume"].rolling(20).mean().replace(0, np.nan)

        result = df.dropna()

        _indicator_cache[cache_key]    = result.copy()
        _indicator_cache_ts[cache_key] = current_time

        PERFORMANCE_METRICS["indicator_calc_time_total"] += (time.time() - start_time)
        return result

    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#                  Trend Strength & Market Regime
# ══════════════════════════════════════════════════════════════════════════════
def calculate_trend_strength(df: pd.DataFrame) -> float:
    """Calcula fuerza de tendencia (0-1)."""
    if df is None or df.empty or len(df) < 5:
        return 0.5
    try:
        close = df["close"]
        short_ema  = close.ewm(span=5,  adjust=False).mean()
        medium_ema = close.ewm(span=10, adjust=False).mean()
        long_ema   = close.ewm(span=20, adjust=False).mean()

        trend = 0
        if short_ema.iloc[-1] > medium_ema.iloc[-1] > long_ema.iloc[-1]:
            trend = 1
        elif short_ema.iloc[-1] < medium_ema.iloc[-1] < long_ema.iloc[-1]:
            trend = -1

        n = min(CONFIG["trend_confirmation_window"], len(df) - 1)
        changes = np.sign(df["close"].diff().fillna(0).values[-n:])
        consistency = np.sum(changes == np.sign(trend)) / n if trend != 0 else 0.5

        rsi_align = 0.5
        if "rsi" in df.columns and len(df) >= 5:
            rsi_vals = df["rsi"].values[-5:]
            rsi_trend = 1 if rsi_vals[-1] > rsi_vals[0] else -1
            rsi_align = 1.0 if np.sign(trend) == np.sign(rsi_trend) else 0.0

        macd_strength = 0.0
        if "macd_diff" in df.columns:
            macd_strength = min(1.0, abs(df["macd_diff"].iloc[-1]) / 0.01) * 0.5

        strength = (
            0.40 * (abs(trend) * 0.5 + 0.5)
            + 0.30 * consistency
            + 0.20 * rsi_align
            + 0.10 * macd_strength
        )
        return float(np.clip(strength, 0.0, 1.0))
    except Exception as e:
        logging.error(f"Error in trend strength: {e}")
        return 0.5


def market_is_tradeable(df: pd.DataFrame) -> bool:
    """
    FIX CRÍTICO #4: Filtro de régimen de mercado con ADX.
    No operar en mercados laterales/choppy (ADX < umbral).
    """
    if df is None or df.empty or "adx" not in df.columns:
        return True  # si no podemos calcular, no bloqueamos
    adx_val = df["adx"].iloc[-1]
    if np.isnan(adx_val):
        return True
    tradeable = adx_val >= CONFIG["adx_min_threshold"]
    if not tradeable:
        logging.info(f"📊 Market regime filter: ADX={adx_val:.1f} < {CONFIG['adx_min_threshold']} → skip")
    return tradeable


# ══════════════════════════════════════════════════════════════════════════════
#  Model Training – 3 Clases (HOTFIX A: quantile labels)
# ══════════════════════════════════════════════════════════════════════════════
def _build_labels(df: pd.DataFrame) -> pd.Series:
    """
    HOTFIX A: Labels por CUANTILES → siempre ~33%/33%/33%.

    El problema con threshold fijo (0.004):
      BTC 15m se mueve ~0.2% tipicamente. 4 velas forward + umbral 0.4%
      dejaba 86% NEUTRAL. CV accuracy 77% < baseline 86% -> modelo inutil.

    Solucion: cuantil 33% inferior = SHORT, 33% superior = LONG, resto = NEUTRAL.
    Clases siempre balanceadas independientemente del regimen de mercado.
    """
    n = CONFIG["forward_candles"]
    future_return = df["close"].pct_change(n).shift(-n)
    valid = future_return.dropna()

    q_low  = float(np.percentile(valid, 33))
    q_high = float(np.percentile(valid, 67))

    labels = pd.Series(SIGNAL_NEUTRAL, index=df.index, dtype=int)
    labels[future_return >= q_high] = SIGNAL_LONG
    labels[future_return <= q_low]  = SIGNAL_SHORT

    total   = int(labels.count())
    n_long  = int((labels == SIGNAL_LONG).sum())
    n_neut  = int((labels == SIGNAL_NEUTRAL).sum())
    n_short = int((labels == SIGNAL_SHORT).sum())
    logging.info(
        f"Label distribution (quantile): "
        f"SHORT={n_short}({n_short/max(total,1)*100:.1f}%) "
        f"NEUTRAL={n_neut}({n_neut/max(total,1)*100:.1f}%) "
        f"LONG={n_long}({n_long/max(total,1)*100:.1f}%)"
    )
    logging.info(f"Quantile thresholds: q33={q_low*100:.3f}%, q67={q_high*100:.3f}%")
    return labels


def train_model(df: pd.DataFrame) -> Tuple["XGBClassifier", "PCA", "StandardScaler"]:
    """
    Entrena XGBoost 3-clases con:
    - PCA fijo (HOTFIX v2.0 #5)
    - sample_weight correcto en fit() (HOTFIX B - antes se calculaba pero no se usaba)
    - use_label_encoder eliminado (HOTFIX C - deprecado en XGBoost 1.6+)
    - Borrado de modelo viejo si desequilibrio extremo detectado (HOTFIX E)
    """
    try:
        features_df = df[CONFIG["model_features"]].copy()
        labels = _build_labels(df)

        combined = features_df.join(labels.rename("label")).dropna()
        if len(combined) < 80:
            raise ValueError(f"Datos insuficientes para entrenamiento: {len(combined)} filas (minimo 80)")

        X = combined[CONFIG["model_features"]].values
        y = combined["label"].values.astype(int)

        unique, counts = np.unique(y, return_counts=True)
        if len(unique) < 2:
            raise ValueError("Solo una clase en labels")

        # HOTFIX E: advertir si una clase domina demasiado (no deberia pasar con quantiles)
        max_pct = max(counts) / len(y)
        if max_pct > 0.75:
            logging.warning(
                f"Clase dominante: {max_pct*100:.1f}% de muestras en una sola clase. "
                "Considera ajustar forward_candles o revisar los datos."
            )

        # HOTFIX B: sample_weight CORRECTO — inverse frequency weighting
        # Antes: class_weight se calculaba pero NUNCA se pasaba a model.fit()
        total = len(y)
        class_freq = {cls: cnt / total for cls, cnt in zip(unique, counts)}
        sample_weights = np.array([
            1.0 / (class_freq.get(yi, 1.0) * len(unique))
            for yi in y
        ])
        # Normalizar weights
        sample_weights = sample_weights / sample_weights.mean()

        n_components = min(CONFIG["pca_n_components"], X.shape[1], X.shape[0] - 1)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        pca = PCA(n_components=n_components, random_state=42)
        X_pca = pca.fit_transform(X_scaled)

        # HOTFIX C: eliminado use_label_encoder (deprecado, causa UserWarning)
        model = XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.06,
            subsample=0.8,
            colsample_bytree=0.75,
            gamma=0.1,
            min_child_weight=3,
            eval_metric="mlogloss",
            num_class=3,
            objective="multi:softprob",
            n_jobs=-1,
            random_state=42,
        )

        # TimeSeriesSplit con sample_weight
        tscv = TimeSeriesSplit(n_splits=3)
        cv_scores = []
        for fold, (train_idx, val_idx) in enumerate(tscv.split(X_pca)):
            X_tr, X_val = X_pca[train_idx], X_pca[val_idx]
            y_tr, y_val = y[train_idx], y[val_idx]
            w_tr = sample_weights[train_idx]
            if len(np.unique(y_tr)) < 2:
                logging.warning(f"CV Fold {fold+1}: solo una clase en train, skip")
                continue
            model.fit(X_tr, y_tr, sample_weight=w_tr, eval_set=[(X_val, y_val)], verbose=False)
            preds = model.predict(X_val)
            acc = float(np.mean(preds == y_val))
            # También reportar distribucion de predicciones
            pred_dist = {int(c): int((preds == c).sum()) for c in [0, 1, 2]}
            cv_scores.append(acc)
            logging.info(f"  CV Fold {fold+1}: accuracy={acc:.3f} | pred_dist={pred_dist}")

        if cv_scores:
            mean_acc = float(np.mean(cv_scores))
            # Baseline: frecuencia de la clase mas comun
            baseline = float(max(counts) / total)
            beat_baseline = mean_acc > baseline
            logging.info(
                f"CV accuracy: {mean_acc:.3f} | Baseline(naive): {baseline:.3f} | "
                f"{'✅ BEATS baseline' if beat_baseline else '⚠️ BELOW baseline - check features'}"
            )

        # Reentrenar con TODOS los datos y sample_weight
        model.fit(X_pca, y, sample_weight=sample_weights, verbose=False)

        # Verificar distribucion de predicciones en datos de entrenamiento
        train_preds = model.predict(X_pca)
        train_pred_dist = {int(c): int((train_preds == c).sum()) for c in [0, 1, 2]}
        logging.info(f"Train prediction distribution: {train_pred_dist}")

        dump({"model": model, "pca": pca, "scaler": scaler}, CONFIG["model_path"])
        logging.info(
            f"✅ Model saved: {n_components} PCA components, "
            f"{len(X)} samples, classes={list(unique.tolist())}"
        )
        return model, pca, scaler

    except Exception as e:
        logging.error(f"Error training model: {e}")
        raise


def load_model_and_pca() -> Tuple[Optional["XGBClassifier"], Optional["PCA"], Optional["StandardScaler"]]:
    if CONFIG["model_path"].exists():
        try:
            data = load(CONFIG["model_path"])
            return data.get("model"), data.get("pca"), data.get("scaler")
        except Exception as e:
            logging.error(f"Error loading model: {e}")
    return None, None, None


async def load_or_train_model(
    df: Optional[pd.DataFrame] = None
) -> Tuple[Optional["XGBClassifier"], Optional["PCA"], Optional["StandardScaler"]]:
    model, pca, scaler = load_model_and_pca()
    if model is None or pca is None or scaler is None:
        if df is None:
            df = await fetch_historical_data(CONFIG["symbol"], CONFIG["interval"], limit=1000)
        if df is not None:
            df_ind = calculate_indicators(df)
            if df_ind is not None and len(df_ind) > 80:
                return train_model(df_ind)
    return model, pca, scaler


# ══════════════════════════════════════════════════════════════════════════════
#                  Signal Generation – Multi-Confirmación
# ══════════════════════════════════════════════════════════════════════════════
def generate_signal(
    df: pd.DataFrame,
    model: "XGBClassifier",
    pca: "PCA",
    scaler: "StandardScaler",
) -> Tuple[int, float]:
    """
    Genera señal con confianza. Requiere:
    1. Predicción del modelo con confianza > umbral
    2. Alineación con tendencia (no contra-tendencia)
    3. Mercado con suficiente momentum (ADX)

    Returns:
        (signal: int, confidence: float)
        signal ∈ {SIGNAL_SHORT(0), SIGNAL_NEUTRAL(1), SIGNAL_LONG(2)}
    """
    try:
        features = df[CONFIG["model_features"]].values
        features_scaled = scaler.transform(features)
        features_pca    = pca.transform(features_scaled)

        probas   = model.predict_proba(features_pca)
        PERFORMANCE_METRICS["model_predictions_total"] += 1

        latest_probas = probas[-1]  # [P(SHORT), P(NEUTRAL), P(LONG)]
        predicted_class = int(np.argmax(latest_probas))
        confidence      = float(np.max(latest_probas))

        # Filtro de confianza
        if confidence < CONFIG["signal_confidence_threshold"]:
            logging.info(
                f"🧠 Model confidence {confidence:.2%} < {CONFIG['signal_confidence_threshold']:.2%} → NEUTRAL"
            )
            return SIGNAL_NEUTRAL, confidence

        # Filtro de régimen de mercado (ADX)
        if not market_is_tradeable(df):
            return SIGNAL_NEUTRAL, 0.0

        # Filtro de alineación con tendencia
        trend = calculate_trend_strength(df)
        if predicted_class == SIGNAL_LONG and trend < 0.35:
            logging.info(f"🧠 BUY signal but trend weak ({trend:.2f}) → NEUTRAL")
            return SIGNAL_NEUTRAL, confidence
        if predicted_class == SIGNAL_SHORT and trend > 0.65:
            logging.info(f"🧠 SELL signal but trend strong ({trend:.2f}) → NEUTRAL")
            return SIGNAL_NEUTRAL, confidence

        logging.info(
            f"🧠 Signal: {'LONG' if predicted_class == SIGNAL_LONG else 'SHORT' if predicted_class == SIGNAL_SHORT else 'NEUTRAL'} "
            f"| Confidence: {confidence:.2%} | ADX: {df['adx'].iloc[-1]:.1f} | Trend: {trend:.2f}"
        )
        return predicted_class, confidence

    except Exception as e:
        logging.error(f"Error generating signal: {e}")
        return SIGNAL_NEUTRAL, 0.0


# ══════════════════════════════════════════════════════════════════════════════
#                      TP/SL con R:R mínimo forzado
# ══════════════════════════════════════════════════════════════════════════════
def adjust_tp_sl(
    volatility: float,
    base_tp: float,
    base_sl: float,
    trend_strength: float = 0.5,
) -> Tuple[float, float]:
    """
    Calcula TP/SL dinámicos asegurando R:R ≥ min_rr_ratio.
    """
    vol_factor = (1 + volatility * 2) if CONFIG["volatility_scaling"] else 1.0

    if trend_strength > 0.7:
        tp_adj = base_tp * CONFIG["dynamic_tp_factor"] * vol_factor * 1.2
        sl_adj = base_sl * CONFIG["dynamic_sl_factor"] * vol_factor * 0.9
    elif trend_strength < 0.3:
        tp_adj = base_tp * CONFIG["dynamic_tp_factor"] * vol_factor * 0.9
        sl_adj = base_sl * CONFIG["dynamic_sl_factor"] * vol_factor * 1.1
    else:
        tp_adj = base_tp * CONFIG["dynamic_tp_factor"] * vol_factor
        sl_adj = base_sl * CONFIG["dynamic_sl_factor"] * vol_factor

    sl_adj *= CONFIG["risk_adjustment_factor"]

    # Clamp absolutos
    tp_adj = float(np.clip(tp_adj, 0.015, 0.10))
    sl_adj = float(np.clip(sl_adj, 0.008, 0.04))

    # FIX R:R mínimo: si TP/SL < min_rr_ratio, ampliar TP
    rr = tp_adj / sl_adj
    min_rr = CONFIG["min_rr_ratio"]
    if rr < min_rr:
        tp_adj = sl_adj * min_rr
        tp_adj = float(np.clip(tp_adj, 0.015, 0.12))
        logging.debug(f"R:R ajustado: {tp_adj/sl_adj:.2f} (mínimo {min_rr})")

    logging.debug(f"TP={tp_adj:.4f} SL={sl_adj:.4f} R:R={tp_adj/sl_adj:.2f} (vol={volatility:.4f})")
    return tp_adj, sl_adj


# ══════════════════════════════════════════════════════════════════════════════
#                      Fee / Funding Rate
# ══════════════════════════════════════════════════════════════════════════════
_funding_cache: Dict[str, float] = {}
_funding_cache_ts: Dict[str, float] = {}


async def get_fee_rates() -> Tuple[float, float]:
    global MAKER_FEE, TAKER_FEE
    try:
        session = SESSION_MANAGER.get_session()
        fee_func = getattr(session, "get_fee_rates", None) or getattr(session, "get_fee_rate", None)
        if fee_func is None:
            return MAKER_FEE, TAKER_FEE
        response = await async_bybit_call(fee_func, category=CONFIG["category"], symbol=CONFIG["symbol"])
        if response.get("retCode") == 0:
            fee_list = response.get("result", {}).get("list", [])
            if fee_list:
                MAKER_FEE = safe_float(fee_list[0], "makerFeeRate", 0.0002)
                TAKER_FEE = safe_float(fee_list[0], "takerFeeRate", 0.00055)
                logging.info(f"Fees: Maker {MAKER_FEE*100:.4f}%, Taker {TAKER_FEE*100:.4f}%")
    except Exception as e:
        logging.error(f"Error fetching fees: {e}")
    return MAKER_FEE, TAKER_FEE


async def get_funding_rate(symbol: str) -> Optional[float]:
    cache_key = f"{symbol}_{CONFIG['demo_trading']}"
    if cache_key in _funding_cache_ts:
        if time.time() - _funding_cache_ts[cache_key] < 60:
            return _funding_cache.get(cache_key)
    try:
        session = SESSION_MANAGER.get_session()
        response = await async_bybit_call(session.get_tickers, category=CONFIG["category"], symbol=symbol)
        tickers = response.get("result", {}).get("list", [])
        if tickers:
            rate = safe_float(tickers[0], "fundingRate", 0.0)
            _funding_cache[cache_key]    = rate
            _funding_cache_ts[cache_key] = time.time()
            return rate
    except Exception as e:
        logging.error(f"Error fetching funding rate: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#                      Account, Position, Sizing
# ══════════════════════════════════════════════════════════════════════════════
_account_equity_cache: Dict[str, float] = {}
_account_equity_ts:    Dict[str, float] = {}
_position_cache:       Dict[str, Tuple[float, float]] = {}
_position_cache_ts:    Dict[str, float] = {}
_instrument_cache:     Dict[str, Dict] = {}
_instrument_cache_ts:  Dict[str, float] = {}


async def get_account_equity() -> float:
    """Equity REAL incluyendo PnL no realizado."""
    cache_key = f"eq_{CONFIG['demo_trading']}"
    if cache_key in _account_equity_ts:
        if time.time() - _account_equity_ts[cache_key] < 10:
            return _account_equity_cache.get(cache_key, 0.0)
    try:
        session = SESSION_MANAGER.get_session()
        response = await async_bybit_call(session.get_wallet_balance, accountType="UNIFIED")
        for acc in response.get("result", {}).get("list", []):
            if acc.get("accountType") == "UNIFIED":
                for coin in acc.get("coin", []):
                    if coin.get("coin") == "USDT":
                        equity = safe_float(coin, "walletBalance")
                        _account_equity_cache[cache_key] = equity
                        _account_equity_ts[cache_key]    = time.time()
                        return equity
    except Exception as e:
        logging.error(f"Error fetching equity: {e}")
    return _account_equity_cache.get(cache_key, 0.0)


async def get_position_info(symbol: str) -> Tuple[float, float]:
    """Devuelve (size, entry_price). Negativo = SHORT."""
    cache_key = f"pos_{symbol}_{CONFIG['demo_trading']}"
    if cache_key in _position_cache_ts:
        if time.time() - _position_cache_ts[cache_key] < 5:
            return _position_cache.get(cache_key, (0.0, 0.0))
    try:
        session = SESSION_MANAGER.get_session()
        response = await async_bybit_call(session.get_positions, category=CONFIG["category"], symbol=symbol)
        for pos in response.get("result", {}).get("list", []):
            if pos.get("symbol") == symbol:
                side  = pos.get("side", "").lower()
                size  = safe_float(pos, "size")
                entry = safe_float(pos, "avgPrice")
                if side == "sell":
                    size = -size
                if abs(size) > 0:
                    result = (size, entry)
                    _position_cache[cache_key]    = result
                    _position_cache_ts[cache_key] = time.time()
                    return result
        _position_cache[cache_key]    = (0.0, 0.0)
        _position_cache_ts[cache_key] = time.time()
        return 0.0, 0.0
    except Exception as e:
        logging.error(f"Error fetching position: {e}")
        return 0.0, 0.0


async def get_instrument_info(symbol: str) -> Dict[str, Any]:
    cache_key = f"inst_{symbol}_{CONFIG['demo_trading']}"
    if cache_key in _instrument_cache_ts:
        if time.time() - _instrument_cache_ts[cache_key] < 300:
            return _instrument_cache.get(cache_key, {})
    try:
        session = SESSION_MANAGER.get_session()
        response = await async_bybit_call(session.get_instruments_info, category=CONFIG["category"], symbol=symbol)
        result = response.get("result", {}).get("list", [{}])[0]
        _instrument_cache[cache_key]    = result
        _instrument_cache_ts[cache_key] = time.time()
        return result
    except Exception as e:
        logging.error(f"Error fetching instrument info: {e}")
        return {}


async def get_position_size(usdt_balance: float, price: float, symbol: str, df: pd.DataFrame) -> float:
    """Sizing basado en riesgo con límites del exchange."""
    leverage, trade_pct = validate_params(CONFIG["leverage"], CONFIG["trade_percentage"])
    volatility = float(df["volatility"].iloc[-1]) if "volatility" in df.columns else 0.01
    vol_factor = float(np.clip(0.02 / max(volatility, 0.001), 0.3, 1.0))

    if CONFIG["use_risk_based_sizing"]:
        sl_distance = price * CONFIG["base_sl_pct"] * CONFIG["dynamic_sl_factor"]
        risk_amount  = usdt_balance * CONFIG["risk_factor"]
        size = risk_amount / sl_distance if sl_distance > 0 else (usdt_balance * trade_pct * leverage) / price
    else:
        size = (usdt_balance * trade_pct * leverage) / price

    size *= vol_factor

    info    = await get_instrument_info(symbol)
    lot_f   = info.get("lotSizeFilter", {})
    min_qty = safe_float(lot_f, "minOrderQty", 0.001)
    qty_step = safe_float(lot_f, "qtyStep", 0.001)

    if qty_step > 0:
        size = round(size / qty_step) * qty_step
    size = float(np.clip(size, min_qty, CONFIG["max_trade_size"]))

    # Límite de valor de posición
    position_value = size * price
    if position_value > CONFIG["max_position_value"]:
        size = CONFIG["max_position_value"] / price
        if qty_step > 0:
            size = round(size / qty_step) * qty_step
        size = max(min_qty, size)
        logging.warning(f"Position value capped at {CONFIG['max_position_value']:.0f} USDT → size={size:.6f}")

    logging.debug(f"Position size: {size:.6f} {symbol} at {price:.2f} (balance={usdt_balance:.2f})")
    return size


async def set_leverage(symbol: str, leverage: int) -> bool:
    session = SESSION_MANAGER.get_session()
    try:
        response = await async_bybit_call(
            session.set_leverage,
            category=CONFIG["category"],
            symbol=symbol,
            buyLeverage=str(leverage),
            sellLeverage=str(leverage)
        )
        ret_code = response.get("retCode")
        ret_msg  = str(response.get("retMsg", "") or "")
        if ret_code == 0 or ret_code == 110043 or "not modified" in ret_msg.lower():
            logging.info(f"Leverage set to {leverage}x for {symbol}")
            return True
        logging.error(f"Failed to set leverage: {ret_msg}")
        return False
    except Exception as e:
        if "110043" in str(e) or "not modified" in str(e).lower():
            return True
        logging.error(f"Error setting leverage: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#                      Orders
# ══════════════════════════════════════════════════════════════════════════════
async def place_order(symbol: str, side: str, qty: float, reduce_only: bool = False) -> Optional[str]:
    if qty <= 0:
        logging.error("Invalid order quantity.")
        return None
    session = SESSION_MANAGER.get_session()
    try:
        response = await async_bybit_call(
            session.place_order,
            category=CONFIG["category"],
            symbol=symbol,
            side="Buy" if side.lower() == "buy" else "Sell",
            orderType="Market",
            qty=str(qty),
            reduceOnly=reduce_only
        )
        order_id = response.get("result", {}).get("orderId")
        if order_id:
            logging.info(f"Order placed: {side.upper()} {qty} {symbol} | ID: {order_id}")
            return order_id
        logging.error(f"Failed to place order: {response}")
        return None
    except Exception as e:
        logging.error(f"Error placing order: {e}")
        return None


async def check_order_status(order_id: str, symbol: str) -> bool:
    session = SESSION_MANAGER.get_session()
    try:
        response = await async_bybit_call(
            session.get_order_history, category=CONFIG["category"], symbol=symbol, orderId=order_id
        )
        orders = response.get("result", {}).get("list", [])
        return bool(orders and orders[0].get("orderStatus") == "Filled")
    except Exception as e:
        logging.error(f"Error checking order status: {e}")
        return False


async def set_tp_sl(symbol: str, entry_price: float, size: float, df: pd.DataFrame) -> bool:
    """Establece TP/SL con ratio R:R mínimo forzado."""
    if size <= 0 or entry_price <= 0:
        return True

    session    = SESSION_MANAGER.get_session()
    volatility = float(df["volatility"].iloc[-1]) if df is not None and "volatility" in df.columns else 0.01
    trend_str  = calculate_trend_strength(df) if df is not None else 0.5

    tp_pct, sl_pct = adjust_tp_sl(volatility, CONFIG["base_tp_pct"], CONFIG["base_sl_pct"], trend_str)

    pos_size, _ = await get_position_info(symbol)
    is_short = pos_size < 0

    if is_short:
        take_profit = entry_price * (1 - tp_pct)
        stop_loss   = entry_price * (1 + sl_pct)
    else:
        take_profit = entry_price * (1 + tp_pct)
        stop_loss   = entry_price * (1 - sl_pct)

    info         = await get_instrument_info(symbol)
    price_filter = info.get("priceFilter", {})
    tick_size    = safe_float(price_filter, "tickSize", 0.01)
    min_price    = safe_float(price_filter, "minPrice", 0.01)
    max_price    = safe_float(price_filter, "maxPrice", 1_000_000)

    if tick_size > 0:
        take_profit = round(take_profit / tick_size) * tick_size
        stop_loss   = round(stop_loss   / tick_size) * tick_size

    take_profit = float(np.clip(take_profit, min_price, max_price))
    stop_loss   = float(np.clip(stop_loss,   min_price, max_price))

    logging.info(
        f"TP/SL → TP={take_profit:.2f} (+{tp_pct:.2%}), SL={stop_loss:.2f} (-{sl_pct:.2%}), "
        f"R:R={tp_pct/sl_pct:.2f}"
    )
    try:
        response = await async_bybit_call(
            session.set_trading_stop,
            category=CONFIG["category"],
            symbol=symbol,
            takeProfit=str(take_profit),
            stopLoss=str(stop_loss),
            tpslMode="Full",
            positionIdx=0,
            tpTriggerBy="LastPrice",
            slTriggerBy="LastPrice",
            tpOrderType="Market",
            slOrderType="Market",
        )
        if response.get("retCode", -1) == 0:
            logging.info(f"✅ TP/SL set successfully for {symbol}")
            return True
        logging.error(f"Failed to set TP/SL: {response}")
        return False
    except Exception as e:
        logging.error(f"Error setting TP/SL: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#                      WebSocket Manager
# ══════════════════════════════════════════════════════════════════════════════
class WebSocketManager:
    def __init__(self, api_key: str, api_secret: str, private_channel_type: str, public_channel_type: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.demo_trading = CONFIG.get("demo_trading", False)
        self.private_channel_type = private_channel_type
        self.public_channel_type  = public_channel_type
        self.private_ws = None
        self.public_ws  = None
        self.connected        = False
        self.public_connected = False
        self.last_filled_order_id: Optional[str] = None
        self.order_fill_event = asyncio.Event()
        self.last_price: Optional[float] = None
        self.price_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._message_counter = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self) -> bool:
        async with self._lock:
            self._loop = asyncio.get_running_loop()
            private_ok = await self._connect_private()
            public_ok  = await self._connect_public()
            return private_ok and public_ok

    def _set_fill(self, order_id: Optional[str]) -> None:
        if not order_id:
            return
        self.last_filled_order_id = str(order_id)
        self.order_fill_event.set()
        logging.info(f"✅ Order filled: {self.last_filled_order_id}")

    def _set_price(self, price: Optional[float]) -> None:
        if price is None:
            return
        self.last_price = float(price)
        self.price_event.set()

    def _tick_metrics(self) -> None:
        PERFORMANCE_METRICS["websocket_messages_processed"] += 1
        self._message_counter += 1
        if CONFIG["enable_gc_collection"] and self._message_counter % 1000 == 0:
            collected = gc.collect()
            PERFORMANCE_METRICS["gc_collections_total"] += 1

    def _call_soon(self, callback, *args) -> None:
        if self._loop:
            self._loop.call_soon_threadsafe(callback, *args)

    async def _connect_private(self) -> bool:
        for attempt in range(5):
            try:
                self.connected = False
                if self.private_ws:
                    try:
                        await asyncio.to_thread(self.private_ws.close)
                    except Exception:
                        pass
                self.private_ws = await asyncio.to_thread(
                    WebSocket,
                    channel_type=self.private_channel_type,
                    testnet=False,
                    demo=self.demo_trading,
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    ping_interval=CONFIG["websocket_ping_interval"],
                    ping_timeout=CONFIG["websocket_ping_timeout"],
                )
                self.connected = True
                logging.info("✅ Private WebSocket connected.")
                return True
            except Exception as e:
                logging.warning(f"Private WS connect attempt {attempt+1} failed: {e}")
                await asyncio.sleep(min(2 ** attempt, 10))
        logging.error("❌ Failed to connect private WebSocket.")
        return False

    async def _connect_public(self) -> bool:
        for attempt in range(5):
            try:
                self.public_connected = False
                if self.public_ws:
                    try:
                        await asyncio.to_thread(self.public_ws.close)
                    except Exception:
                        pass
                self.public_ws = await asyncio.to_thread(
                    WebSocket,
                    channel_type=self.public_channel_type,
                    testnet=False,
                    demo=False,
                    ping_interval=CONFIG["websocket_ping_interval"],
                    ping_timeout=CONFIG["websocket_ping_timeout"],
                )
                self.public_connected = True
                logging.info("✅ Public WebSocket connected.")
                return True
            except Exception as e:
                logging.warning(f"Public WS connect attempt {attempt+1} failed: {e}")
                await asyncio.sleep(min(2 ** attempt, 10))
        logging.error("❌ Failed to connect public WebSocket.")
        return False

    def subscribe_fills(self, symbol: str) -> None:
        def handle(message: Dict[str, Any]) -> None:
            try:
                self._tick_metrics()
                data = message.get("data", [])
                if isinstance(data, list):
                    for fill in data:
                        if fill.get("symbol") == symbol and fill.get("orderStatus") == "Filled":
                            self._call_soon(self._set_fill, fill.get("orderId"))
            except Exception as e:
                logging.error(f"WS fill error: {e}")
        if self.private_ws and self.connected:
            self.private_ws.order_stream(callback=handle)

    def subscribe_price(self, symbol: str) -> None:
        def handle(message: Dict[str, Any]) -> None:
            try:
                self._tick_metrics()
                data = message.get("data", {})
                if data.get("symbol") == symbol:
                    self._call_soon(self._set_price, safe_float(data, "lastPrice") or None)
            except Exception as e:
                logging.error(f"WS price error: {e}")
        if self.public_ws and self.public_connected:
            self.public_ws.ticker_stream(symbol=symbol, callback=handle)

    async def _reconnect_private(self) -> None:
        await self._connect_private()
        if self.connected:
            self.subscribe_fills(CONFIG["symbol"])

    async def _reconnect_public(self) -> None:
        await self._connect_public()
        if self.public_connected:
            self.subscribe_price(CONFIG["symbol"])

    async def wait_for_fill(self, order_id: str, symbol: str, timeout: int = 60) -> bool:
        if self.last_filled_order_id == order_id:
            return True
        self.order_fill_event.clear()
        deadline = time.monotonic() + max(0, int(timeout))
        while time.monotonic() < deadline:
            if self.last_filled_order_id == order_id:
                return True
            if not self.connected:
                if await check_order_status(order_id, symbol):
                    self.last_filled_order_id = order_id
                    return True
                await self._reconnect_private()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(self.order_fill_event.wait(), timeout=min(1.0, remaining))
            except asyncio.TimeoutError:
                continue
            finally:
                self.order_fill_event.clear()
        return await check_order_status(order_id, symbol)

    async def get_latest_price(self, symbol: str, timeout: int = 10) -> Optional[float]:
        if self.last_price is not None:
            return self.last_price
        self.price_event.clear()
        deadline = time.monotonic() + max(0, int(timeout))
        while time.monotonic() < deadline:
            if self.last_price is not None:
                return self.last_price
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(self.price_event.wait(), timeout=min(1.0, remaining))
            except asyncio.TimeoutError:
                continue
            finally:
                self.price_event.clear()
        if self.last_price is not None:
            return self.last_price
        # REST fallback
        try:
            session = SESSION_MANAGER.get_session()
            response = await async_bybit_call(session.get_tickers, category=CONFIG["category"], symbol=symbol)
            tickers = response.get("result", {}).get("list", [])
            if tickers:
                return safe_float(tickers[0], "lastPrice") or None
        except Exception as e:
            logging.error(f"REST price fallback failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  Position Health – FIX CRÍTICO #1 #2 #6 (trailing stop real)
# ══════════════════════════════════════════════════════════════════════════════
async def check_position_health(state: TradingState) -> float:
    """
    Monitorea la posición abierta con:
    - TP/SL dinámico por umbrales de PnL neto
    - Trailing stop REAL (persistente en state)
    """
    try:
        position_amt, entry_price = await get_position_info(CONFIG["symbol"])
        if position_amt == 0:
            # Reset tracker cuando no hay posición
            state.highest_pnl_tracker.clear()
            state.trailing_activated   = False
            state.trailing_stop_price  = 0.0
            state.trailing_peak_price  = 0.0
            return 0.0

        position_key = f"{CONFIG['symbol']}_{entry_price:.2f}"
        if position_key not in state.highest_pnl_tracker:
            state.highest_pnl_tracker[position_key] = {
                "highest_pnl": 0.0,
                "highest_pnl_pct": 0.0,
            }

        # Precio actual
        latest_price = await WS_MANAGER.get_latest_price(CONFIG["symbol"])
        if latest_price is None:
            df_q = await fetch_historical_data(CONFIG["symbol"], CONFIG["interval"], limit=50)
            latest_price = float(df_q["close"].iloc[-1]) if df_q is not None else entry_price

        # Mark price
        mark_price = latest_price
        try:
            session = SESSION_MANAGER.get_session()
            ticker_r = await async_bybit_call(session.get_tickers, category=CONFIG["category"], symbol=CONFIG["symbol"])
            tickers  = ticker_r.get("result", {}).get("list", [])
            if tickers:
                mark_price = safe_float(tickers[0], "markPrice", latest_price)
        except Exception:
            pass

        # PnL bruto y neto (con fees)
        if position_amt > 0:
            pnl_gross = (mark_price - entry_price) * position_amt
        else:
            pnl_gross = (entry_price - mark_price) * abs(position_amt)

        entry_fee = entry_price * abs(position_amt) * TAKER_FEE
        exit_fee  = mark_price  * abs(position_amt) * TAKER_FEE
        pnl_net   = pnl_gross - entry_fee - exit_fee

        position_value  = abs(position_amt * entry_price)
        position_margin = position_value / max(1, CONFIG["leverage"])
        pnl_net_pct     = (pnl_net / position_value * 100) if position_value > 0 else 0

        # Actualizar tracker
        tracker = state.highest_pnl_tracker[position_key]
        if pnl_net > tracker["highest_pnl"]:
            tracker["highest_pnl"]     = pnl_net
            tracker["highest_pnl_pct"] = pnl_net_pct

        # Indicadores para umbrales dinámicos
        df_quick = await fetch_historical_data(CONFIG["symbol"], CONFIG["interval"], limit=50)
        if df_quick is not None:
            df_quick = calculate_indicators(df_quick)
        volatility   = float(df_quick["volatility"].iloc[-1]) if df_quick is not None and "volatility" in df_quick.columns else 0.01
        trend_str    = calculate_trend_strength(df_quick) if df_quick is not None else 0.5

        tp_pct, sl_pct = adjust_tp_sl(volatility, CONFIG["profit_take_threshold"], CONFIG["stop_loss_threshold"], trend_str)
        tp_threshold = abs(position_margin * tp_pct)
        sl_threshold = -abs(position_margin * sl_pct)

        logging.info(
            f"🛡️ Health | Mark: {mark_price:.2f} | PnL net: {pnl_net:.2f}$ ({pnl_net_pct:.2f}%) "
            f"| Peak: {tracker['highest_pnl']:.2f}$ | Trail: {state.trailing_activated}"
        )

        # ── TRAILING STOP LOGIC (FIX CRÍTICO #6) ─────────────────────────────
        activation_pct = CONFIG["trailing_stop_activation"]  # fracción del TP para activar
        trail_pct      = CONFIG["trailing_stop_pct"]

        if pnl_net >= tp_threshold * activation_pct and not state.trailing_activated:
            state.trailing_activated  = True
            state.trailing_peak_price = mark_price
            # Precio de stop inicial
            if position_amt > 0:
                state.trailing_stop_price = mark_price * (1 - trail_pct)
            else:
                state.trailing_stop_price = mark_price * (1 + trail_pct)
            logging.info(
                f"🔔 Trailing stop ACTIVATED at {mark_price:.2f} | "
                f"Stop price: {state.trailing_stop_price:.2f}"
            )

        if state.trailing_activated:
            # Actualizar stop si el precio sigue a favor
            if position_amt > 0:
                if mark_price > state.trailing_peak_price:
                    state.trailing_peak_price = mark_price
                    state.trailing_stop_price = mark_price * (1 - trail_pct)
                    logging.debug(f"Trail updated → stop: {state.trailing_stop_price:.2f}")
                # Verificar si tocamos el trailing stop
                if mark_price <= state.trailing_stop_price:
                    logging.info(f"🔒 Trailing stop hit at {mark_price:.2f} (stop={state.trailing_stop_price:.2f})")
                    order_id = await place_order(CONFIG["symbol"], "sell", abs(position_amt), reduce_only=True)
                    if order_id and await WS_MANAGER.wait_for_fill(order_id, CONFIG["symbol"]):
                        _record_trade_result(pnl_net)
                        state.trailing_activated   = False
                        state.trailing_stop_price  = 0.0
                        state.trailing_peak_price  = 0.0
                        state.highest_pnl_tracker.pop(position_key, None)
                        return 0.0
            else:  # SHORT
                if mark_price < state.trailing_peak_price:
                    state.trailing_peak_price = mark_price
                    state.trailing_stop_price = mark_price * (1 + trail_pct)
                    logging.debug(f"Trail updated → stop: {state.trailing_stop_price:.2f}")
                if mark_price >= state.trailing_stop_price:
                    logging.info(f"🔒 Trailing stop hit at {mark_price:.2f} (stop={state.trailing_stop_price:.2f})")
                    order_id = await place_order(CONFIG["symbol"], "buy", abs(position_amt), reduce_only=True)
                    if order_id and await WS_MANAGER.wait_for_fill(order_id, CONFIG["symbol"]):
                        _record_trade_result(pnl_net)
                        state.trailing_activated   = False
                        state.trailing_stop_price  = 0.0
                        state.trailing_peak_price  = 0.0
                        state.highest_pnl_tracker.pop(position_key, None)
                        return 0.0

        # ── TAKE PROFIT ───────────────────────────────────────────────────
        if pnl_net >= tp_threshold:
            side = "sell" if position_amt > 0 else "buy"
            logging.info(f"✅ TAKE PROFIT | Net PnL: {pnl_net:.2f}$")
            order_id = await place_order(CONFIG["symbol"], side, abs(position_amt), reduce_only=True)
            if order_id and await WS_MANAGER.wait_for_fill(order_id, CONFIG["symbol"]):
                _record_trade_result(pnl_net)
                state.trailing_activated = False
                state.highest_pnl_tracker.pop(position_key, None)
                return 0.0

        # ── STOP LOSS ─────────────────────────────────────────────────────
        elif pnl_net <= sl_threshold and not state.trailing_activated:
            side = "sell" if position_amt > 0 else "buy"
            logging.warning(f"⛔ STOP LOSS | Net PnL: {pnl_net:.2f}$")
            order_id = await place_order(CONFIG["symbol"], side, abs(position_amt), reduce_only=True)
            if order_id and await WS_MANAGER.wait_for_fill(order_id, CONFIG["symbol"]):
                _record_trade_result(pnl_net)
                state.highest_pnl_tracker.pop(position_key, None)
                return 0.0

        return position_amt

    except Exception as e:
        logging.error(f"Error in position health check: {e}")
        return 0.0


def _record_trade_result(pnl_net: float) -> None:
    """Registra resultado de trade para métricas."""
    PERFORMANCE_METRICS["trades_total"] += 1
    PERFORMANCE_METRICS["total_pnl"]   += pnl_net
    if pnl_net > 0:
        PERFORMANCE_METRICS["trades_won"]  += 1
    else:
        PERFORMANCE_METRICS["trades_lost"] += 1
    total  = PERFORMANCE_METRICS["trades_total"]
    won    = PERFORMANCE_METRICS["trades_won"]
    wr     = won / total * 100 if total else 0
    avg_pnl = PERFORMANCE_METRICS["total_pnl"] / total if total else 0
    logging.info(
        f"📈 Trade result: {'WIN' if pnl_net > 0 else 'LOSS'} {pnl_net:.2f}$ | "
        f"W/L: {won}/{total} ({wr:.1f}%) | Avg PnL: {avg_pnl:.2f}$"
    )


# ══════════════════════════════════════════════════════════════════════════════
#                      Graceful Shutdown
# ══════════════════════════════════════════════════════════════════════════════
async def graceful_shutdown() -> None:
    global _shutdown_requested
    _shutdown_requested = True
    logging.info("🛑 Graceful shutdown initiated.")
    try:
        position_amt, _ = await get_position_info(CONFIG["symbol"])
        if position_amt != 0:
            side = "sell" if position_amt > 0 else "buy"
            logging.warning(f"Closing position before shutdown: {position_amt} {CONFIG['symbol']}")
            order_id = await place_order(CONFIG["symbol"], side, abs(position_amt), reduce_only=True)
            if order_id:
                filled = await WS_MANAGER.wait_for_fill(order_id, CONFIG["symbol"], timeout=30)
                logging.info("✅ Position closed." if filled else "⚠️ Could not confirm closure – check manually.")
        else:
            logging.info("✅ No open positions. Clean shutdown.")
    except Exception as e:
        logging.error(f"Error during graceful shutdown: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#                      Main Trading Loop
# ══════════════════════════════════════════════════════════════════════════════
async def sync_and_trade(
    model: "XGBClassifier",
    pca: "PCA",
    scaler: "StandardScaler",
) -> None:
    """Main trading loop con TradingState persistente y todas las correcciones aplicadas."""
    global _shutdown_requested

    state = TradingState()
    state.peak_equity  = await get_account_equity()
    state.current_day  = datetime.now(timezone.utc).date()
    max_consecutive_errors = 5

    while not _shutdown_requested:
        try:
            current_time = time.time()

            # ── Reset diario ────────────────────────────────────────────────
            today = datetime.now(timezone.utc).date()
            if today != state.current_day:
                state.daily_trades = 0
                state.current_day  = today
                logging.info("🔄 New trading day – daily trades counter reset.")

            # ── Health check de posición ────────────────────────────────────
            if current_time - state.last_check_time >= CONFIG["position_health_interval"]:
                await check_position_health(state)  # pasa state completo (FIX #1 #2 #6)
                state.last_check_time = current_time

            # ── Protección drawdown ─────────────────────────────────────────
            current_equity = await get_account_equity()
            if current_equity > state.peak_equity:
                state.peak_equity = current_equity

            drawdown = (state.peak_equity - current_equity) / state.peak_equity if state.peak_equity > 0 else 0.0
            if drawdown > CONFIG["max_drawdown_pct"]:
                if not state.max_drawdown_reached:
                    logging.critical(
                        f"⚠️ MAX DRAWDOWN {drawdown:.2%} > {CONFIG['max_drawdown_pct']:.2%} | "
                        "Pausando trading hasta recuperación."
                    )
                    state.max_drawdown_reached = True
                await asyncio.sleep(CONFIG["signal_poll_seconds"] * 5)
                continue
            elif state.max_drawdown_reached:
                logging.info(f"✅ Drawdown recuperado: {drawdown:.2%}. Reanudando.")
                state.max_drawdown_reached = False

            # ── Cooldown ────────────────────────────────────────────────────
            if current_time - state.last_trade_time < CONFIG["trade_cooldown"]:
                await asyncio.sleep(CONFIG["signal_poll_seconds"])
                continue

            # ── Límite diario ───────────────────────────────────────────────
            if state.daily_trades >= CONFIG["max_trades_per_day"]:
                logging.info(f"📊 Max diario ({CONFIG['max_trades_per_day']}) alcanzado.")
                await asyncio.sleep(CONFIG["signal_poll_seconds"] * 5)
                continue

            state.iteration += 1

            # ── Balance real ─────────────────────────────────────────────────
            usdt_balance = await get_account_equity()
            if usdt_balance < 5:
                logging.error("⚠️ Balance insuficiente (<5 USDT). Esperando...")
                await asyncio.sleep(CONFIG["signal_poll_seconds"] * 5)
                continue

            # ── Datos e indicadores ─────────────────────────────────────────
            df = await fetch_historical_data(CONFIG["symbol"], CONFIG["interval"])
            if df is None or df.empty:
                await asyncio.sleep(CONFIG["signal_poll_seconds"])
                continue

            df = calculate_indicators(df)
            if df is None or df.empty:
                await asyncio.sleep(CONFIG["signal_poll_seconds"])
                continue

            # ── Reentrenamiento periódico ───────────────────────────────────
            if state.iteration % CONFIG["retrain_interval"] == 0:
                logging.info("🔄 Retraining model...")
                try:
                    new_model, new_pca, new_scaler = await load_or_train_model(df)
                    if new_model and new_pca and new_scaler:
                        model, pca, scaler = new_model, new_pca, new_scaler
                        logging.info("✅ Model retrained successfully.")
                except Exception as e:
                    logging.warning(f"Retraining failed: {e}")

            # ── Señal con multi-confirmación ────────────────────────────────
            latest_signal, confidence = generate_signal(df, model, pca, scaler)
            latest_price = float(df["close"].iloc[-1])

            # Tamaño de posición
            pos_size = await get_position_size(usdt_balance, latest_price, CONFIG["symbol"], df)

            # Estado actual de posición
            position_amt, entry_price = await get_position_info(CONFIG["symbol"])

            logging.info(
                f"📊 [{state.iteration}] Price={latest_price:.2f} | Balance={usdt_balance:.2f} | "
                f"Pos={position_amt:.6f} | Signal={'LONG' if latest_signal==SIGNAL_LONG else 'SHORT' if latest_signal==SIGNAL_SHORT else 'NEUTRAL'} "
                f"({confidence:.2%}) | Trades={state.daily_trades}/{CONFIG['max_trades_per_day']}"
            )

            # ── Pending TP/SL ───────────────────────────────────────────────
            if state.pending_tp_sl and position_amt != 0:
                if await set_tp_sl(CONFIG["symbol"], entry_price, abs(position_amt), df):
                    state.pending_tp_sl = False

            # ── Señales NEUTRAL → no operar ─────────────────────────────────
            if latest_signal == SIGNAL_NEUTRAL:
                state.previous_signal = latest_signal
                await asyncio.sleep(CONFIG["signal_poll_seconds"])
                continue

            # ── Confirmación de señal (2 velas consecutivas iguales) ────────
            if state.previous_signal != latest_signal:
                logging.info(f"⏳ Signal changed to {latest_signal}. Waiting for confirmation...")
                state.previous_signal = latest_signal
                await asyncio.sleep(CONFIG["signal_poll_seconds"])
                continue

            # Señal repetida = confirmada
            if latest_signal == state.last_signal:
                await asyncio.sleep(CONFIG["signal_poll_seconds"])
                continue

            state.last_signal = latest_signal

            # ── Funding rate check ──────────────────────────────────────────
            if position_amt == 0:
                funding = await get_funding_rate(CONFIG["symbol"])
                if funding is not None and abs(funding) > 0.001:
                    if (latest_signal == SIGNAL_LONG and funding > 0.001) or \
                       (latest_signal == SIGNAL_SHORT and funding < -0.001):
                        logging.warning(
                            f"⚠️ Funding rate {funding*100:.4f}% contrario a la señal. Skip."
                        )
                        await asyncio.sleep(CONFIG["signal_poll_seconds"])
                        continue

            # ══════════════════════════════════════════════════════════════
            #  EJECUCIÓN DE TRADES
            # ══════════════════════════════════════════════════════════════

            if latest_signal == SIGNAL_SHORT and position_amt > 0:
                # Cierra LONG → abre SHORT
                logging.info(f"🔻 Cerrando LONG ({position_amt}) y abriendo SHORT ({pos_size})")
                oid = await place_order(CONFIG["symbol"], "sell", abs(position_amt), reduce_only=True)
                if oid and await WS_MANAGER.wait_for_fill(oid, CONFIG["symbol"]):
                    _record_trade_result(0)  # cierre a precio actual (TP/SL lo había manejado)
                    if state.daily_trades < CONFIG["max_trades_per_day"]:
                        oid2 = await place_order(CONFIG["symbol"], "sell", pos_size, reduce_only=False)
                        if oid2 and await WS_MANAGER.wait_for_fill(oid2, CONFIG["symbol"]):
                            sz, ep = await get_position_info(CONFIG["symbol"])
                            if sz < 0 and ep > 0:
                                if not await set_tp_sl(CONFIG["symbol"], ep, abs(sz), df):
                                    state.pending_tp_sl = True
                            state.last_trade_time  = time.time()
                            state.daily_trades    += 1
                            state.previous_signal  = None
                            state.trailing_activated = False

            elif latest_signal == SIGNAL_LONG and position_amt < 0:
                # Cierra SHORT → abre LONG
                logging.info(f"🔺 Cerrando SHORT ({position_amt}) y abriendo LONG ({pos_size})")
                oid = await place_order(CONFIG["symbol"], "buy", abs(position_amt), reduce_only=True)
                if oid and await WS_MANAGER.wait_for_fill(oid, CONFIG["symbol"]):
                    _record_trade_result(0)
                    if state.daily_trades < CONFIG["max_trades_per_day"]:
                        oid2 = await place_order(CONFIG["symbol"], "buy", pos_size, reduce_only=False)
                        if oid2 and await WS_MANAGER.wait_for_fill(oid2, CONFIG["symbol"]):
                            sz, ep = await get_position_info(CONFIG["symbol"])
                            if sz > 0 and ep > 0:
                                if not await set_tp_sl(CONFIG["symbol"], ep, sz, df):
                                    state.pending_tp_sl = True
                            state.last_trade_time  = time.time()
                            state.daily_trades    += 1
                            state.previous_signal  = None
                            state.trailing_activated = False

            elif latest_signal == SIGNAL_LONG and position_amt == 0:
                # Abre LONG
                if state.daily_trades < CONFIG["max_trades_per_day"]:
                    logging.info(f"🔺 Abriendo LONG: {pos_size} {CONFIG['symbol']}")
                    oid = await place_order(CONFIG["symbol"], "buy", pos_size, reduce_only=False)
                    if oid and await WS_MANAGER.wait_for_fill(oid, CONFIG["symbol"]):
                        sz, ep = await get_position_info(CONFIG["symbol"])
                        if sz > 0 and ep > 0:
                            if not await set_tp_sl(CONFIG["symbol"], ep, sz, df):
                                state.pending_tp_sl = True
                        state.last_trade_time  = time.time()
                        state.daily_trades    += 1
                        state.previous_signal  = None
                        state.trailing_activated = False

            elif latest_signal == SIGNAL_SHORT and position_amt == 0:
                # Abre SHORT
                if state.daily_trades < CONFIG["max_trades_per_day"]:
                    logging.info(f"🔻 Abriendo SHORT: {pos_size} {CONFIG['symbol']}")
                    oid = await place_order(CONFIG["symbol"], "sell", pos_size, reduce_only=False)
                    if oid and await WS_MANAGER.wait_for_fill(oid, CONFIG["symbol"]):
                        sz, ep = await get_position_info(CONFIG["symbol"])
                        if sz < 0 and ep > 0:
                            if not await set_tp_sl(CONFIG["symbol"], ep, abs(sz), df):
                                state.pending_tp_sl = True
                        state.last_trade_time  = time.time()
                        state.daily_trades    += 1
                        state.previous_signal  = None
                        state.trailing_activated = False

            state.consecutive_errors = 0

            # ── Memory cleanup periódica ────────────────────────────────────
            if CONFIG["enable_gc_collection"] and state.iteration % CONFIG["memory_cleanup_interval"] == 0:
                _cleanup_caches()

            await asyncio.sleep(CONFIG["signal_poll_seconds"])

        except asyncio.CancelledError:
            logging.info("🛑 Trading loop cancelled.")
            break
        except Exception as e:
            state.consecutive_errors += 1
            logging.error(
                f"❌ Trading loop error ({state.consecutive_errors}/{max_consecutive_errors}): {e}"
            )
            if state.consecutive_errors >= max_consecutive_errors:
                logging.error("🔥 Demasiados errores. Reiniciando sesiones...")
                SESSION_MANAGER._initialize_session()
                await WS_MANAGER.connect()
                WS_MANAGER.subscribe_fills(CONFIG["symbol"])
                WS_MANAGER.subscribe_price(CONFIG["symbol"])
                state.consecutive_errors = 0
            await asyncio.sleep(min(CONFIG["signal_poll_seconds"] * 2, 30))


def _cleanup_caches() -> None:
    """Limpia caches expiradas para evitar memory leaks."""
    now = time.time()

    def _purge(cache: dict, ts_dict: dict, max_age: float) -> None:
        to_remove = [k for k, t in ts_dict.items() if now - t > max_age]
        for k in to_remove:
            cache.pop(k, None)
            ts_dict.pop(k, None)

    _purge(_historical_data_cache, _historical_data_timestamp, 300)
    _purge(_indicator_cache,       _indicator_cache_ts,        120)
    _purge(_instrument_cache,      _instrument_cache_ts,       300)
    _purge(_account_equity_cache,  _account_equity_ts,          30)
    _purge(_position_cache,        _position_cache_ts,          10)
    _purge(_funding_cache,         _funding_cache_ts,           60)

    collected = gc.collect()
    if collected > 0:
        logging.debug(f"GC collected {collected} objects")


# ══════════════════════════════════════════════════════════════════════════════
#                          Entry Point
# ══════════════════════════════════════════════════════════════════════════════
SESSION_MANAGER = BybitSessionManager(
    api_key=CONFIG["api_key"],
    api_secret=CONFIG["api_secret"],
    demo_trading=CONFIG["demo_trading"]
)

WS_MANAGER = WebSocketManager(
    api_key=CONFIG["api_key"],
    api_secret=CONFIG["api_secret"],
    private_channel_type=CONFIG["channel_type"],
    public_channel_type=CONFIG["public_channel_type"]
)


async def main() -> None:
    """Inicializa y arranca el bot."""
    logging.info("=" * 70)
    logging.info(" TRADING BOT v2.0 starting...")
    logging.info("=" * 70)

    if not get_server_time_and_sync():
        logging.error("❌ Exiting: time sync failure.")
        return

    if not await SESSION_MANAGER.validate_credentials():
        logging.error("❌ Exiting: invalid API credentials.")
        return

    await get_fee_rates()

    if not await set_leverage(CONFIG["symbol"], CONFIG["leverage"]):
        logging.error("❌ Exiting: leverage setup failure.")
        return

    if not await WS_MANAGER.connect():
        logging.error("❌ Exiting: WebSocket connection failed.")
        return

    WS_MANAGER.subscribe_fills(CONFIG["symbol"])
    WS_MANAGER.subscribe_price(CONFIG["symbol"])

    model, pca, scaler = await load_or_train_model()
    if model is None or pca is None or scaler is None:
        logging.error("❌ Exiting: failed to load/train model.")
        return

    logging.info(f"🚀 Bot started | Symbol={CONFIG['symbol']} | Leverage={CONFIG['leverage']}x")
    logging.info(f"   Max drawdown: {CONFIG['max_drawdown_pct']:.0%} | Min R:R: {CONFIG['min_rr_ratio']:.1f}")
    logging.info(f"   ADX filter: >{CONFIG['adx_min_threshold']} | Confidence: >{CONFIG['signal_confidence_threshold']:.0%}")
    logging.info(f"   Forward candles: {CONFIG['forward_candles']} | PCA components: {CONFIG['pca_n_components']}")

    await sync_and_trade(model, pca, scaler)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()

    def _signal_handler():
        logging.info("🛑 Shutdown signal received...")
        loop.create_task(graceful_shutdown())
        loop.call_later(35, loop.stop)
        logging.info("📊 Performance Summary:")
        logging.info(f"   Total trades: {PERFORMANCE_METRICS['trades_total']}")
        logging.info(f"   Won/Lost: {PERFORMANCE_METRICS['trades_won']}/{PERFORMANCE_METRICS['trades_lost']}")
        total = PERFORMANCE_METRICS["trades_total"]
        if total:
            wr = PERFORMANCE_METRICS["trades_won"] / total * 100
            logging.info(f"   Win rate: {wr:.1f}%")
        logging.info(f"   Total PnL: {PERFORMANCE_METRICS['total_pnl']:.2f}$")
        logging.info(f"   API calls: {PERFORMANCE_METRICS['api_calls_total']}")
        logging.info(f"   WS messages: {PERFORMANCE_METRICS['websocket_messages_processed']}")

    try:
        if os.name != "nt":
            loop.add_signal_handler(signal.SIGINT,  _signal_handler)
            loop.add_signal_handler(signal.SIGTERM, _signal_handler)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("🛑 Interrupted by user.")
        loop.run_until_complete(graceful_shutdown())
    except Exception as e:
        logging.error(f"🔥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        loop.close()
        logging.info("✅ Bot shutdown complete.")
