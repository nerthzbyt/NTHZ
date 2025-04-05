import enum
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, JSON, UniqueConstraint, Enum, \
    Engine, inspect
from sqlalchemy.orm import declarative_base, sessionmaker

# Configuración del logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# Configuración de la base de datos
DATABASE_URL = "postgresql://postgres:angel_amely@localhost:5432/nertzhul"
Base = declarative_base()


class OrderStatus(enum.Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    REJECTED = "REJECTED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class OrderSide(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(enum.Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"


class TimeInForce(enum.Enum):
    GTC = "GTC"  # Good Till Cancelled
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill
    GTD = "GTD"  # Good Till Date


class PositionSide(enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class MarketData(Base):
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    interval = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc),
                        onupdate=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timestamp', 'interval', name='unique_market_data'),
    )
    
    def __init__(self, **kwargs):
        # Establecer valor predeterminado para interval si no se proporciona
        if 'interval' not in kwargs:
            kwargs['interval'] = '1m'  # Valor predeterminado
        super().__init__(**kwargs)

    def __repr__(self):
        return f"<MarketData(symbol='{self.symbol}', timestamp='{self.timestamp}', close={self.close})>"

    def to_dict(self):
        return {
            "id": self.id,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "interval": self.interval,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


class Orderbook(Base):
    __tablename__ = "orderbook"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    bids = Column(JSON, nullable=False)  # JSON array of price/quantity pairs
    asks = Column(JSON, nullable=False)  # JSON array of price/quantity pairs
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    def __repr__(self):
        return f"<Orderbook(symbol='{self.symbol}', timestamp='{self.timestamp}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "bids": self.bids,
            "asks": self.asks,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


class MarketTicker(Base):
    __tablename__ = "market_ticker"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    last_price = Column(Float, nullable=False)
    bid_price = Column(Float, nullable=True)
    ask_price = Column(Float, nullable=True)
    volume_24h = Column(Float, nullable=False)
    high_24h = Column(Float, nullable=False)
    low_24h = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timestamp', name='unique_market_ticker'),
    )

    def __repr__(self):
        return f"<MarketTicker(symbol='{self.symbol}', timestamp='{self.timestamp}', last_price={self.last_price})>"

    def to_dict(self):
        return {
            "id": self.id,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "last_price": self.last_price,
            "bid_price": self.bid_price,
            "ask_price": self.ask_price,
            "volume_24h": self.volume_24h,
            "high_24h": self.high_24h,
            "low_24h": self.low_24h,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    trade_id = Column(String, index=True, unique=True, nullable=False)
    symbol = Column(String, index=True, nullable=False)
    order_id = Column(String, index=True, nullable=True)
    external_id = Column(String, index=True, nullable=True)
    position_id = Column(String, index=True, nullable=True)
    side = Column(String, index=True, nullable=False)  # BUY / SELL
    price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False)
    commission = Column(Float, default=0.0, nullable=False)
    commission_asset = Column(String, nullable=True)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    trade_type = Column(String, nullable=True)  # ENTRY, EXIT, ADJUSTMENT
    pnl = Column(Float, nullable=True)  # Profit/Loss for this trade
    strategy = Column(String, nullable=True)  # Strategy that generated this trade
    tags = Column(JSON, nullable=True)  # Additional metadata as JSON
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc),
                        onupdate=datetime.now(timezone.utc))
    
    # Campos adicionales para compatibilidad
    action = Column(String, nullable=True)
    entry_price = Column(Float, nullable=True)
    exit_price = Column(Float, nullable=True)
    profit_loss = Column(Float, nullable=True)
    decision = Column(String, nullable=True)
    combined = Column(Float, nullable=True)
    tp = Column(Float, nullable=True)
    sl = Column(Float, nullable=True)
    status = Column(String, nullable=True, default="open")
    
    # Campos para métricas de trading
    ild = Column(Float, nullable=True)  # Imbalance Liquidity Delta
    egm = Column(Float, nullable=True)  # Exponential Growth Momentum
    pio = Column(Float, nullable=True)  # Price Impact of Orders
    volatility = Column(Float, nullable=True)  # Volatilidad del mercado
    atr = Column(Float, nullable=True)  # Average True Range
    rsi = Column(Float, nullable=True)  # Relative Strength Index

    def __init__(self, **kw: Any):
        super().__init__(**kw)  # Usar **kw en lugar de kw
        self.profit_loss = 0.0
        self.exit_price = None
        self.action = None
        # No inicializamos entry_price ya que está definido como columna

    def __repr__(self):
        return f"<Trade(id={self.id}, trade_id='{self.trade_id}', symbol='{self.symbol}', side='{self.side}', price={self.price}, quantity={self.quantity})>"

    def to_dict(self):
        return {
            "id": self.id,
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "order_id": self.order_id,
            "external_id": self.external_id,
            "position_id": self.position_id,
            "side": self.side,
            "price": self.price,
            "quantity": self.quantity,
            "commission": self.commission,
            "commission_asset": self.commission_asset,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "trade_type": self.trade_type,
            "pnl": self.pnl,
            "strategy": self.strategy,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "action": self.action,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "profit_loss": self.profit_loss,
            "decision": self.decision,
            "combined": self.combined,
            "tp": self.tp,
            "sl": self.sl,
            "status": self.status,
            # Métricas de trading
            "ild": self.ild,
            "egm": self.egm,
            "pio": self.pio,
            "volatility": self.volatility,
            "atr": self.atr,
            "rsi": self.rsi
        }


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, index=True)
    position_id = Column(String, index=True, unique=True, nullable=False)
    symbol = Column(String, index=True, nullable=False)
    side = Column(Enum(PositionSide), index=True, nullable=False)  # LONG / SHORT
    entry_price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False)
    leverage = Column(Float, default=1.0, nullable=False)
    margin_type = Column(String, nullable=True)  # ISOLATED / CROSS
    status = Column(String, index=True, nullable=False)  # OPEN / CLOSED
    unrealized_pnl = Column(Float, nullable=True)
    realized_pnl = Column(Float, nullable=True)
    liquidation_price = Column(Float, nullable=True)
    bankruptcy_price = Column(Float, nullable=True)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    open_time = Column(DateTime(timezone=True), nullable=True)
    close_time = Column(DateTime(timezone=True), nullable=True)
    take_profit = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    strategy = Column(String, nullable=True)
    tags = Column(JSON, nullable=True)  # Additional metadata as JSON
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc),
                        onupdate=datetime.now(timezone.utc))
    
    # Campos adicionales para compatibilidad con el código existente
    order_id = Column(String, nullable=True)
    action = Column(String, nullable=True)
    tp = Column(Float, nullable=True)
    sl = Column(Float, nullable=True)
    last_update = Column(DateTime(timezone=True), nullable=True)
    partial_fills = Column(String, default="{}")
    remaining_quantity = Column(Float, nullable=False, default=0.0)
    filled_quantity = Column(Float, nullable=False, default=0.0)
    execution_status = Column(String, nullable=False, default="pending")
    retry_count = Column(Integer, nullable=False, default=0)

    def __repr__(self):
        return f"<Position(id={self.id}, position_id='{self.position_id}', symbol='{self.symbol}', side='{self.side}', quantity={self.quantity}, entry_price={self.entry_price})>"

    def to_dict(self):
        return {
            "id": self.id,
            "position_id": self.position_id,
            "symbol": self.symbol,
            "side": self.side.value if isinstance(self.side, enum.Enum) else self.side,
            "entry_price": self.entry_price,
            "quantity": self.quantity,
            "leverage": self.leverage,
            "margin_type": self.margin_type,
            "status": self.status,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "liquidation_price": self.liquidation_price,
            "bankruptcy_price": self.bankruptcy_price,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "take_profit": self.take_profit,
            "stop_loss": self.stop_loss,
            "strategy": self.strategy,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "order_id": self.order_id,
            "action": self.action,
            "tp": self.tp,
            "sl": self.sl,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "partial_fills": self.partial_fills,
            "remaining_quantity": self.remaining_quantity,
            "filled_quantity": self.filled_quantity,
            "execution_status": self.execution_status,
            "retry_count": self.retry_count
        }


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, index=True, unique=True, nullable=False)
    external_id = Column(String, index=True, nullable=True)  # ID from exchange
    client_order_id = Column(String, index=True, nullable=True)
    symbol = Column(String, index=True, nullable=False)
    side = Column(Enum(OrderSide), index=True, nullable=False)  # BUY / SELL
    type = Column(Enum(OrderType), index=True, nullable=False)  # MARKET / LIMIT / STOP / etc.
    time_in_force = Column(Enum(TimeInForce), nullable=True)  # GTC, IOC, FOK
    price = Column(Float, nullable=True)  # Null for market orders
    quantity = Column(Float, nullable=False)
    executed_quantity = Column(Float, default=0.0, nullable=False)
    remaining_quantity = Column(Float, nullable=True)
    status = Column(Enum(OrderStatus), index=True, nullable=False)
    stop_price = Column(Float, nullable=True)
    position_id = Column(String, index=True, nullable=True)  # Related position
    reduce_only = Column(Boolean, default=False, nullable=False)
    close_position = Column(Boolean, default=False, nullable=False)
    working = Column(Boolean, nullable=True)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    created_time = Column(DateTime(timezone=True), nullable=True)
    updated_time = Column(DateTime(timezone=True), nullable=True)
    strategy = Column(String, nullable=True)
    tags = Column(JSON, nullable=True)  # Additional metadata as JSON
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc),
                        onupdate=datetime.now(timezone.utc))

    def __repr__(self):
        return f"<Order(id={self.id}, order_id='{self.order_id}', symbol='{self.symbol}', side='{self.side}', type='{self.type}', status='{self.status}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "order_id": self.order_id,
            "external_id": self.external_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side.value if isinstance(self.side, enum.Enum) else self.side,
            "type": self.type.value if isinstance(self.type, enum.Enum) else self.type,
            "time_in_force": self.time_in_force.value if isinstance(self.time_in_force,
                                                                enum.Enum) else self.time_in_force,
            "price": self.price,
            "quantity": self.quantity,
            "executed_quantity": self.executed_quantity,
            "remaining_quantity": self.remaining_quantity,
            "status": self.status.value if isinstance(self.status, enum.Enum) else self.status,
            "stop_price": self.stop_price,
            "position_id": self.position_id,
            "reduce_only": self.reduce_only,
            "close_position": self.close_position,
            "working": self.working,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "created_time": self.created_time.isoformat() if self.created_time else None,
            "updated_time": self.updated_time.isoformat() if self.updated_time else None,
            "strategy": self.strategy,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


def init_db(database_url: str = DATABASE_URL) -> "Engine":
    """Inicializa la base de datos y crea las tablas si no existen.

    Args:
        database_url (str, optional): URL de conexión a la base de datos. Por defecto usa DATABASE_URL.

    Returns:
        Engine: Motor de SQLAlchemy para la conexión a la base de datos.
    """
    # Configuración optimizada para PostgreSQL
    engine = create_engine(
        database_url,
        pool_size=5,  # Número de conexiones en el pool
        max_overflow=10,  # Conexiones adicionales permitidas
        pool_timeout=30,  # Tiempo de espera para obtener una conexión
        pool_recycle=1800,  # Reciclar conexiones cada 30 minutos
        echo=False  # Desactivar logs de SQL para mejor rendimiento
    )

    try:
        # Verificar si la base de datos ya existe
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        # Si ya existen tablas, solo crear las que faltan
        if existing_tables:
            logger.info(f"Tablas existentes detectadas: {len(existing_tables)}")
            # Crear solo las tablas que no existen
            Base.metadata.create_all(bind=engine, checkfirst=True)
        else:
            # Si no hay tablas, crear todas
            logger.info("No se detectaron tablas existentes. Creando esquema completo.")
            Base.metadata.create_all(bind=engine)
        
        logger.info("Base de datos inicializada correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar la base de datos: {e}")
        # En caso de error, intentar crear las tablas de forma segura
        Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("Base de datos inicializada en modo seguro")
        
    return engine


# Crear el motor global
engine = init_db(DATABASE_URL)

# Crear SessionLocal usando el motor global
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Función de utilidad para obtener una sesión
def get_db():
    """Generador para obtener una sesión de base de datos."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
