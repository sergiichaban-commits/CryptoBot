# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v10.0 — Fast RSI + Momentum + Real-time levels
"""
# ... (импорты и базовые классы остаются)

# =========================
# SCALPING КОНФИГ
# =========================
# Таймфрейм только 5m
TF_SCALP = "5"

# Более чувствительные параметры
RSI_OVERSOLD = 38    # было 32
RSI_OVERBOUGHT = 62  # было 68
RSI_PERIOD = 14

# Быстрые EMA для момента
EMA_FAST = 5
EMA_SLOW = 13

# Объемный фильтр
VOLUME_SPIKE_MULT = 1.3  # было 1.8

# Risk management для скальпинга
ATR_SL_MULT = 0.8
ATR_TP_MULT = 1.5
TP_MIN_PCT = 0.002   # 0.2%
TP_MAX_PCT = 0.008   # 0.8%
RR_MIN = 1.8

# Убрать лишние подтверждения
MIN_CONFIRMATIONS = 1  # было 2

# =========================
# ИСПРАВЛЕННЫЕ ИНДИКАТОРЫ
# =========================
def relative_strength_index_fixed(data: List[Tuple[float, float, float, float, float]], period: int = RSI_PERIOD) -> float:
    """ИСПРАВЛЕННЫЙ RSI - только последние period+1 свечей"""
    if len(data) < period + 1:
        return 50.0
    
    # Берем ТОЛЬКО последние period+1 свечей
    closes = [bar[3] for bar in data[-(period+1):]]
    
    gains = []
    losses = []
    
    for i in range(1, len(closes)):
        change = closes[i] - closes[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))
    
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def momentum_indicator(data: List[Tuple[float, float, float, float, float]], period: int = 5) -> float:
    """Индикатор момента - скорость движения цены"""
    if len(data) < period:
        return 0.0
    
    current_close = data[-1][3]
    past_close = data[-period][3]
    
    return ((current_close - past_close) / past_close) * 100

def detect_key_levels(data: List[Tuple[float, float, float, float, float]], lookback: int = 20) -> Tuple[float, float]:
    """Определение ближайших уровней поддержки/сопротивления"""
    if len(data) < lookback:
        return 0.0, 0.0
    
    recent_data = data[-lookback:]
    highs = [bar[1] for bar in recent_data]
    lows = [bar[2] for bar in recent_data]
    
    resistance = max(highs)
    support = min(lows)
    
    return support, resistance

# =========================
# УПРОЩЕННАЯ ЛОГИКА СИГНАЛОВ
# =========================
class ScalpingEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt
    
    def update_indicators_fast(self, sym: str) -> None:
        """Быстрое обновление индикаторов только для 5m"""
        st = self.mkt.state[sym]
        
        if len(st.k5) >= RSI_PERIOD + 1:
            st.rsi_5m = relative_strength_index_fixed(st.k5)
        
        if len(st.k5) >= EMA_SLOW:
            closes = [bar[3] for bar in st.k5]
            st.ema_fast = exponential_moving_average(closes, EMA_FAST)
            st.ema_slow = exponential_moving_average(closes, EMA_SLOW)
        
        # Momentum
        st.momentum = momentum_indicator(st.k5)
        
        # Key levels
        st.support, st.resistance = detect_key_levels(st.k5)
    
    def generate_scalp_signal(self, sym: str) -> Optional[Dict[str, Any]]:
        """Быстрые сигналы для скальпинга"""
        st = self.mkt.state[sym]
        
        # Минимальные требования
        if len(st.k5) < max(RSI_PERIOD + 5, EMA_SLOW + 5):
            return None
        
        current_price = st.k5[-1][3]
        current_time = now_s()
        
        # Кулдаун
        if current_time - st.last_signal_ts < 60:  # 1 мин между сигналами
            return None
        
        # Базовые условия
        rsi = st.rsi_5m
        momentum = st.momentum
        ema_fast = st.ema_fast
        ema_slow = st.ema_slow
        
        # Volume check
        avg_vol = sum(bar[4] for bar in st.k5[-6:-1]) / 5  # 5 последних свечей
        current_vol = st.k5[-1][4]
        volume_ok = current_vol > avg_vol * VOLUME_SPIKE_MULT
        
        # LONG сигналы (более агрессивные)
        long_conditions = [
            rsi < RSI_OVERSOLD and rsi > 25,  # Не слишком глубоко
            momentum > -1.0,  # Не сильное падение
            ema_fast > ema_slow or current_price > ema_fast,
            volume_ok,
            current_price > st.support  # Выше поддержки
        ]
        
        # SHORT сигналы  
        short_conditions = [
            rsi > RSI_OVERBOUGHT and rsi < 75,  # Не слишком высоко
            momentum < 1.0,  # Не сильный рост
            ema_fast < ema_slow or current_price < ema_fast, 
            volume_ok,
            current_price < st.resistance  # Ниже сопротивления
        ]
        
        side = None
        reasons = []
        
        if sum(long_conditions) >= 3:  # 3 из 5 условий
            side = "LONG"
            reasons = [
                f"RSI отскок: {rsi:.1f}",
                f"Momentum: {momentum:.2f}%",
                "Выше поддержки" if long_conditions[4] else "Возле поддержки",
                "Объем подтверждает" if volume_ok else ""
            ]
        elif sum(short_conditions) >= 3:
            side = "SHORT"  
            reasons = [
                f"RSI отскок: {rsi:.1f}",
                f"Momentum: {momentum:.2f}%",
                "Ниже сопротивления" if short_conditions[4] else "Возле сопротивления",
                "Объем подтверждает" if volume_ok else ""
            ]
        
        if not side:
            return None
        
        # Быстрый расчет SL/TP
        atr_val = average_true_range(st.k5, 10)  # 10 периодов для скорости
        if atr_val == 0:
            atr_val = current_price * 0.005  # 0.5% fallback
        
        if side == "LONG":
            sl = current_price - (atr_val * ATR_SL_MULT)
            tp = current_price + (atr_val * ATR_TP_MULT)
        else:
            sl = current_price + (atr_val * ATR_SL_MULT)  
            tp = current_price - (atr_val * ATR_TP_MULT)
        
        # Корректировка по процентам
        tp_pct = abs(tp - current_price) / current_price
        if tp_pct < TP_MIN_PCT:
            tp = current_price * (1 + TP_MIN_PCT) if side == "LONG" else current_price * (1 - TP_MIN_PCT)
        
        rr = (abs(tp - current_price)) / (abs(sl - current_price))
        
        if rr < RR_MIN:
            return None
        
        st.last_signal_ts = current_time
        
        return {
            "symbol": sym,
            "side": side, 
            "entry": current_price,
            "tp1": tp,
            "sl": sl,
            "rr": rr,
            "rsi": rsi,
            "momentum": momentum,
            "reason": [r for r in reasons if r],
            "timeframe": "5m SCALP",
            "duration": "5-15min",
            "confidence": min(80, 40 + sum(long_conditions if side == "LONG" else short_conditions) * 10)
        }
