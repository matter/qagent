"""Unified technical indicator API with TA-Lib / pandas-ta / numpy fallback.

Usage::

    from backend.indicators import ta

    ta.rsi(close, period=14)          # -> pd.Series
    ta.macd(close)                    # -> (dif, dea, histogram)
    ta.sma(close, period=20)          # -> pd.Series

Every public function tries TA-Lib first, then pandas-ta, and finally a
pure-numpy/pandas reference implementation so that the adapter always works
even when optional native libraries are not installed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from backend.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Optional library detection
# ---------------------------------------------------------------------------

_HAS_TALIB = False
_HAS_PANDAS_TA = False

try:
    import talib as _talib  # type: ignore[import-untyped]

    _HAS_TALIB = True
    log.debug("indicators.talib_available")
except ImportError:
    _talib = None  # type: ignore[assignment]

try:
    import pandas_ta as _pta  # type: ignore[import-untyped]

    _HAS_PANDAS_TA = True
    log.debug("indicators.pandas_ta_available")
except ImportError:
    _pta = None  # type: ignore[assignment]


def _to_series(arr, index=None, name: str | None = None) -> pd.Series:
    """Ensure *arr* is a pd.Series with the given *index*."""
    if isinstance(arr, pd.Series):
        if index is not None:
            arr.index = index
        if name:
            arr.name = name
        return arr
    return pd.Series(arr, index=index, name=name, dtype=float)


# ======================================================================
# Momentum indicators
# ======================================================================


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index.

    Args:
        close: Closing prices.
        period: Look-back window (default 14).

    Returns:
        pd.Series of RSI values (0-100).
    """
    if _HAS_TALIB:
        result = _talib.RSI(close.values, timeperiod=period)
        return _to_series(result, index=close.index, name="rsi")

    if _HAS_PANDAS_TA:
        result = _pta.rsi(close, length=period)
        if result is not None:
            result.name = "rsi"
            return result

    # Pure-pandas fallback
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return _to_series(100.0 - 100.0 / (1.0 + rs), index=close.index, name="rsi")


def macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Moving Average Convergence/Divergence.

    Args:
        close: Closing prices.
        fast: Fast EMA period (default 12).
        slow: Slow EMA period (default 26).
        signal: Signal line EMA period (default 9).

    Returns:
        Tuple of (DIF, DEA, histogram) as pd.Series.
    """
    if _HAS_TALIB:
        dif, dea, hist = _talib.MACD(close.values, fastperiod=fast, slowperiod=slow, signalperiod=signal)
        return (
            _to_series(dif, close.index, "macd_dif"),
            _to_series(dea, close.index, "macd_dea"),
            _to_series(hist, close.index, "macd_hist"),
        )

    if _HAS_PANDAS_TA:
        df = _pta.macd(close, fast=fast, slow=slow, signal=signal)
        if df is not None and not df.empty:
            cols = df.columns.tolist()
            return (
                _to_series(df.iloc[:, 0], close.index, "macd_dif"),
                _to_series(df.iloc[:, 2], close.index, "macd_dea"),
                _to_series(df.iloc[:, 1], close.index, "macd_hist"),
            )

    # Pure-pandas fallback
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif_s = ema_fast - ema_slow
    dea_s = dif_s.ewm(span=signal, adjust=False).mean()
    hist_s = dif_s - dea_s
    return (
        _to_series(dif_s, close.index, "macd_dif"),
        _to_series(dea_s, close.index, "macd_dea"),
        _to_series(hist_s, close.index, "macd_hist"),
    )


def roc(close: pd.Series, period: int = 10) -> pd.Series:
    """Rate of Change (percentage).

    Args:
        close: Closing prices.
        period: Look-back window (default 10).

    Returns:
        pd.Series of ROC values.
    """
    if _HAS_TALIB:
        result = _talib.ROC(close.values, timeperiod=period)
        return _to_series(result, close.index, "roc")

    if _HAS_PANDAS_TA:
        result = _pta.roc(close, length=period)
        if result is not None:
            result.name = "roc"
            return result

    result = (close - close.shift(period)) / close.shift(period) * 100.0
    return _to_series(result, close.index, "roc")


def stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k_period: int = 14,
    d_period: int = 3,
) -> tuple[pd.Series, pd.Series]:
    """Stochastic Oscillator (%K, %D).

    Args:
        high: High prices.
        low: Low prices.
        close: Closing prices.
        k_period: %K look-back (default 14).
        d_period: %D smoothing period (default 3).

    Returns:
        Tuple of (stoch_k, stoch_d) pd.Series.
    """
    if _HAS_TALIB:
        k, d = _talib.STOCH(
            high.values, low.values, close.values,
            fastk_period=k_period, slowk_period=d_period, slowd_period=d_period,
        )
        return _to_series(k, close.index, "stoch_k"), _to_series(d, close.index, "stoch_d")

    if _HAS_PANDAS_TA:
        df = _pta.stoch(high, low, close, k=k_period, d=d_period)
        if df is not None and not df.empty:
            return (
                _to_series(df.iloc[:, 0], close.index, "stoch_k"),
                _to_series(df.iloc[:, 1], close.index, "stoch_d"),
            )

    # Pure fallback
    lowest_low = low.rolling(window=k_period, min_periods=k_period).min()
    highest_high = high.rolling(window=k_period, min_periods=k_period).max()
    k_s = 100.0 * (close - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)
    d_s = k_s.rolling(window=d_period, min_periods=d_period).mean()
    return _to_series(k_s, close.index, "stoch_k"), _to_series(d_s, close.index, "stoch_d")


def williams_r(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Williams %R.

    Args:
        high: High prices.
        low: Low prices.
        close: Closing prices.
        period: Look-back (default 14).

    Returns:
        pd.Series of Williams %R values (-100 to 0).
    """
    if _HAS_TALIB:
        result = _talib.WILLR(high.values, low.values, close.values, timeperiod=period)
        return _to_series(result, close.index, "williams_r")

    if _HAS_PANDAS_TA:
        result = _pta.willr(high, low, close, length=period)
        if result is not None:
            result.name = "williams_r"
            return result

    highest = high.rolling(window=period, min_periods=period).max()
    lowest = low.rolling(window=period, min_periods=period).min()
    wr = -100.0 * (highest - close) / (highest - lowest).replace(0, np.nan)
    return _to_series(wr, close.index, "williams_r")


def cci(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 20,
) -> pd.Series:
    """Commodity Channel Index.

    Args:
        high: High prices.
        low: Low prices.
        close: Closing prices.
        period: Look-back (default 20).

    Returns:
        pd.Series of CCI values.
    """
    if _HAS_TALIB:
        result = _talib.CCI(high.values, low.values, close.values, timeperiod=period)
        return _to_series(result, close.index, "cci")

    if _HAS_PANDAS_TA:
        result = _pta.cci(high, low, close, length=period)
        if result is not None:
            result.name = "cci"
            return result

    tp = (high + low + close) / 3.0
    tp_sma = tp.rolling(window=period, min_periods=period).mean()
    tp_mad = tp.rolling(window=period, min_periods=period).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
    )
    result_s = (tp - tp_sma) / (0.015 * tp_mad).replace(0, np.nan)
    return _to_series(result_s, close.index, "cci")


# ======================================================================
# Trend indicators
# ======================================================================


def sma(close: pd.Series, period: int = 20) -> pd.Series:
    """Simple Moving Average.

    Args:
        close: Input series (typically closing prices).
        period: Window size (default 20).

    Returns:
        pd.Series of SMA values.
    """
    if _HAS_TALIB:
        result = _talib.SMA(close.values, timeperiod=period)
        return _to_series(result, close.index, "sma")

    return _to_series(
        close.rolling(window=period, min_periods=period).mean(),
        close.index,
        "sma",
    )


def ema(close: pd.Series, period: int = 20) -> pd.Series:
    """Exponential Moving Average.

    Args:
        close: Input series (typically closing prices).
        period: Span (default 20).

    Returns:
        pd.Series of EMA values.
    """
    if _HAS_TALIB:
        result = _talib.EMA(close.values, timeperiod=period)
        return _to_series(result, close.index, "ema")

    return _to_series(
        close.ewm(span=period, adjust=False).mean(),
        close.index,
        "ema",
    )


def adx(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Average Directional Index.

    Args:
        high: High prices.
        low: Low prices.
        close: Closing prices.
        period: Look-back (default 14).

    Returns:
        pd.Series of ADX values.
    """
    if _HAS_TALIB:
        result = _talib.ADX(high.values, low.values, close.values, timeperiod=period)
        return _to_series(result, close.index, "adx")

    if _HAS_PANDAS_TA:
        df = _pta.adx(high, low, close, length=period)
        if df is not None and not df.empty:
            # First column is ADX
            return _to_series(df.iloc[:, 0], close.index, "adx")

    # Pure fallback
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    atr_vals = atr(high, low, close, period)
    atr_safe = atr_vals.replace(0, np.nan)

    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean() / atr_safe
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean() / atr_safe

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_s = dx.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    return _to_series(adx_s, close.index, "adx")


def aroon(
    high: pd.Series,
    low: pd.Series,
    period: int = 25,
) -> tuple[pd.Series, pd.Series]:
    """Aroon Up / Down.

    Args:
        high: High prices.
        low: Low prices.
        period: Look-back window (default 25).

    Returns:
        Tuple of (aroon_up, aroon_down) pd.Series.
    """
    if _HAS_TALIB:
        down, up = _talib.AROON(high.values, low.values, timeperiod=period)
        return _to_series(up, high.index, "aroon_up"), _to_series(down, high.index, "aroon_down")

    if _HAS_PANDAS_TA:
        df = _pta.aroon(high, low, length=period)
        if df is not None and not df.empty:
            return (
                _to_series(df.iloc[:, 0], high.index, "aroon_up"),
                _to_series(df.iloc[:, 1], high.index, "aroon_down"),
            )

    # Pure fallback
    aroon_up_vals = high.rolling(window=period + 1, min_periods=period + 1).apply(
        lambda x: 100.0 * (period - (period - np.argmax(x))) / period, raw=True
    )
    aroon_down_vals = low.rolling(window=period + 1, min_periods=period + 1).apply(
        lambda x: 100.0 * (period - (period - np.argmin(x))) / period, raw=True
    )
    return (
        _to_series(aroon_up_vals, high.index, "aroon_up"),
        _to_series(aroon_down_vals, low.index, "aroon_down"),
    )


# ======================================================================
# Volatility indicators
# ======================================================================


def atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Average True Range.

    Args:
        high: High prices.
        low: Low prices.
        close: Closing prices.
        period: Look-back (default 14).

    Returns:
        pd.Series of ATR values.
    """
    if _HAS_TALIB:
        result = _talib.ATR(high.values, low.values, close.values, timeperiod=period)
        return _to_series(result, close.index, "atr")

    if _HAS_PANDAS_TA:
        result = _pta.atr(high, low, close, length=period)
        if result is not None:
            result.name = "atr"
            return result

    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return _to_series(
        tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean(),
        close.index,
        "atr",
    )


def bbands(
    close: pd.Series,
    period: int = 20,
    nbdev: float = 2.0,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger Bands.

    Args:
        close: Closing prices.
        period: SMA window (default 20).
        nbdev: Standard deviation multiplier (default 2.0).

    Returns:
        Tuple of (upper, middle, lower) pd.Series.
    """
    if _HAS_TALIB:
        upper, mid, lower = _talib.BBANDS(close.values, timeperiod=period, nbdevup=nbdev, nbdevdn=nbdev)
        return (
            _to_series(upper, close.index, "bb_upper"),
            _to_series(mid, close.index, "bb_middle"),
            _to_series(lower, close.index, "bb_lower"),
        )

    if _HAS_PANDAS_TA:
        df = _pta.bbands(close, length=period, std=nbdev)
        if df is not None and not df.empty:
            return (
                _to_series(df.iloc[:, 2], close.index, "bb_upper"),
                _to_series(df.iloc[:, 1], close.index, "bb_middle"),
                _to_series(df.iloc[:, 0], close.index, "bb_lower"),
            )

    mid_s = close.rolling(window=period, min_periods=period).mean()
    std_s = close.rolling(window=period, min_periods=period).std()
    upper_s = mid_s + nbdev * std_s
    lower_s = mid_s - nbdev * std_s
    return (
        _to_series(upper_s, close.index, "bb_upper"),
        _to_series(mid_s, close.index, "bb_middle"),
        _to_series(lower_s, close.index, "bb_lower"),
    )


def realized_vol(close: pd.Series, period: int = 20) -> pd.Series:
    """Realized (historical) volatility as annualised standard deviation of log returns.

    Args:
        close: Closing prices.
        period: Look-back window (default 20).

    Returns:
        pd.Series of annualised volatility.
    """
    log_ret = np.log(close / close.shift(1))
    vol = log_ret.rolling(window=period, min_periods=period).std() * np.sqrt(252)
    return _to_series(vol, close.index, "realized_vol")


# ======================================================================
# Volume indicators
# ======================================================================


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """On-Balance Volume.

    Args:
        close: Closing prices.
        volume: Trading volume.

    Returns:
        pd.Series of OBV values.
    """
    if _HAS_TALIB:
        result = _talib.OBV(close.values, volume.values.astype(float))
        return _to_series(result, close.index, "obv")

    if _HAS_PANDAS_TA:
        result = _pta.obv(close, volume)
        if result is not None:
            result.name = "obv"
            return result

    direction = np.sign(close.diff())
    obv_s = (direction * volume).fillna(0).cumsum()
    return _to_series(obv_s, close.index, "obv")


def mfi(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Money Flow Index.

    Args:
        high: High prices.
        low: Low prices.
        close: Closing prices.
        volume: Trading volume.
        period: Look-back (default 14).

    Returns:
        pd.Series of MFI values (0-100).
    """
    if _HAS_TALIB:
        result = _talib.MFI(high.values, low.values, close.values, volume.values.astype(float), timeperiod=period)
        return _to_series(result, close.index, "mfi")

    if _HAS_PANDAS_TA:
        result = _pta.mfi(high, low, close, volume, length=period)
        if result is not None:
            result.name = "mfi"
            return result

    # Pure fallback
    tp = (high + low + close) / 3.0
    raw_mf = tp * volume
    direction = tp.diff()
    pos_mf = raw_mf.where(direction > 0, 0.0).rolling(window=period, min_periods=period).sum()
    neg_mf = raw_mf.where(direction <= 0, 0.0).rolling(window=period, min_periods=period).sum()
    mr = pos_mf / neg_mf.replace(0, np.nan)
    mfi_s = 100.0 - 100.0 / (1.0 + mr)
    return _to_series(mfi_s, close.index, "mfi")


# ======================================================================
# Statistical indicators
# ======================================================================


def zscore(close: pd.Series, period: int = 20) -> pd.Series:
    """Rolling Z-score of price.

    Args:
        close: Closing prices.
        period: Look-back window (default 20).

    Returns:
        pd.Series of z-score values.
    """
    mean = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std()
    z = (close - mean) / std.replace(0, np.nan)
    return _to_series(z, close.index, "zscore")


def linreg_slope(close: pd.Series, period: int = 20) -> pd.Series:
    """Rolling linear regression slope.

    Args:
        close: Closing prices.
        period: Look-back window (default 20).

    Returns:
        pd.Series of slope values (per bar).
    """
    if _HAS_TALIB:
        result = _talib.LINEARREG_SLOPE(close.values, timeperiod=period)
        return _to_series(result, close.index, "linreg_slope")

    # Pure fallback using least-squares
    x = np.arange(period, dtype=float)
    x_mean = x.mean()
    x_var = ((x - x_mean) ** 2).sum()

    def _slope(window: np.ndarray) -> float:
        if len(window) < period:
            return np.nan
        y_mean = np.mean(window)
        return float(np.sum((x - x_mean) * (window - y_mean)) / x_var)

    result_s = close.rolling(window=period, min_periods=period).apply(_slope, raw=True)
    return _to_series(result_s, close.index, "linreg_slope")
