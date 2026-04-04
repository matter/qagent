"""Technical indicator adapter with TA-Lib / pandas-ta / numpy fallback.

Usage::

    from backend.indicators import ta

    ta.rsi(close, period=14)
    ta.macd(close)
    ta.sma(close, period=20)
"""

from backend.indicators.adapter import (  # noqa: F401
    adx,
    aroon,
    atr,
    bbands,
    cci,
    ema,
    linreg_slope,
    macd,
    mfi,
    obv,
    realized_vol,
    roc,
    rsi,
    sma,
    stochastic,
    williams_r,
    zscore,
)

# Expose the module itself as ``ta`` for convenience:
#   from backend.indicators import ta
#   ta.rsi(close)
import backend.indicators.adapter as ta  # noqa: E402, F811

__all__ = [
    "ta",
    "adx",
    "aroon",
    "atr",
    "bbands",
    "cci",
    "ema",
    "linreg_slope",
    "macd",
    "mfi",
    "obv",
    "realized_vol",
    "roc",
    "rsi",
    "sma",
    "stochastic",
    "williams_r",
    "zscore",
]
