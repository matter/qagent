"""Built-in factor templates as source-code strings.

Each template is a complete, runnable Python file that:
- Imports from ``backend.indicators``
- Inherits from ``FactorBase``
- Has clear comments

These are stored as plain strings so users can view and modify them.
"""

from __future__ import annotations

from backend.factors.alpha360 import ALPHA360_TEMPLATES

TEMPLATES: dict[str, str] = {}

# ------------------------------------------------------------------
# 1. Momentum_20 – 20-day Rate of Change
# ------------------------------------------------------------------
TEMPLATES["Momentum_20"] = '''\
"""Momentum factor: 20-day Rate of Change (ROC)."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class Momentum20(FactorBase):
    name = "Momentum_20"
    description = "20-day rate of change (percentage)"
    category = "momentum"
    params = {"period": 20}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.roc(data["close"], period=self.params["period"])
'''

# ------------------------------------------------------------------
# 2. RSI_14
# ------------------------------------------------------------------
TEMPLATES["RSI_14"] = '''\
"""RSI factor: 14-period Relative Strength Index."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class RSI14(FactorBase):
    name = "RSI_14"
    description = "14-period Relative Strength Index"
    category = "momentum"
    params = {"period": 14}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.rsi(data["close"], period=self.params["period"])
'''

# ------------------------------------------------------------------
# 3. MACD_Signal
# ------------------------------------------------------------------
TEMPLATES["MACD_Signal"] = '''\
"""MACD Signal factor: MACD histogram (DIF - DEA)."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class MACDSignal(FactorBase):
    name = "MACD_Signal"
    description = "MACD histogram (DIF minus DEA signal line)"
    category = "momentum"
    params = {"fast": 12, "slow": 26, "signal": 9}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        _dif, _dea, histogram = ta.macd(
            data["close"],
            fast=self.params["fast"],
            slow=self.params["slow"],
            signal=self.params["signal"],
        )
        return histogram
'''

# ------------------------------------------------------------------
# 4. ADX_14
# ------------------------------------------------------------------
TEMPLATES["ADX_14"] = '''\
"""ADX factor: 14-period Average Directional Index."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class ADX14(FactorBase):
    name = "ADX_14"
    description = "14-period Average Directional Index (trend strength)"
    category = "trend"
    params = {"period": 14}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.adx(data["high"], data["low"], data["close"],
                       period=self.params["period"])
'''

# ------------------------------------------------------------------
# 5. Volatility_20 – 20-day realized volatility
# ------------------------------------------------------------------
TEMPLATES["Volatility_20"] = '''\
"""Volatility factor: 20-day annualised realised volatility."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class Volatility20(FactorBase):
    name = "Volatility_20"
    description = "20-day annualised realised volatility of log returns"
    category = "volatility"
    params = {"period": 20}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.realized_vol(data["close"], period=self.params["period"])
'''

# ------------------------------------------------------------------
# 6. ATR_14
# ------------------------------------------------------------------
TEMPLATES["ATR_14"] = '''\
"""ATR factor: 14-period Average True Range."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class ATR14(FactorBase):
    name = "ATR_14"
    description = "14-period Average True Range"
    category = "volatility"
    params = {"period": 14}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.atr(data["high"], data["low"], data["close"],
                       period=self.params["period"])
'''

# ------------------------------------------------------------------
# 7. BBands_Width_20 – Bollinger Band width
# ------------------------------------------------------------------
TEMPLATES["BBands_Width_20"] = '''\
"""Bollinger Band Width factor: (upper - lower) / middle."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase
from backend.indicators import ta


class BBandsWidth20(FactorBase):
    name = "BBands_Width_20"
    description = "Bollinger Band width: (upper - lower) / middle"
    category = "volatility"
    params = {"period": 20, "nbdev": 2.0}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        upper, middle, lower = ta.bbands(
            data["close"],
            period=self.params["period"],
            nbdev=self.params["nbdev"],
        )
        width = (upper - lower) / middle.replace(0, np.nan)
        width.name = "bbands_width"
        return width
'''

# ------------------------------------------------------------------
# 8. Volume_Ratio_10 – volume relative to 10-day average
# ------------------------------------------------------------------
TEMPLATES["Volume_Ratio_10"] = '''\
"""Volume Ratio factor: today\'s volume / 10-day SMA of volume."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase
from backend.indicators import ta


class VolumeRatio10(FactorBase):
    name = "Volume_Ratio_10"
    description = "Volume divided by its 10-day simple moving average"
    category = "volume"
    params = {"period": 10}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        vol = data["volume"].astype(float)
        avg_vol = ta.sma(vol, period=self.params["period"])
        ratio = vol / avg_vol.replace(0, np.nan)
        ratio.name = "volume_ratio"
        return ratio
'''

# ------------------------------------------------------------------
# 9. OBV_Slope_20 – slope of OBV over 20 days
# ------------------------------------------------------------------
TEMPLATES["OBV_Slope_20"] = '''\
"""OBV Slope factor: linear-regression slope of OBV over 20 days."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class OBVSlope20(FactorBase):
    name = "OBV_Slope_20"
    description = "20-day linear regression slope of On-Balance Volume"
    category = "volume"
    params = {"period": 20}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        obv_vals = ta.obv(data["close"], data["volume"])
        slope = ta.linreg_slope(obv_vals, period=self.params["period"])
        slope.name = "obv_slope"
        return slope
'''

# ------------------------------------------------------------------
# 10. MFI_14
# ------------------------------------------------------------------
TEMPLATES["MFI_14"] = '''\
"""MFI factor: 14-period Money Flow Index."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class MFI14(FactorBase):
    name = "MFI_14"
    description = "14-period Money Flow Index"
    category = "volume"
    params = {"period": 14}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.mfi(data["high"], data["low"], data["close"],
                       data["volume"], period=self.params["period"])
'''

# ------------------------------------------------------------------
# 11. ZScore_20
# ------------------------------------------------------------------
TEMPLATES["ZScore_20"] = '''\
"""Z-Score factor: 20-day rolling z-score of closing price."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class ZScore20(FactorBase):
    name = "ZScore_20"
    description = "20-day rolling z-score of closing price"
    category = "statistical"
    params = {"period": 20}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.zscore(data["close"], period=self.params["period"])
'''

# ------------------------------------------------------------------
# 12. LinReg_Slope_20
# ------------------------------------------------------------------
TEMPLATES["LinReg_Slope_20"] = '''\
"""Linear Regression Slope factor: 20-day slope of closing price."""

import pandas as pd
from backend.factors.base import FactorBase
from backend.indicators import ta


class LinRegSlope20(FactorBase):
    name = "LinReg_Slope_20"
    description = "20-day linear regression slope of closing price"
    category = "statistical"
    params = {"period": 20}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return ta.linreg_slope(data["close"], period=self.params["period"])
'''


# Merge Alpha360 templates
TEMPLATES.update(ALPHA360_TEMPLATES)


def get_template_names() -> list[str]:
    """Return sorted list of all available template names."""
    return sorted(TEMPLATES.keys())


def get_template_source(name: str) -> str | None:
    """Return source code for the named template, or None if not found."""
    return TEMPLATES.get(name)
