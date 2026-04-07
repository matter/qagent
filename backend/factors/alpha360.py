"""Qlib Alpha360 factors adapted for the QAgent factor library.

Alpha360 in Qlib creates features from OHLCV data using lookback windows.
This module reproduces the core factor patterns using:
- Price return ratios (close_d / close_0) at multiple horizons
- Price field ratios (open/close, high/close, low/close) at multiple horizons
- Volume ratios (volume_d / volume_0) at multiple horizons
- K-line shape features (KMID, KLEN, KUP, KLOW, KSFT)
- Rolling statistical features (std, mean, max, min, quantile, correlation)

VWAP is NOT available in local data and is skipped.

All factors use the "统计_" (statistical) prefix to distinguish them.
"""

from __future__ import annotations

ALPHA360_TEMPLATES: dict[str, str] = {}

# ------------------------------------------------------------------
# Helper: periods used across Alpha360
# Qlib Alpha360 uses d=0..59; we use representative periods
# ------------------------------------------------------------------
_RETURN_PERIODS = [1, 2, 3, 5, 10, 20, 30, 60]
_STAT_PERIODS = [5, 10, 20, 60]

# ==================================================================
# 1. CLOSE return ratios: close_{t-d} / close_t - 1
# ==================================================================
for d in _RETURN_PERIODS:
    name = f"统计_CLOSE_ROC_{d}"
    cls_name = f"AlphaCloseROC{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day close return ratio (close_{{t-{d}}} / close_t - 1)."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日收盘价收益率"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"].pct_change(periods=self.params["period"])
'''

# ==================================================================
# 2. OPEN / CLOSE ratios at various lags
# ==================================================================
for d in _RETURN_PERIODS:
    name = f"统计_OPEN_REF_{d}"
    cls_name = f"AlphaOpenRef{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: open_{{t-{d}}} / close_t ratio."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日前开盘价/当日收盘价"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        d = self.params["period"]
        return data["open"].shift(d) / data["close"] - 1
'''

# ==================================================================
# 3. HIGH / CLOSE ratios at various lags
# ==================================================================
for d in _RETURN_PERIODS:
    name = f"统计_HIGH_REF_{d}"
    cls_name = f"AlphaHighRef{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: high_{{t-{d}}} / close_t ratio."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日前最高价/当日收盘价"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        d = self.params["period"]
        return data["high"].shift(d) / data["close"] - 1
'''

# ==================================================================
# 4. LOW / CLOSE ratios at various lags
# ==================================================================
for d in _RETURN_PERIODS:
    name = f"统计_LOW_REF_{d}"
    cls_name = f"AlphaLowRef{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: low_{{t-{d}}} / close_t ratio."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日前最低价/当日收盘价"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        d = self.params["period"]
        return data["low"].shift(d) / data["close"] - 1
'''

# ==================================================================
# 5. VOLUME ratios at various lags: volume_{t-d} / volume_t
# ==================================================================
for d in _RETURN_PERIODS:
    name = f"统计_VOL_REF_{d}"
    cls_name = f"AlphaVolRef{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: volume_{{t-{d}}} / volume_t ratio."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日前成交量/当日成交量"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        vol = data["volume"].astype(float)
        d = self.params["period"]
        return vol.shift(d) / vol.replace(0, np.nan)
'''

# ==================================================================
# 6. K-line shape features (current day)
# ==================================================================
ALPHA360_TEMPLATES["统计_KMID"] = '''\
"""Alpha360 K-line: KMID = (close - open) / open."""

import pandas as pd
from backend.factors.base import FactorBase


class AlphaKMID(FactorBase):
    name = "统计_KMID"
    description = "Alpha360 K线中间体 (close-open)/open"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return (data["close"] - data["open"]) / data["open"]
'''

ALPHA360_TEMPLATES["统计_KLEN"] = '''\
"""Alpha360 K-line: KLEN = (high - low) / open."""

import pandas as pd
from backend.factors.base import FactorBase


class AlphaKLEN(FactorBase):
    name = "统计_KLEN"
    description = "Alpha360 K线长度 (high-low)/open"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return (data["high"] - data["low"]) / data["open"]
'''

ALPHA360_TEMPLATES["统计_KMID2"] = '''\
"""Alpha360 K-line: KMID2 = (close - open) / (high - low)."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class AlphaKMID2(FactorBase):
    name = "统计_KMID2"
    description = "Alpha360 K线中间体占比 (close-open)/(high-low)"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        hl = (data["high"] - data["low"]).replace(0, np.nan)
        return (data["close"] - data["open"]) / hl
'''

ALPHA360_TEMPLATES["统计_KUP"] = '''\
"""Alpha360 K-line: KUP = (high - max(open,close)) / open."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class AlphaKUP(FactorBase):
    name = "统计_KUP"
    description = "Alpha360 上影线 (high-max(open,close))/open"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        upper = data["high"] - np.maximum(data["open"], data["close"])
        return upper / data["open"]
'''

ALPHA360_TEMPLATES["统计_KUP2"] = '''\
"""Alpha360 K-line: KUP2 = (high - max(open,close)) / (high - low)."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class AlphaKUP2(FactorBase):
    name = "统计_KUP2"
    description = "Alpha360 上影线占比 (high-max(open,close))/(high-low)"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        upper = data["high"] - np.maximum(data["open"], data["close"])
        hl = (data["high"] - data["low"]).replace(0, np.nan)
        return upper / hl
'''

ALPHA360_TEMPLATES["统计_KLOW"] = '''\
"""Alpha360 K-line: KLOW = (min(open,close) - low) / open."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class AlphaKLOW(FactorBase):
    name = "统计_KLOW"
    description = "Alpha360 下影线 (min(open,close)-low)/open"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        lower = np.minimum(data["open"], data["close"]) - data["low"]
        return lower / data["open"]
'''

ALPHA360_TEMPLATES["统计_KLOW2"] = '''\
"""Alpha360 K-line: KLOW2 = (min(open,close) - low) / (high - low)."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class AlphaKLOW2(FactorBase):
    name = "统计_KLOW2"
    description = "Alpha360 下影线占比 (min(open,close)-low)/(high-low)"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        lower = np.minimum(data["open"], data["close"]) - data["low"]
        hl = (data["high"] - data["low"]).replace(0, np.nan)
        return lower / hl
'''

ALPHA360_TEMPLATES["统计_KSFT"] = '''\
"""Alpha360 K-line: KSFT = (2*close - high - low) / open."""

import pandas as pd
from backend.factors.base import FactorBase


class AlphaKSFT(FactorBase):
    name = "统计_KSFT"
    description = "Alpha360 K线偏移 (2*close-high-low)/open"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return (2 * data["close"] - data["high"] - data["low"]) / data["open"]
'''

ALPHA360_TEMPLATES["统计_KSFT2"] = '''\
"""Alpha360 K-line: KSFT2 = (2*close - high - low) / (high - low)."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class AlphaKSFT2(FactorBase):
    name = "统计_KSFT2"
    description = "Alpha360 K线偏移占比 (2*close-high-low)/(high-low)"
    category = "statistical"
    params = {}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        hl = (data["high"] - data["low"]).replace(0, np.nan)
        return (2 * data["close"] - data["high"] - data["low"]) / hl
'''

# ==================================================================
# 7. Rolling STD of returns
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_ROC_STD_{d}"
    cls_name = f"AlphaRocStd{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling std of daily returns."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日收益率滚动标准差"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        ret = data["close"].pct_change()
        return ret.rolling(self.params["period"], min_periods=2).std()
'''

# ==================================================================
# 8. Rolling MEAN of returns
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_ROC_MEAN_{d}"
    cls_name = f"AlphaRocMean{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling mean of daily returns."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日收益率滚动均值"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        ret = data["close"].pct_change()
        return ret.rolling(self.params["period"], min_periods=2).mean()
'''

# ==================================================================
# 9. Rolling MAX / MIN of close (normalized)
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_MAX_{d}"
    cls_name = f"AlphaMax{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling max of close / current close - 1."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日滚动最高价偏离度"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        roll_max = data["close"].rolling(self.params["period"], min_periods=1).max()
        return roll_max / data["close"] - 1
'''

for d in _STAT_PERIODS:
    name = f"统计_MIN_{d}"
    cls_name = f"AlphaMin{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling min of close / current close - 1."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日滚动最低价偏离度"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        roll_min = data["close"].rolling(self.params["period"], min_periods=1).min()
        return roll_min / data["close"] - 1
'''

# ==================================================================
# 10. Rolling quantile of close (median position)
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_QTLU_{d}"
    cls_name = f"AlphaQtlu{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling 80th percentile of close / current close - 1."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日滚动80分位偏离度"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        q80 = data["close"].rolling(self.params["period"], min_periods=2).quantile(0.8)
        return q80 / data["close"] - 1
'''

for d in _STAT_PERIODS:
    name = f"统计_QTLD_{d}"
    cls_name = f"AlphaQtld{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling 20th percentile of close / current close - 1."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日滚动20分位偏离度"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        q20 = data["close"].rolling(self.params["period"], min_periods=2).quantile(0.2)
        return q20 / data["close"] - 1
'''

# ==================================================================
# 11. Rolling rank (percentile position within window)
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_RANK_{d}"
    cls_name = f"AlphaRank{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling rank percentile of close."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日滚动排名分位数"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        def _pct_rank(x):
            return x.rank(pct=True).iloc[-1] if len(x) > 0 else float("nan")
        return data["close"].rolling(self.params["period"], min_periods=2).apply(_pct_rank, raw=False)
'''

# ==================================================================
# 12. RSV (Raw Stochastic Value)
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_RSV_{d}"
    cls_name = f"AlphaRSV{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day RSV = (close - low_min) / (high_max - low_min)."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日RSV(未成熟随机值)"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        p = self.params["period"]
        low_min = data["low"].rolling(p, min_periods=1).min()
        high_max = data["high"].rolling(p, min_periods=1).max()
        denom = (high_max - low_min).replace(0, np.nan)
        return (data["close"] - low_min) / denom
'''

# ==================================================================
# 13. Price-volume correlation
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_CORR_CV_{d}"
    cls_name = f"AlphaCorrCV{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling correlation between close and volume."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日价量相关系数"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        return data["close"].rolling(self.params["period"], min_periods=3).corr(
            data["volume"].astype(float)
        )
'''

# ==================================================================
# 14. Volume rolling std (normalized)
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_VSTD_{d}"
    cls_name = f"AlphaVstd{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: {d}-day rolling std of volume / mean volume."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {d}日成交量滚动变异系数"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        vol = data["volume"].astype(float)
        std = vol.rolling(self.params["period"], min_periods=2).std()
        mean = vol.rolling(self.params["period"], min_periods=2).mean().replace(0, np.nan)
        return std / mean
'''

# ==================================================================
# 15. Volume rolling mean ratio (current vol / rolling mean)
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_VSMA_{d}"
    cls_name = f"AlphaVsma{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: volume / {d}-day mean volume - 1."""

import pandas as pd
import numpy as np
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 成交量/{d}日均量偏离"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        vol = data["volume"].astype(float)
        mean = vol.rolling(self.params["period"], min_periods=2).mean().replace(0, np.nan)
        return vol / mean - 1
'''

# ==================================================================
# 16. Close / SMA ratio
# ==================================================================
for d in _STAT_PERIODS:
    name = f"统计_CSMA_{d}"
    cls_name = f"AlphaCsma{d}"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: close / {d}-day SMA - 1 (price deviation from MA)."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 收盘价/{d}日均线偏离"
    category = "statistical"
    params = {{"period": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        sma = data["close"].rolling(self.params["period"], min_periods=1).mean()
        return data["close"] / sma - 1
'''

# ==================================================================
# 17. Intraday return (close / open - 1, at lag d)
# ==================================================================
for d in [0, 1, 5, 10, 20]:
    name = f"统计_IRET_{d}" if d > 0 else "统计_IRET_0"
    cls_name = f"AlphaIret{d}"
    desc_lag = f"{d}日前" if d > 0 else "当日"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: intraday return at lag {d} = (close_{{t-{d}}} / open_{{t-{d}}}) - 1."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {desc_lag}日内收益率"
    category = "statistical"
    params = {{"lag": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        lag = self.params["lag"]
        iret = data["close"] / data["open"] - 1
        if lag > 0:
            iret = iret.shift(lag)
        return iret
'''

# ==================================================================
# 18. Overnight return (open_t / close_{t-1} - 1, at lag d)
# ==================================================================
for d in [0, 1, 5, 10, 20]:
    name = f"统计_ORET_{d}" if d > 0 else "统计_ORET_0"
    cls_name = f"AlphaOret{d}"
    desc_lag = f"{d}日前" if d > 0 else "当日"
    ALPHA360_TEMPLATES[name] = f'''\
"""Alpha360: overnight return at lag {d} = open_t / close_{{t-1}} - 1."""

import pandas as pd
from backend.factors.base import FactorBase


class {cls_name}(FactorBase):
    name = "{name}"
    description = "Alpha360 {desc_lag}隔夜收益率"
    category = "statistical"
    params = {{"lag": {d}}}

    def compute(self, data: pd.DataFrame) -> pd.Series:
        lag = self.params["lag"]
        oret = data["open"] / data["close"].shift(1) - 1
        if lag > 0:
            oret = oret.shift(lag)
        return oret
'''

# ==================================================================
# Summary / skipped
# ==================================================================
# VWAP factors: SKIPPED - local data does not have VWAP field.
# Full 60-day individual lookback: SKIPPED - using representative
#   periods [1,2,3,5,10,20,30,60] instead of d=0..59 to keep
#   factor count manageable.

_SKIPPED_FACTORS = [
    "VWAP_REF_d (d=0..59) — 本地数据无VWAP字段",
    "个别逐日回望 (d=0..59全量) — 已用代表性周期 [1,2,3,5,10,20,30,60] 替代",
]


def get_alpha360_template_names() -> list[str]:
    return sorted(ALPHA360_TEMPLATES.keys())


def get_alpha360_template_source(name: str) -> str | None:
    return ALPHA360_TEMPLATES.get(name)


def get_skipped_factors() -> list[str]:
    return list(_SKIPPED_FACTORS)
