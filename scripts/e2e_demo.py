#!/usr/bin/env python3
"""QAgent 端到端示例流程 - 从因子到信号的完整演示"""

import json
import time
import requests

BASE = "http://127.0.0.1:8000"
s = requests.Session()


def api(method, path, **kwargs):
    resp = getattr(s, method)(f"{BASE}{path}", **kwargs)
    if not resp.ok:
        print(f"  API error: {resp.status_code} {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json()


def api_safe(method, path, **kwargs):
    """Like api() but returns None on error instead of raising."""
    resp = getattr(s, method)(f"{BASE}{path}", **kwargs)
    if not resp.ok:
        return None
    return resp.json()


def wait_task(task_id, label="", timeout=300):
    for _ in range(timeout // 2):
        try:
            resp = s.get(f"{BASE}/api/tasks/{task_id}", timeout=10)
            t = resp.json()
            status = t.get("status", "unknown")
        except Exception:
            time.sleep(2)
            continue
        if status == "completed":
            print(f"  ✓ {label}完成")
            return t
        if status == "failed":
            print(f"  ✗ {label}失败: {(t.get('error') or '')[:300]}")
            raise RuntimeError(f"{label}失败")
        if status == "timeout":
            print(f"  ✗ {label}超时")
            raise RuntimeError(f"{label}超时")
        time.sleep(2)
    raise TimeoutError(f"{label}等待超时")


print("=" * 60)
print("  QAgent 端到端示例流程")
print("=" * 60)

# --- Step 1: 创建因子 ---
print("\n--- Step 1: 创建因子 (Momentum_20) ---")
template = api("get", "/api/factors/templates/Momentum_20")

# Check if already exists
existing = api("get", "/api/factors")
factor = next((f for f in existing if f["name"] == "Demo_Momentum_20"), None)
if factor:
    FACTOR_ID = factor["id"]
    print(f"  因子已存在，复用: {FACTOR_ID}")
else:
    factor = api("post", "/api/factors", json={
        "name": "Demo_Momentum_20",
        "description": "20日动量因子示例",
        "category": "momentum",
        "source_code": template["source_code"],
    })
    FACTOR_ID = factor["id"]
print(f"  因子ID: {FACTOR_ID}")
print(f"  名称: {factor['name']}, 版本: {factor['version']}, 状态: {factor['status']}")

# --- Step 2: 评价因子 ---
print("\n--- Step 2: 评价因子 (vs fwd_return_5d) ---")
labels = api("get", "/api/labels")
label = next(l for l in labels if l["name"] == "fwd_return_5d")
LABEL_ID = label["id"]
print(f"  标签: {label['name']} (horizon={label['horizon']}d)")

result = api("post", f"/api/factors/{FACTOR_ID}/evaluate", json={
    "label_id": LABEL_ID,
    "universe_group_id": "test20",
    "start_date": "2024-01-01",
    "end_date": "2025-12-31",
})
wait_task(result["task_id"], "因子评价")

evals = api("get", f"/api/factors/{FACTOR_ID}/evaluations")
if evals:
    detail = api("get", f"/api/factors/evaluations/{evals[0]['id']}")
    sm = detail["summary"]
    print(f"  IC均值:     {sm['ic_mean']:.4f}")
    print(f"  IR:         {sm['ir']:.4f}")
    print(f"  IC胜率:     {sm['ic_win_rate']:.2%}")
    print(f"  多空年化:   {sm['long_short_annual_return']:.2%}")
    print(f"  换手率:     {sm['turnover']:.4f}")
    print(f"  覆盖率:     {sm['coverage']:.2%}")
    print(f"  IC序列点数: {len(detail.get('ic_series', []))}")

# --- Step 3: 创建特征集 ---
print("\n--- Step 3: 创建特征集 ---")
existing_fs = api("get", "/api/feature-sets")
fs = next((f for f in existing_fs if f["name"] == "Demo_FeatureSet"), None)
if fs:
    FS_ID = fs["id"]
    print(f"  特征集已存在，复用: {FS_ID}")
else:
    fs = api("post", "/api/feature-sets", json={
        "name": "Demo_FeatureSet",
        "description": "单因子特征集示例",
        "factor_refs": [{"factor_id": FACTOR_ID, "factor_name": "Demo_Momentum_20", "version": 1}],
        "preprocessing": {"missing": "forward_fill", "outlier": "mad", "normalize": "rank", "neutralize": None},
    })
    FS_ID = fs["id"]
print(f"  特征集ID: {FS_ID}")

# --- Step 4: 训练模型 ---
print("\n--- Step 4: 训练模型 (LightGBM) ---")
result = api("post", "/api/models/train", json={
    "name": "Demo_LightGBM",
    "feature_set_id": FS_ID,
    "label_id": LABEL_ID,
    "model_type": "lightgbm",
    "model_params": {"n_estimators": 100, "max_depth": 5, "learning_rate": 0.05},
    "train_config": {
        "method": "single_split",
        "train_start": "2022-01-03",
        "train_end": "2024-12-31",
        "valid_start": "2025-01-02",
        "valid_end": "2025-06-30",
        "test_start": "2025-07-01",
        "test_end": "2025-12-31",
        "purge_gap": 5,
    },
    "universe_group_id": "test20",
})
wait_task(result["task_id"], "模型训练", timeout=600)

models = api("get", "/api/models")
if models:
    MODEL_ID = models[0]["id"]
    model_detail = api("get", f"/api/models/{MODEL_ID}")
    metrics = model_detail.get("eval_metrics", {})
    print(f"  模型ID: {MODEL_ID}")
    if metrics:
        print(f"  测试IC:   {metrics.get('test_ic', 'N/A')}")
        print(f"  测试RMSE: {metrics.get('test_rmse', 'N/A')}")
        print(f"  IR:       {metrics.get('ir', 'N/A')}")

# --- Step 5: 创建策略 ---
print("\n--- Step 5: 创建策略 (动量Top10) ---")
strategy_code = '''
from backend.strategies.base import StrategyBase, StrategyContext
import pandas as pd

class DemoTopNStrategy(StrategyBase):
    name = "Demo_TopN"
    description = "买入20日动量因子排名前10的股票"

    def required_factors(self):
        return ["Demo_Momentum_20"]

    def required_models(self):
        return []

    def generate_signals(self, context):
        factor_df = context.factor_values.get("Demo_Momentum_20")
        if factor_df is None or factor_df.empty:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        cur = context.current_date
        if cur not in factor_df.index:
            dates = factor_df.index[factor_df.index <= cur]
            if len(dates) == 0:
                return pd.DataFrame(columns=["signal", "weight", "strength"])
            cur = dates[-1]

        row = factor_df.loc[cur].dropna()
        if len(row) == 0:
            return pd.DataFrame(columns=["signal", "weight", "strength"])

        ranked = row.sort_values(ascending=False)
        top_n = min(10, len(ranked))
        top_tickers = ranked.head(top_n)

        records = []
        for ticker in ranked.index:
            if ticker in top_tickers.index:
                records.append({"signal": 1, "weight": 1.0 / top_n, "strength": float(ranked[ticker])})
            else:
                records.append({"signal": 0, "weight": 0.0, "strength": float(ranked[ticker])})

        return pd.DataFrame(records, index=ranked.index)
'''

existing_strats = api("get", "/api/strategies")
strat = next((st for st in existing_strats if st["name"] == "Demo_TopN"), None)
if strat:
    STRAT_ID = strat["id"]
    print(f"  策略已存在，复用: {STRAT_ID}")
else:
    strat = api("post", "/api/strategies", json={
        "name": "Demo_TopN",
        "description": "买入动量前10",
        "source_code": strategy_code,
        "position_sizing": "equal_weight",
    })
    STRAT_ID = strat["id"]
print(f"  策略ID: {STRAT_ID}")
print(f"  版本: {strat['version']}, 状态: {strat['status']}")

# --- Step 6: 回测策略 ---
print("\n--- Step 6: 回测策略 (2025年) ---")
result = api("post", f"/api/strategies/{STRAT_ID}/backtest", json={
    "config": {
        "initial_capital": 1000000,
        "start_date": "2025-01-02",
        "end_date": "2025-12-31",
        "benchmark": "SPY",
        "commission_rate": 0.001,
        "slippage_rate": 0.001,
        "max_positions": 10,
        "rebalance_freq": "weekly",
    },
    "universe_group_id": "test20",
})
wait_task(result["task_id"], "回测", timeout=600)

backtests = api("get", "/api/strategies/backtests")
if backtests:
    BT_ID = backtests[0]["id"]
    bt = api("get", f"/api/strategies/backtests/{BT_ID}")
    sm = bt["summary"]
    print(f"  回测ID: {BT_ID}")
    print(f"  年化收益:   {sm['annual_return']:.2%}")
    print(f"  夏普比率:   {sm['sharpe_ratio']:.2f}")
    print(f"  最大回撤:   {sm['max_drawdown']:.2%}")
    print(f"  卡尔玛比率: {sm['calmar_ratio']:.2f}")
    print(f"  索提诺比率: {sm['sortino_ratio']:.2f}")
    print(f"  总交易次数: {sm['total_trades']}")
    print(f"  胜率:       {sm['win_rate']:.2%}")
    print(f"  总交易成本: ${sm['total_cost']:,.0f}")
    print(f"  年化换手:   {sm['annual_turnover']:.2f}")
    print(f"  结果级别:   {bt['result_level']}")
    print(f"  NAV数据点:  {len(bt.get('nav_series', []))}")

# --- Step 7: 生成最新信号 ---
print("\n--- Step 7: 生成交易信号 (2026-04-02) ---")
result = api("post", "/api/signals/generate", json={
    "strategy_id": STRAT_ID,
    "target_date": "2026-04-02",
    "universe_group_id": "test20",
})
wait_task(result["task_id"], "信号生成")

signals = api("get", "/api/signals")
if signals:
    SIG_ID = signals[0]["id"]
    sig = api("get", f"/api/signals/{SIG_ID}")
    print(f"  信号ID: {SIG_ID}")
    print(f"  目标日期:  {sig['target_date']}")
    print(f"  结果级别:  {sig['result_level']}")
    print(f"  信号总数:  {sig['signal_count']}")

    sigs = sig.get("signals", [])
    buys = [x for x in sigs if x["signal"] == 1]
    holds = [x for x in sigs if x["signal"] == 0]
    print(f"  买入信号:  {len(buys)} 只")
    print(f"  持仓外:    {len(holds)} 只")
    print(f"\n  买入标的详情:")
    for x in sorted(buys, key=lambda x: -x["strength"]):
        print(f"    {x['ticker']:6s}  权重: {x['target_weight']:.1%}  强度: {x['strength']:+.4f}")

print("\n" + "=" * 60)
print("  全链路验证完成!")
print("=" * 60)
