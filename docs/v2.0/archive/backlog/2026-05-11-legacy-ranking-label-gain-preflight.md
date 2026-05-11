# Legacy Ranking Label Gain Preflight

## Archived Status

- **Status**: Fixed
- **Original priority**: P2
- **Market**: US
- **Entry**: `POST /api/models/train`, `backend/services/ranking_dataset.py`, `backend/models/lightgbm_model.py`

## Original Problem

LightGBM ranker training could fail after a long training setup with errors like `Label 501 is not less than the number of label mappings (501)`. The service generated ordinal date-group labels but did not derive the required LightGBM `label_gain` mapping length from the actual post-filter maximum label.

## Fix

- Ranking training now computes `max_label_gain` after date grouping.
- For LightGBM ranking objectives, missing `model_params.label_gain` is generated as `0..max_label_gain`.
- If a user supplies a short `label_gain`, training fails before model fit with `label_gain length must be > max ordinal gain`.
- Ranking metrics now report `max_label_gain`, `lightgbm_label_gain_length`, and `lightgbm_label_gain_source`.

## Validation

```bash
uv run python -m unittest tests.test_model_market_scope.ModelMarketScopeTests.test_ranking_training_injects_safe_lightgbm_label_gain_length tests.test_model_market_scope.ModelMarketScopeTests.test_ranking_training_rejects_short_lightgbm_label_gain_before_fit -v
uv run python -m unittest tests.test_model_market_scope -v
```
