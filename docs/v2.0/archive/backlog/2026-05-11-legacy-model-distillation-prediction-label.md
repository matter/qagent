# Legacy Model Distillation Prediction Label Workflow

## Archived Status

- **Status**: Fixed
- **Original priority**: P2
- **Market**: US
- **Entry**: `POST /api/models/train-distillation`, `ModelService.create_prediction_label_from_model`, `LabelService.compute_label_values`

## Original Problem

Legacy model training could train from price-derived labels and composite labels, but it could not create an audited label from a teacher model's frozen predictions. Agents had no API-supported way to use pre-cutoff teacher predictions as soft labels for a larger student feature set.

## Fix

- Added `target_type="prediction"` labels. These labels read frozen prediction rows from `prediction_label_values` instead of recomputing price-derived targets.
- Added `ModelService.create_prediction_label_from_model()` to bulk-generate teacher predictions over a bounded date range and record teacher lineage, feature set, label id, universe, cutoff, row count, and frozen values.
- Added `ModelService.train_distilled_model()` plus `POST /api/models/train-distillation` and MCP `train_model_distillation`.
- Distillation tasks run through `TaskExecutor` as `model_distillation_train` and return the student model plus `distillation_label_id`.
- Updated agent documentation with the cutoff and validation rules.

## Validation

```bash
uv run python -m unittest tests.test_model_market_scope.ModelMarketScopeTests.test_distillation_api_generates_prediction_label_before_training tests.test_model_market_scope.ModelMarketScopeTests.test_prediction_label_records_teacher_lineage_and_cutoff -v
```
