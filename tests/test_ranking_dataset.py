import unittest

import pandas as pd

from backend.models.lightgbm_model import LightGBMModel
from backend.services import ranking_dataset
from backend.services.ranking_dataset import build_date_groups, compute_ranking_metrics


class RankingDatasetTests(unittest.TestCase):
    def test_build_date_groups_sorts_samples_and_tracks_group_sizes(self):
        idx = pd.MultiIndex.from_tuples(
            [
                ("2024-01-03", "B"),
                ("2024-01-02", "B"),
                ("2024-01-03", "A"),
                ("2024-01-02", "A"),
            ],
            names=["date", "ticker"],
        )
        X = pd.DataFrame({"x": [4, 2, 3, 1]}, index=idx)
        y = pd.Series([0.3, 0.2, -0.1, 0.1], index=idx)

        grouped = build_date_groups(X, y, min_group_size=2)

        self.assertEqual(grouped.group_sizes, [2, 2])
        self.assertEqual(
            list(grouped.X.index),
            [
                (pd.Timestamp("2024-01-02"), "A"),
                (pd.Timestamp("2024-01-02"), "B"),
                (pd.Timestamp("2024-01-03"), "A"),
                (pd.Timestamp("2024-01-03"), "B"),
            ],
        )
        self.assertEqual(grouped.y.tolist(), [0, 1, 0, 1])
        self.assertEqual(grouped.raw_y.tolist(), [0.1, 0.2, -0.1, 0.3])

    def test_integer_rank_labels_are_remapped_to_dense_relevance_by_default(self):
        idx = pd.MultiIndex.from_product(
            [pd.to_datetime(["2024-01-02"]), ["A", "B", "C"]],
            names=["date", "ticker"],
        )
        X = pd.DataFrame({"x": [1, 2, 3]}, index=idx)
        y = pd.Series([170, 31, 5], index=idx)

        grouped = build_date_groups(X, y, min_group_size=2)

        self.assertEqual(grouped.y.tolist(), [2, 1, 0])
        self.assertEqual(grouped.raw_y.tolist(), [170.0, 31.0, 5.0])

    def test_identity_label_gain_requires_dense_non_negative_integer_labels(self):
        idx = pd.MultiIndex.from_product(
            [pd.to_datetime(["2024-01-02"]), ["A", "B", "C"]],
            names=["date", "ticker"],
        )
        X = pd.DataFrame({"x": [1, 2, 3]}, index=idx)
        y = pd.Series([170, 31, 5], index=idx)

        grouped = build_date_groups(X, y, min_group_size=2, label_gain="identity")

        self.assertEqual(grouped.y.tolist(), [2, 1, 0])

    def test_lightgbm_ranking_model_accepts_query_groups(self):
        idx = pd.MultiIndex.from_product(
            [pd.to_datetime(["2024-01-02", "2024-01-03"]), ["A", "B", "C"]],
            names=["date", "ticker"],
        )
        X = pd.DataFrame({"x": [1, 2, 3, 2, 3, 4]}, index=idx)
        y = pd.Series([0, 1, 2, 0, 1, 2], index=idx)

        model = LightGBMModel(task="ranking", params={"n_estimators": 3, "min_child_samples": 1})
        model.fit(X, y, group=[3, 3])
        preds = model.predict(X)

        self.assertEqual(model.get_params()["task"], "ranking")
        self.assertEqual(len(preds), len(X))

    def test_ranking_metrics_stop_pairwise_sampling_at_limit(self):
        idx = pd.MultiIndex.from_product(
            [pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]), ["A", "B", "C"]],
            names=["date", "ticker"],
        )
        raw_y = pd.Series(range(len(idx)), index=idx, dtype=float)
        preds = pd.Series(range(len(idx)), index=idx, dtype=float)
        calls = 0

        def fake_pairwise_counts(y_true, y_score, *, limit):
            nonlocal calls
            calls += 1
            return 1, 1

        original = ranking_dataset._pairwise_accuracy_counts
        try:
            ranking_dataset._pairwise_accuracy_counts = fake_pairwise_counts
            metrics = compute_ranking_metrics(raw_y, preds, pairwise_sample_limit=1)
        finally:
            ranking_dataset._pairwise_accuracy_counts = original

        self.assertEqual(calls, 1)
        self.assertEqual(metrics["pairwise_pairs_sampled"], 1)


if __name__ == "__main__":
    unittest.main()
