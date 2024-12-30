import context  # noqa: F401
# from agct.analyzer import VariantPredictionAnalyzer

# from agct.model import VariantId  # noqa: F401


def test_compute_metrics(variant_bm_analyzer,
                         sample_user_scores):
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", sample_user_scores, list_variants=True)
    metrics
