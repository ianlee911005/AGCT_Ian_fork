import context  # noqa: F401
from agct.analyzer import VariantPredictionAnalyzer
from agct.model import VariantQueryParams
from agct.query import VariantQueryMgr
from agct.reporter import VariantPredictionReporter

# from agct.model import VariantId  # noqa: F401


def test_write_summary_stdout(
        variant_bm_analyzer,
        sample_user_scores,
        variant_bm_reporter: VariantPredictionReporter):
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", sample_user_scores, list_variants=True)
    variant_bm_reporter.write_summary(metrics)
    pass


def test_write_summary_file(
        variant_bm_analyzer,
        sample_user_scores,
        variant_bm_reporter: VariantPredictionReporter):
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", sample_user_scores, list_variants=True)
    variant_bm_reporter.write_summary(metrics, ".")
    pass

