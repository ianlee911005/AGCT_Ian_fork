import context  # noqa: F401
<<<<<<< HEAD
from agct.analyzer import VariantPredictionAnalyzer
from agct.model import VariantQueryParams
from agct.query import VariantQueryMgr
=======
from agct.analyzer import VEAnalyzer
from agct.model import VEQueryCriteria
from agct.query import VEBenchmarkQueryMgr
>>>>>>> upstream/feature-phase2

# from agct.model import VariantId  # noqa: F401


def test_compute_metrics_basic(variant_bm_analyzer,
                               sample_user_scores):
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", sample_user_scores, list_variants=True)


def test_compute_metrics_col_name_map_include_ve_sources(
<<<<<<< HEAD
        variant_bm_analyzer: VariantPredictionAnalyzer,
=======
        variant_bm_analyzer: VEAnalyzer,
>>>>>>> upstream/feature-phase2
        sample_user_scores_col_name_map):
    user_scores, col_name_map = sample_user_scores_col_name_map
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", user_scores, col_name_map, 
        ['REVEL', 'EVE'], list_variants=True)
    metrics

def test_compute_metrics_exclude_ve_sources(
<<<<<<< HEAD
        variant_bm_analyzer: VariantPredictionAnalyzer,
=======
        variant_bm_analyzer: VEAnalyzer,
>>>>>>> upstream/feature-phase2
        sample_user_scores_col_name_map):
    user_scores, col_name_map = sample_user_scores_col_name_map
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", user_scores, col_name_map, 
        ['REVEL', 'EVE'], False, list_variants=True)
    metrics

def test_compute_metrics_query_params(
<<<<<<< HEAD
        variant_bm_analyzer: VariantPredictionAnalyzer,
        variant_query_mgr: VariantQueryMgr,
        sample_user_scores):
    qry = VariantQueryParams(['MTOR','PTEN'])
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", sample_user_scores, None,
        ['REVEL', 'EVE'], False, qry, list_variants=True)
    qry = VariantQueryParams(variant_ids=metrics.variants_included)
=======
        variant_bm_analyzer: VEAnalyzer,
        variant_query_mgr: VEBenchmarkQueryMgr,
        sample_user_scores):
    qry = VEQueryCriteria(['MTOR','PTEN'])
    metrics = variant_bm_analyzer.compute_metrics(
        "cancer", sample_user_scores, None,
        ['REVEL', 'EVE'], False, qry, list_variants=True)
    qry = VEQueryCriteria(variant_ids=metrics.variants_included)
>>>>>>> upstream/feature-phase2
    variants = variant_query_mgr.get_variants("cancer", qry)
    genes = variants['GENE_SYMBOL'].unique()
    metrics


