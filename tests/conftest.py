
import pytest
import random
import context  # noqa: F401
from agct.container import VEBenchmarkContainer
from agct.repository import (
    VARIANT_PK_COLUMNS
)


@pytest.fixture
def variant_bm_container():
    return VEBenchmarkContainer()


@pytest.fixture
def variant_bm_analyzer(variant_bm_container):
    return variant_bm_container.analyzer


@pytest.fixture
def variant_bm_reporter(variant_bm_container):
    return variant_bm_container.reporter


@pytest.fixture
def variant_bm_plotter(variant_bm_container):
    return variant_bm_container.plotter


def generate_random_floats(n, start, end):
    return [random.uniform(start, end) for _ in range(n)]


@pytest.fixture
def sample_user_scores(variant_bm_container):
    user_scores_df = variant_bm_container._score_repo.get_all_by_task(
        "cancer")
    user_variants = user_scores_df[VARIANT_PK_COLUMNS].drop_duplicates()
    random_idxs = random.sample(list(range(len(user_variants))), 670)
    user_variants = user_variants.iloc[random_idxs]
    random_scores = generate_random_floats(len(user_variants), 0.01, 0.99)
    user_variants['RANK_SCORE'] = random_scores
    return user_variants


@pytest.fixture
def sample_user_scores_col_name_map(variant_bm_container, sample_user_scores):
    user_scores_df = variant_bm_container._score_repo.get("cancer",
                                                          "REVEL")
    user_scores_df["RANK_SCORE"] = user_scores_df["RANK_SCORE"].apply(
        lambda sc: sc + 0.1 if sc < 0.9 else 0.95)
    col_name_map = {"CHROMOSOME": "chr", "POSITION": "pos",
                    "REFERENCE_NUCLEOTIDE": "ref",
                    "ALTERNATE_NUCLEOTIDE": "alt",
                    "GENOME_ASSEMBLY": "assembly",
                    "RANK_SCORE": "score"}
    user_scores_df.rename(columns=col_name_map, inplace=True)
    col_name_map = {val: key for key, val in col_name_map.items()}
    return user_scores_df, col_name_map


@pytest.fixture
def variant_query_mgr(variant_bm_container):
    return variant_bm_container.query_mgr


@pytest.fixture
def ve_analysis_exporter(variant_bm_container):
    return variant_bm_container.exporter
 
