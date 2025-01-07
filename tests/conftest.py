import pytest
import context  # noqa: F401
from agct.container import VBMContainer


@pytest.fixture
def variant_bm_container():
    return VBMContainer()


@pytest.fixture
def variant_bm_analyzer(variant_bm_container):
    return variant_bm_container.analyzer


@pytest.fixture
def variant_bm_reporter(variant_bm_container):
    return variant_bm_container.reporter


@pytest.fixture
def sample_user_scores(variant_bm_container):
    user_scores_df = variant_bm_container._score_repo.get("cancer",
                                                          "REVEL")
    user_scores_df["RANK_SCORE"] = user_scores_df["RANK_SCORE"].apply(
        lambda sc: sc + 0.1 if sc < 0.9 else 0.95)
    return user_scores_df


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
 
