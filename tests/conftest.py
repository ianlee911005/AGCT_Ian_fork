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
def sample_user_scores(variant_bm_container):
    user_scores_df = variant_bm_container._score_repo.get("cancer",
                                                          "REVEL")
    user_scores_df["RANK_SCORE"] = user_scores_df["RANK_SCORE"].apply(
        lambda sc: sc + 0.1 if sc < 0.9 else 0.95)
    return user_scores_df
 
