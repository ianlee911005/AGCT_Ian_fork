"""
Class to simulate a proper Dependency Injection container.
It could be reimplemented in the future if we decide to use
a proper one. The interface, however, would remain the same.
"""

from .repository import (
    VariantEffectScoreRepository,
    VariantEffectLabelRepository,
    RepoSessionContext,
    VariantFilterRepository,
    VariantRepository,
    VariantEffectSourceRepository
)
from .analyzer import VariantPredictionAnalyzer

import yaml
import os


class VBMContainer:

    def __init__(self, app_root: str = "."):
        with (open(os.path.join(app_root, "config", "config.yaml"), "r") as
              config_file):
            self.config = yaml.safe_load(config_file)
        self._repo_session_context = RepoSessionContext(
            self.config["repository"]["root_dir"])
        self._variant_repo = VariantRepository(self._repo_session_context)
        self._variant_filter_repo = VariantFilterRepository(
            self._repo_session_context)
        self._label_repo = VariantEffectLabelRepository(
            self._repo_session_context,
            self._variant_repo,
            self._variant_filter_repo)
        self._score_repo = VariantEffectScoreRepository(
            self._repo_session_context,
            self._variant_repo,
            self._variant_filter_repo)
        self._variant_effect_source_repo = VariantEffectSourceRepository(
            self._repo_session_context,
            self._score_repo)
        self._analyzer = VariantPredictionAnalyzer(
            self._score_repo,
            self._label_repo,
            self._variant_effect_source_repo)

    @property
    def analyzer(self):
        return self._analyzer


