import pandas as pd
import numpy as np
from sklearn.metrics import (
     roc_curve,
     roc_auc_score,
     precision_recall_curve,
     auc
)
from .model import (
    VariantQueryParams,
    VariantBenchmarkMetrics
)
from .repository import (
     VariantEffectScoreRepository,
     VariantEffectLabelRepository,
     VariantEffectSourceRepository,
     VARIANT_PK_COLUMNS
)
from .pd_util import filter_dataframe_by_list

VARIANT_EFFECT_SCORE_COLS = ["SCORE_SOURCE"] +\
    VARIANT_PK_COLUMNS + ["RANK_SCORE"]


class VariantPredictionAnalyzer:

    def __init__(self, variant_effect_score_repo: VariantEffectScoreRepository,
                 variant_effect_label_repo: VariantEffectLabelRepository,
                 variant_effect_source_repo: VariantEffectSourceRepository):
        self._variant_effect_score_repo = variant_effect_score_repo
        self._variant_effect_label_repo = variant_effect_label_repo
        self._variant_effect_source_repo = variant_effect_source_repo

    def get_analysis_scores_and_labels(
            self,
            task_name: str,
            user_ve_scores: pd.DataFrame = None,
            column_name_map: dict = None,
            variant_effect_sources: list[str] = None,
            include_variant_effect_sources: bool = None,
            variant_query_criteria: VariantQueryParams = None,
            vep_min_overlap_percent: float = 0,
            variant_vep_retention_percent: float = 0) -> pd.DataFrame:

        if vep_min_overlap_percent is None:
            vep_min_overlap_percent = 0
        if variant_vep_retention_percent is None:
            variant_vep_retention_percent = 0
        if (user_ve_scores is not None and
                len(user_ve_scores) == 0):
            user_ve_scores = None

        # Get the full universe of variants for query criteria. The universe
        # is limited to those for which we have labels.
        variant_universe_pks_df = self._variant_effect_label_repo.get(
                task_name, variant_query_criteria)[VARIANT_PK_COLUMNS]

        # if user has specified variant scores then restrict user
        # variants to those in the universe(i.e. those for which we have
        # labels) and then reset the universe to the filtered user variant list
        if user_ve_scores is not None:
            if column_name_map is not None and len(column_name_map) > 0:
                user_ve_scores = user_ve_scores.rename(
                    columns=column_name_map)
            user_ve_scores = user_ve_scores.merge(variant_universe_pks_df,
                                                  how="inner",
                                                  on=VARIANT_PK_COLUMNS)
            user_ve_scores = user_ve_scores.assign(SCORE_SOURCE="USER")
            variant_universe_pks_df = user_ve_scores[VARIANT_PK_COLUMNS]

        # get the system vep scores based upon selection critera
        system_ve_scores_df = self._variant_effect_score_repo.get(
            task_name, variant_effect_sources,
            include_variant_effect_sources,
            variant_query_criteria)
        
        # First restrict the set of system vep scores to the variants
        # in the universe. Then compute how many variants there are for
        # each vep. Then we only keep the vep scores for veps where
        # the variant count is above the vep_min_overlap_count        
        vep_min_overlap_count = (len(variant_universe_pks_df) *
                                 vep_min_overlap_percent)
        retained_veps = []
        system_ve_scores_count_by_vep = system_ve_scores_df.merge(
             variant_universe_pks_df, how="inner",
             on=VARIANT_PK_COLUMNS).groupby("SCORE_SOURCE").size()
        retained_veps = system_ve_scores_count_by_vep[
            system_ve_scores_count_by_vep >= vep_min_overlap_count
            ].index
        system_ve_scores_df = filter_dataframe_by_list(system_ve_scores_df,
                                                       retained_veps,
                                                       'SCORE_SOURCE')

        # Now for each variant we compute how many veps for which we have
        # scores. We then retain only those variants where the number of
        # veps is above variant_vep_retention_count
        variant_vep_retention_count = (len(retained_veps) *
                                       variant_vep_retention_percent)

        system_ve_scores_count_by_var = system_ve_scores_df.groupby(
             VARIANT_PK_COLUMNS).size()
        retained_variants = (system_ve_scores_count_by_var[
            system_ve_scores_count_by_var >= variant_vep_retention_count].
                reset_index())

        # If user specified scores append them to the system ones. Then merge
        # in the labels for all of the variants.
        if user_ve_scores is not None:
            analysis_ve_scores_df = pd.concat([
                system_ve_scores_df[VARIANT_EFFECT_SCORE_COLS],
                user_ve_scores[VARIANT_EFFECT_SCORE_COLS]])
        else:
            analysis_ve_scores_df = system_ve_scores_df[
                VARIANT_EFFECT_SCORE_COLS]
        analysis_ve_scores_df = filter_dataframe_by_list(analysis_ve_scores_df,
                                                         retained_variants,
                                                         VARIANT_PK_COLUMNS)
        analysis_labels_df = self._variant_effect_label_repo.get(
            task_name, VariantQueryParams(variant_ids=retained_variants))
        analysis_ve_scores_labels_df = analysis_ve_scores_df.merge(
            analysis_labels_df, how="inner", on=VARIANT_PK_COLUMNS)
        return analysis_ve_scores_labels_df

    def _compute_pr(
            self, grouped_ve_scores_labels,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        aucs = []
        precision_recall_score_sources = []
        precisions = []
        recalls = []
        thresholds_list = []
        for score_source, scores_labels_df in grouped_ve_scores_labels:
            prs, recs, thresholds = precision_recall_curve(
                scores_labels_df['BINARY_LABEL'],
                scores_labels_df['RANK_SCORE'])
            aucs.append(auc(recs, prs))
            precision_recall_score_sources.extend([score_source] * len(prs))
            precisions.extend(prs)
            recalls.extend(recs)
            thresholds_list.extend(thresholds)
        pr_df = pd.DataFrame({"SCORE_SOURCE":
                              grouped_ve_scores_labels.indices.keys(),
                              "PR_AUC": aucs})
        pr_curve_coords_df = pd.DataFrame(
            {"SCORE_SOURCE": precision_recall_score_sources,
             "PRECISION": precisions,
             "RECALL": recalls,
             "THRESHOLD": thresholds_list
             })
        return pr_df, pr_curve_coords_df

    def _compute_roc(
            self, grouped_ve_scores_labels,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        aucs = []
        fpr_tpr_score_sources = []
        false_positive_rates = []
        true_positive_rates = []
        thresholds_list = []
        exceps = []
        for score_source, scores_labels_df in grouped_ve_scores_labels:
            try:
                if (len(scores_labels_df) ==
                        sum(scores_labels_df['BINARY_LABEL']) or
                        sum(scores_labels_df['BINARY_LABEL']) == 0):
                    raise Exception("Cannot compute roc metrics because all "
                                    "labels have same value")
                auc = roc_auc_score(scores_labels_df['BINARY_LABEL'],
                                    scores_labels_df['RANK_SCORE'])
                fpr, tpr, thresholds = roc_curve(
                    scores_labels_df['BINARY_LABEL'],
                    scores_labels_df['RANK_SCORE'])
                aucs.append(auc)
                fpr_tpr_score_sources.extend([score_source] * len(fpr))
                false_positive_rates.extend(fpr)
                true_positive_rates.extend(tpr)
                thresholds_list.extend(thresholds)
                exceps.append(np.nan)
            except Exception as e:
                aucs.append(np.nan)
                exceps.append(str(e))

        roc_df = pd.DataFrame({"SCORE_SOURCE": grouped_ve_scores_labels.
                               indices.keys(),
                               "ROC_AUC": aucs,
                               "EXCEPTION": exceps
                               })
        roc_curve_coords_df = pd.DataFrame(
            {"SCORE_SOURCE": fpr_tpr_score_sources,
             "FALSE_POSITIVE_RATE": false_positive_rates,
             "TRUE_POSITIVE_RATE": true_positive_rates,
             "THRESHOLD": thresholds_list
             })
        return roc_df, roc_curve_coords_df

    @staticmethod
    def _compute_general_metrics(group) -> pd.Series:
        return pd.Series(
            {"NUM_VARIANTS": len(group),
             "NUM_POSITIVE_LABELS": group["BINARY_LABEL"].sum(),
             "NUM_NEGATIVE_LABELS": (group["BINARY_LABEL"] ^ 1).sum()
             })

    def _add_info_to_metric_dataframes(self, *dfs):  # -> list(pd.DataFrame):
        return_dfs = []
        for df in dfs:
            if df is not None:
                df = df.merge(self._variant_effect_source_repo.get_all()[
                    ["CODE", "NAME"]], how="left", left_on="SCORE_SOURCE",
                            right_on="CODE")
                df.rename(columns={"NAME": "SOURCE_NAME"}, inplace=True)
                df.drop(columns="CODE", inplace=True)
            return_dfs.append(df)
        return return_dfs

    def _compute_metrics(
            self, task_name: str, ve_scores_labels_df: pd.DataFrame,
            metrics: list[str], list_variants: bool = False
    ) -> VariantBenchmarkMetrics:

        grouped_ve_scores_labels = ve_scores_labels_df.groupby("SCORE_SOURCE")
        general_metrics_df = grouped_ve_scores_labels.apply(
            self._compute_general_metrics, include_groups=False).reset_index()
        roc_df = None
        roc_curve_coords_df = None
        pr_df = None
        pr_curve_coords_df = None
        mwu_df = None
        if "roc" in metrics:
            roc_df, roc_curve_coords_df = self._compute_roc(
                grouped_ve_scores_labels)
        if "pr" in metrics:
            pr_df, pr_curve_coords_df = self._compute_pr
            (grouped_ve_scores_labels)
        if "mwu" in metrics:
            pass
        if list_variants:
            included_variants_df = ve_scores_labels_df[["SCORE_SOURCE"] +
                                                       VARIANT_PK_COLUMNS]
        else:
            included_variants_df = None
        general_metrics_df, roc_df, pr_df, mwu_df, roc_curve_coords_df, \
            pr_curve_coords_df = self._add_info_to_metric_dataframes(
                general_metrics_df, roc_df, pr_df,
                mwu_df, roc_curve_coords_df, pr_curve_coords_df)
        return VariantBenchmarkMetrics(vep_min_overlap_percent,
    variant_vep_retention_percent: float
    num_user_variants: int

            general_metrics_df, roc_df, pr_df, mwu_df, roc_curve_coords_df,
            pr_curve_coords_df, included_variants_df)

    def compute_metrics(
            self,
            task_name: str,
            user_ve_scores: pd.DataFrame = None,
            column_name_map: dict = None,
            variant_effect_sources: list[str] = None,
            include_variant_effect_sources: bool = True,
            variant_query_criteria: VariantQueryParams = None,
            vep_min_overlap_percent: float = 0,
            variant_vep_retention_percent: float = 0,
            metrics: str | list[str] = "roc",
            list_variants: bool = False) -> VariantBenchmarkMetrics:

        scores_and_labels_df = self.get_analysis_scores_and_labels(
            task_name,
            user_ve_scores,
            column_name_map,
            variant_effect_sources,
            include_variant_effect_sources,
            variant_query_criteria,
            vep_min_overlap_percent,
            variant_vep_retention_percent)

        if type(metrics) is str:
            metrics = [metrics]
        return self._compute_metrics(task_name, scores_and_labels_df,
                                     metrics, list_variants)
