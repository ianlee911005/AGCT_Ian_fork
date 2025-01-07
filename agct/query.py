"""
Classes and methods to query the variant repository.
This layer provides an abstraction layer that sits on top of the
data access layer in the repository module.
It uses the repository module to access the raw data and includes
methods to optionally transform the data to make it more meaningful
or presentation ready to the caller.
"""

from .repository import (
    VariantEffectLabelRepository,
    VariantRepository,
    VariantTaskRepository,
    VariantEffectSourceRepository,
    VariantEffectScoreRepository
)
from .model import VariantQueryParams

import pandas as pd


def cleanup_variant_query_params(params: VariantQueryParams):
    if params.gene_symbols is not None:
        if len(params.gene_symbols) == 0:
            params.gene_symbols = None
        else:
            if not isinstance(params.gene_symbols, pd.DataFrame):
                params.gene_symbols = pd.DataFrame(
                    {"GENE_SYMBOL": params.gene_symbols})
            if params.include_genes is None:
                params.include_genes = True
    if params.variant_ids is not None:
        if len(params.variant_ids) == 0:
            params.variant_ids = None
        else:
            if params.include_variant_ids is None:
                params.include_genes = True
    if params.column_name_map is not None:
        if len(params.column_name) == 0:
            params.column_name_map = None
    if params.allele_frequency_operator is None:
        params.allele_frequency_operator = "="
    return params


class VariantQueryMgr:
    """
    Methods to query the variant repository
    """

    def __init__(self,
                 variant_effect_label_repo: VariantEffectLabelRepository,
                 variant_repo: VariantRepository,
                 variant_task_repo: VariantTaskRepository,
                 variant_effect_source_repo: VariantEffectSourceRepository,
                 variant_effect_score_repo: VariantEffectScoreRepository
                 ):
        self._variant_effect_label_repo = variant_effect_label_repo
        self._variant_repo = variant_repo
        self._variant_task_repo = variant_task_repo
        self._variant_effect_source_repo = variant_effect_source_repo
        self._variant_effect_score_repo = variant_effect_score_repo

    def get_tasks(self) -> pd.DataFrame:
        return self._variant_task_repo.get_all()

    def get_all_variants(self) -> pd.DataFrame:
        return self._variant_repository.get_all()

    def get_variant_effect_sources(self, task_name: str) -> pd.DataFrame:
        return self._variant_effect_source_repo.get_by_task(task_name)

    def get_variant_effect_scores(self, task_name: str,
                                  variant_effect_sources=None,
                                  include_variant_effect_sources: bool = None,
                                  gene_symbols=None,
                                  include_genes: bool = None,
                                  variant_ids: pd.DataFrame = None,
                                  include_variant_ids: bool = None,
                                  allele_frequency_operator: str = None,
                                  allele_frequency: float = None,
                                  filter_name: str = None) -> pd.DataFrame:
        """
        Fetches variants. The optional parameters are filter criteria used to
        limit the set of variants returned.

        Parameters
        ----------
        task_name : str
            Retrieve variants associated with this task.
        gene_symbols : list, optional
            List of gene symbols
        include_genes : bool, optional
            If gene_symbols is provided, indicates whether to limit variants
            to associated with those gene_symbols to exclude variants
            associated with the gene_symbols.
        variant_ids : DataFrame, optional
            List of variant ids
        include_variant_ids : bool, optional
            If variant_ids is provided, indicates whether to limit variants
            to the variant_ids provided or to fetch all variants but those
            in variant_ids
        allele_frequency_operator : str, optional
            If allele_frequency is provided, this is one of "eq", "gt",
            "lt", "ge", "le". i.e. limit variants to those whose
            allele_frequency is equal to, greater than, etc. the
            allele_frequency.
        allele_frequency : float, optional
            Used in conjunction to allele_frequency_operator to limit variants
            to those meeting a certain allele_frequency criteria.
        filter_name : str, optional
            The name of a system filter that can be used to limit the variants
            returned.
        """

        return self._variant_effect_score_repo.get(
            task_name,
            variant_effect_sources,
            include_variant_effect_sources,
            gene_symbols,
            include_genes, variant_ids,
            include_variant_ids,
            allele_frequency_operator,
            allele_frequency,
            filter_name)

    def get_variants(self, task_name: str,
                     query_criteria: VariantQueryParams = None
                     ) -> pd.DataFrame:
        """
        Fetches variants. The optional parameters are filter criteria used to
        limit the set of variants returned.

        Parameters
        ----------
        task_name : str
            Retrieve variants associated with this task.
        gene_symbols : list, optional
            List of gene symbols
        include_genes : bool, optional
            If gene_symbols is provided, indicates whether to limit variants
            to associated with those gene_symbols to exclude variants
            associated with the gene_symbols.
        variant_ids : DataFrame, optional
            List of variant ids
        include_variant_ids : bool, optional
            If variant_ids is provided, indicates whether to limit variants
            to the variant_ids provided or to fetch all variants but those
            in variant_ids
        allele_frequency_operator : str, optional
            If allele_frequency is provided, this is one of "eq", "gt",
            "lt", "ge", "le". i.e. limit variants to those whose
            allele_frequency is equal to, greater than, etc. the
            allele_frequency.
        allele_frequency : float, optional
            Used in conjunction to allele_frequency_operator to limit variants
            to those meeting a certain allele_frequency criteria.
        filter_name : str, optional
            The name of a system filter that can be used to limit the variants
            returned.
        """

        return self._variant_effect_label_repo.get(task_name,
                                                   query_criteria)
        
    def get_variant_distribution(self, by: str, task_name: str,
                                 gene_symbols=None,
                                 include_genes: bool = None,
                                 variant_ids: pd.DataFrame = None,
                                 include_variant_ids: bool = None,
                                 allele_frequency_operator: str = None,
                                 allele_frequency: float = None,
                                 filter_name: str = None
                                 ) -> pd.DataFrame:

        label_df = self._variant_effect_label_repo.get_all_by_task(
            task_name)[['CHROMOSOME', 'BINARY_LABEL']]
        label_df[['POSITIVE_LABEL', 'NEGATIVE_LABEL']] = label_df.apply(
            lambda row: [row['BINARY_LABEL'], 1 ^ row['BINARY_LABEL']]
        )
        if by == 'chromosome':
            grouped = label_df.groupby('CHROMOSOME')
            return grouped.agg(
                NUM_POSITIVE_VARIANTS=pd.NamedAgg(column='POSITIVE_LABEL',
                                                  aggfunc='sum'),
                NUM_NEGATIVE_VARIANTS=pd.NamedAgg(column='NEGATIVE_LABEL',
                                                  aggfunc='sum')
                            )['CHROMSOME', 'COUNT_POSITIVE', 'COUNT_NEGATIVE']
        else:
            grouped = label_df.groupby('GENE_SYMBOL')
            return grouped.agg(
                NUM_POSITIVE_VARIANTS=pd.NamedAgg(column='POSITIVE_LABEL',
                                                  aggfunc='sum'),
                NUM_NEGATIVE_VARIANTS=pd.NamedAgg(column='NEGATIVE_LABEL',
                                                  aggfunc='sum')
                            )['GENE_SYMBOL', 'COUNT_POSITIVE',
                              'COUNT_NEGATIVE']

    
