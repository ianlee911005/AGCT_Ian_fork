"""
Classes that represent model objects.

Model objects are containers for data. They generally do not have
behavior associated with them.
"""

from dataclasses import dataclass
import pandas as pd
from typing import List, Dict


@dataclass
class VariantId:
    """
    Model object that represents a variant id.

    Attributes
    ----------
    genome_assembly : str
        genome assembly symbol, i.e. hg38
    chromosome : str
    position : int
    reference_nucleotide : str
    alternate_nucleotide : str
    """

    genome_assembly: str
    chromosome: str
    position: int
    reference_nucleotide: str
    alternate_nucleotide: str


@dataclass
class VariantEffectSource:
    """
    Model object that represents a variant effect source.

    Attributes
    ----------
    code : str
        A unique code that identifies the variant effect source
    name : str
        A unique name of the source
    source_type : str
        i.e. VEP
    description : str
    """

    code: str
    name: str
    source_type: str
    description: str


@dataclass
class VariantFilterDf:
    """
    Model object that represents a variant effect source.

    Attributes
    ----------
    code : str
        A unique code that identifies the variant effect source
    name : str
        A unique name of the source
    source_type : str
1        i.e. VEP
    description : str
    """

    filter: pd.Series
    filter_genes: pd.DataFrame
    filter_variants: pd.DataFrame


@dataclass
class VariantQueryParams:
    """
    Model object that represents variant query criteria.

    Attributes
    ----------
    gene_symbols : list or DataFrame, optional
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

    gene_symbols: List[str] | pd.DataFrame | pd.Series = None
    include_genes: bool = True
    variant_ids: pd.DataFrame = None
    include_variant_ids: bool = True
    column_name_map: Dict = None
    allele_frequency_operator: str = "="
    allele_frequency: float = None
    filter_name: str = None


@dataclass
class VariantBenchmarkRoc:

    """
    Roc_aucs – A dataframe containing columns: variant_effect_source,
      roc_auc, num_variants, num_positive_labels, num_negative_labels
    Roc_curve_coordinates – A dataframe containing columns:
     variant_effect_source, false_positive_rate, true_positive_rate, threshold
	variants_included – A dataframe of all variants included in the benchmarking.
      Columns are: variant_effect_source, chromosome, position,
        reference_nucleotide, alternate_nucleotide.
        Only populated if parameter, list_variants is True.
    """

    roc_aucs: pd.DataFrame
    roc_curve_coordinates: pd.DataFrame
    variants_included: pd.DataFrame


@dataclass
class VariantBenchmarkMetrics:
    """
    Roc_aucs – A dataframe containing columns: variant_effect_source,
      roc_auc, num_variants, num_positive_labels, num_negative_labels
    Roc_curve_coordinates – A dataframe containing columns:
     variant_effect_source, false_positive_rate, true_positive_rate, threshold
    variants_included – A dataframe of all variants included in the
    benchmarking.
    Columns are: variant_effect_source, chromosome, position,
    reference_nucleotide, alternate_nucleotide.
    Only populated if parameter, list_variants is True.
    """

    num_variants_included: int
    num_user_variants: int
    general_metrics: pd.DataFrame
    roc_metrics: pd.DataFrame
    pr_metrics: pd.DataFrame
    mwu_metrics: pd.DataFrame
    roc_curve_coordinates: pd.DataFrame
    pr_curve_coordinates: pd.DataFrame
    variants_included: pd.DataFrame

