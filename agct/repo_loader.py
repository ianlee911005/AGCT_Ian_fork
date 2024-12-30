"""
Module description here
"""

import os
import pandas as pd
import numpy as np
import re
from typing import List
from util import FileUtil
from util import TimeUtil
from repository import DATA_FOLDER, TASK_FOLDERS, TABLE_DEFS


COLUMN_NAME_MAP = {
    "chr": "CHROMOSOME",
    "pos": "POSITION",
    "ref": "REFERENCE_NUCLEOTIDE",
    "alt": "ALTERNATE_NUCLEOTIDE",
    "aaref": "REFERENCE_AMINO_ACID",
    "aaalt": "ALTERNATE_AMINO_ACID",
    "aapos": "AMINO_ACID_POSITION",
    "rs_dbSNP": "RS_DBSNP",
    "hg19_chr": "PRIOR_CHROMOSOME",
    "hg19_pos": "PRIOR_POSITION",
    "hg18_chr": "PRIOR_PRIOR_CHROMOSOME",
    "hg18_pos": "PRIOR_PRIOR_POSITION",
    "genename": "GENE_SYMBOL",
    "Ensembl_geneid": "ENSEMBL_GENE_ID",
    "Ensembl_transcriptid": "ENSEMBL_TRANSCRIPT_ID",
    "Ensembl_proteinid": "ENSEMBL_PROTEIN_ID"
}
VEP_COLUMN_LIST = [
    {"vep": "REVEL", "raw_score": "REVEL_score",
     "rank_score": "REVEL_rankscore"},
    {"vep": "GMVP", "raw_score": "gMVP_score", "rank_score": "gMVP_rankscore"},
    {"vep": "VARITY_R", "raw_score": "VARITY_R_score",
     "rank_score": "VARITY_R_rankscore"},
    {"vep": "VARITY_ER", "raw_score": "VARITY_ER_score",
     "rank_score": "VARITY_ER_rankscore"},
    {"vep": "VARIATY_R_LOO", "raw_score": "VARITY_R_LOO_score",
     "rank_score": "VARITY_R_LOO_rankscore"},
    {"vep": "VARIATY_ER_LOO", "raw_score": "VARITY_ER_LOO_score",
     "rank_score": "VARITY_ER_LOO_rankscore"},
    {"vep": "ESM1B", "raw_score": "ESM1b_score",
     "rank_score": "ESM1b_rankscore"},
    {"vep": "EVE", "raw_score": "EVE_score",
     "rank_score": "EVE_rankscore"},
    {"vep": "ALPHAM", "raw_score": "AlphaMissense_score",
     "rank_score": "AlphaMissense_rankscore"},
    {"vep": "SIFT", "raw_score": "SIFT_score",
     "rank_score": "SIFT_converted_rankscore"},
    {"vep": "SIFT4G", "raw_score": "SIFT4G_score",
     "rank_score": "SIFT4G_converted_rankscore"},
    {"vep": "POLYP2HDIV", "raw_score": "Polyphen2_HDIV_score",
     "rank_score": "Polyphen2_HDIV_rankscore"},
    {"vep": "POLYP2HVAR", "raw_score": "Polyphen2_HVAR_score",
     "rank_score": "Polyphen2_HVAR_rankscore"},
    {"vep": "LRT", "raw_score": "LRT_score",
     "rank_score": "LRT_converted_rankscore"},
    {"vep": "MUTTASTE", "raw_score": "MutationTaster_score",
     "rank_score": "MutationTaster_converted_rankscore"},
    {"vep": "MUTASSESS", "raw_score": "MutationAssessor_score",
     "rank_score": "MutationAssessor_rankscore"},
    {"vep": "FATHMM", "raw_score": "FATHMM_score",
     "rank_score": "FATHMM_converted_rankscore"},
    {"vep": "PROVEAN", "raw_score": "PROVEAN_score",
     "rank_score": "PROVEAN_converted_rankscore"},
    {"vep": "VEST4", "raw_score": "VEST4_score",
     "rank_score": "VEST4_rankscore"},
    {"vep": "METASVM", "raw_score": "MetaSVM_score",
     "rank_score": "MetaSVM_rankscore"},
    {"vep": "METALR", "raw_score": "MetaLR_score",
     "rank_score": "MetaLR_rankscore"},
    {"vep": "METARNN", "raw_score": "MetaRNN_score",
     "rank_score": "MetaRNN_rankscore"},
    {"vep": "MCAP", "raw_score": "M_CAP_score",
     "rank_score": "M_CAP_rankscore"},
    {"vep": "MUTPRED", "raw_score": "MutPred_score",
     "rank_score": "MutPred_rankscore"},
    {"vep": "MVP", "raw_score": "MVP_score",
     "rank_score": "MVP_rankscore"},
    {"vep": "MPC", "raw_score": "MPC_score",
     "rank_score": "MPC_rankscore"},
    {"vep": "PRIMAI", "raw_score": "PrimateAI_score",
     "rank_score": "PrimateAI_rankscore"},
    {"vep": "DEOGEN2", "raw_score": "DEOGEN2_score",
     "rank_score": "DEOGEN2_rankscore"},
    {"vep": "BAYESDAAF", "raw_score": "BayesDel_addAF",
     "rank_score": "BayesDel_addAF_rankscore"},
    {"vep": "BAYESDNAF", "raw_score": "BayesDel_noAF_score",
     "rank_score": "BayesDel_noAF_rankscore"},
    {"vep": "CLINPRED", "raw_score": "ClinPred_score",
     "rank_score": "ClinPred_rankscore"},
    {"vep": "LISTS2", "raw_score": "LIST_S2_score",
     "rank_score": "LIST_S2_rankscore"},
    {"vep": "CADD", "raw_score": "CADD_raw",
     "rank_score": "CADD_raw_rankscore"},
    {"vep": "DANN", "raw_score": "DANN_score",
     "rank_score": "DANN_rankscore"},
    {"vep": "FATHMMMLK", "raw_score": "fathmm_MKL_coding_score",
     "rank_score": "fathmm_MKL_coding_rankscore"},
    {"vep": "FATHMMXF", "raw_score": "fathmm_XF_coding_score",
     "rank_score": "fathmm_XF_coding_rankscore"},
    {"vep": "EIGEN", "raw_score": "Eigen_raw_coding",
     "rank_score": "Eigen_raw_coding_rankscore"},
    {"vep": "EIGENPC", "raw_score": "Eigen_PC_raw_coding",
     "rank_score": "Eigen_PC_raw_coding_rankscore"},
    {"vep": "GENCANYON", "raw_score": "GenoCanyon_score",
     "rank_score": "GenoCanyon_rankscore"},
    {"vep": "INTFITCONS", "raw_score": "Integrated_fitCons_score",
     "rank_score": "Integrated_fitCons_rankscore"},
    {"vep": "GM12878FC", "raw_score": "GM12878_fitCons_score",
     "rank_score": "GM12878_fitCons_rankscore"},
    {"vep": "H1HESCFC", "raw_score": "H1_hESC_fitCons_score",
     "rank_score": "H1_hESC_fitCons_rankscore"},
    {"vep": "HUVECFC", "raw_score": "HUVEC_fitCons_score",
     "rank_score": "HUVEC_fitCons_rankscore"},
    {"vep": "LINSIGHT", "raw_score": "LINSIGHT",
     "rank_score": "LINSIGHT_rankscore"},
    {"vep": "GERPRS", "raw_score": "GERP_RS",
     "rank_score": "GERP_RS_rankscore"},
    {"vep": "PHYLP100VERT", "raw_score": "phyloP100way_vertebrate",
     "rank_score": "phyloP100way_vertebrate_rankscore"},
    {"vep": "PHYLP470MAM", "raw_score": "phyloP470way_mammalian",
     "rank_score": "phyloP470way_mammalian_rankscore"},
    {"vep": "PHYLP17PRI", "raw_score": "phyloP17way_primate",
     "rank_score": "phyloP17way_primate_rankscore"},
    {"vep": "PHCONS100VERT",
     "raw_score": "phastCons100way_vertebrate",
     "rank_score": "phastCons100way_vertebrate_rankscore"},
    {"vep": "PHCONS470MAM",
     "raw_score": "phastCons470way_mammalian",
     "rank_score": "phastCons470way_mammalian_rankscore"},
    {"vep": "PHCONS17PRI", "raw_score": "phastCons17way_primate",
     "rank_score": "phastCons17way_primate_rankscore"},
    {"vep": "SIPHY29LO", "raw_score": "SiPhy_29way_logOdds",
     "rank_score": "SiPhy_29way_logOdds_rankscore"},
    {"vep": "BSTATISTIC", "raw_score": "bStatistic",
     "rank_score": "bStatistic_converted_rankscore"}
]
VARIANT_EFFECT_SOURCE_DATA = [
    ["REVEL", "REVEL", "VEP", "REVEL"],
    ["GVMP", "gVMP", "VEP", "gVMP"],
    ["VAR_R", "VARITY_R", "VEP", "VARITY_R"],
    ["VAR_ER", "VARITY_ER", "VEP", "VARITY_ER"],
    ["VAR_RL", "VARITY_R_LOO", "VEP", "VARITY_R_LOO"],
    ["VAR_ERL", "VARITY_ER_LOO", "VEP", "VARITY_ER_LOO"],
    ["ESM1b", "ESM1b", "VEP", "ESM1b"],
    ["EVE", "EVE", "VEP", "EVE"],
    ["ALPHAM", "AlphaMissense", "VEP", "AlphaMissense"],
]
VARIANT_DATA_SOURCE_DATA = [
    ["GNOMGE", "GNOMAD_GENOMES", "GNOMAD GENOMES"],
    ["GNOMEX", "GNOMAD_EXOMES", "GNOMAD EXOMES"]
]


class RepositoryLoader:

    def __init__(self):
        self._log_folder = None
        pass

    def _convert_dot_to_nan(self, val):
        if val == '.':
            return None
        return val

    def _derive_variant_effect_source_columns(self, row):
        source_name = re.match("(.+)_rankscore", row["RANK_SCORE"]).group(1)
        return [row["VEP"], source_name, "VEP", source_name]

    def _task_full_path_name(task: str, file_name: str):
        return os.path.join(DATA_FOLDER, task, file_name)

    def init_variant_task(self):

        FileUtil.create_folder(DATA_FOLDER)
        for task_folder in TASK_FOLDERS:
            FileUtil.create_folder(task_folder)

        variant_effect_task_df = pd.DataFrame(
            data=np.array([['CANCER', 'CANCER', 'Cancer', 'Cancer']]),
            columns=TABLE_DEFS["VARIANT_TASK"].columns
            )
        variant_effect_task_df.to_csv(os.path.join(DATA_FOLDER,
                                      "variant_task.csv"),
                                      index=False)

    def init_variant_effect_source(self):

        variant_effect_source_df = pd.DataFrame(
            data=VEP_COLUMN_LIST,
            columns=['VEP', 'RAW_SCORE', 'RANK_SCORE']
        )
        ves_columns = TABLE_DEFS["VARIANT_EFFECT_SOURCE"].columns
        ves_file_name = TABLE_DEFS["VARIANT_EFFECT_SOURCE"].file_name
        variant_effect_source_df[ves_columns] =\
            variant_effect_source_df.apply(
                                self._derive_variant_effect_source_columns)

        variant_effect_source_df[ves_columns].to_csv(
            os.path.join(DATA_FOLDER,
                         "variant_effect_source.csv"),
            index=False)

        variant_data_source_df = pd.DataFrame(
            data=np.array(VARIANT_DATA_SOURCE_DATA),
            columns=ves_columns
            )
        variant_data_source_df.to_csv(os.path.join(DATA_FOLDER,
                                      ves_file_name),
                                      index=False)

    def _build_excep_where_clause(self, column_list: List(str),
                                  suffixes: List(str)):
        where_list = ["(" + col + suffixes[0] + " is not None & " +
                      col + suffixes[1] +
                      " is not None & " +
                      col + suffixes[0] + " != " + col + suffixes[1] + ")"
                      for col in column_list]
        return " or ".join(where_list)

    def _excep_file_full_path_name(self, task: str, repo_file_name: str):
        os.path.join(self._log_folder, task + "_" +
                     repo_file_name.removesuffix(".csv") + "_" +
                     TimeUtil.now_str() + ".csv")

    def _upsert_repository_file(self, new_data: pd.DataFrame, task: str,
                                columns: List(str), repo_file_name: str,
                                pk_columns: List(str)):
        repo_file = self._task_full_path_name(task, repo_file_name)
        new_data_df = new_data[columns]
        if not os.path.exists(repo_file):
            new_data_df.to_csv(repo_file, index=False)
            return
        repo_df = pd.read_csv(repo_file)

        df_merge = repo_df.merge(new_data_df, how="inner",
                                 on=pk_columns)
        non_pk_columns = list(set(columns) - set(pk_columns))
        exception_where = self._build_excep_where_clause(non_pk_columns,
                                                         ["_x", "_y"])
        exception_df = df_merge.query(exception_where)
        exception_df.to_csv(
            self._excep_file_full_path_name(
                task, "upsert_" + repo_file_name + "_exceptions"),
            index=False)

        """
        Another less concise way to do an upsert

        repo_df.update(repo_df[pk_columns]
                        .merge(new_data_df, on=pk_columns, how="left"))
        left_join_df = new_data_df.merge(repo_df, how="left",
                                on=pk_columns,
                                indicator=True, suffixes=[None, "_yyyy"])
        repo_df = pd.concat(repo_df,
                            left_join_df[left_join_df["_merge"] == "left_only"]
                            [columns])
        """

        repo_df.set_index(pk_columns, inplace=True)
        new_data_df = new_data_df.set_index(pk_columns)
        repo_df = repo_df.combine_first(new_data_df)
        repo_df.to_csv(repo_file)

    def load_variant_file(self, genome_assembly: str, task: str,
                          data_file: str,
                          file_folder: str, data_source: str,
                          binary_label: int, prior_genome_assembly: str,
                          prior_prior_genome_assembly: str):

        variant_df = pd.read_csv(os.path.join(file_folder, data_file))
        variant_df['GENOME_ASSEMBLY'] = genome_assembly
        variant_df["RAW_LABEL"] = None
        variant_df["LABEL_SOURCE"] = data_source
        variant_df["BINARY_LABEL"] = binary_label
        if "gnomAD_exomes_AF" in variant_df.columns:
            variant_df[["ALLELE_FREQUENCY", "ALLELE_FREQUENCY_SOURCE"]]\
                = variant_df.apply(
                lambda row: [row["gnomAD_exomes_AF"], "GNOMEX"] if
                not row["gnomAD_exomes_AF"].isnull() else
                [row["gnomAD_genomes_AF"], "GNOMGE"] if
                not row["gnomAD_genomes_AF"].isnull() else
                [None, None], axis=1, result_type="expand")
        else:
            variant_df[["ALLELE_FREQUENCY", "ALLELE_FREQUENCY_SOURCE"]]\
                = [None, None]

        variant_df = variant_df.map(self._convert_dot_to_nan)
        variant_df.rename(columns=COLUMN_NAME_MAP, inplace=True)
        variant_df["PRIOR_GENOME_ASSEMBLY"] = np.where(
            variant_df['PRIOR_CHROMOSOME'].isnull(),
            None, prior_genome_assembly)
        variant_df["PRIOR_PRIOR_GENOME_ASSEMBLY"] = np.where(
            variant_df['PRIOR_PRIOR_CHROMOSOME'].isnull(),
            None, prior_prior_genome_assembly)
        self._upsert_repository_file(variant_df, task, VARIANT_COLUMN_LIST,
                                     "variant.csv", VARIANT_PK_COLUMNS)

        variant_effect_score_df = pd.DataFrame(
            columns=VARIANT_EFFECT_SCORE_COLUMNS)
        for vep_columns in VEP_COLUMN_LIST:
            if vep_columns["raw_score"] not in variant_df.columns:
                continue
            vep_df = variant_df.query(vep_columns["rank_score"] +
                                      " is not None")
            if len(vep_df) == 0:
                continue
            vep_df = vep_df[VARIANT_EFFECT_SCORE_COLUMNS[:5] +
                            [vep_columns["raw_score"], vep_columns["rank_score"]]]
            vep_df["SCORE_SOURCE"] = vep_columns["vep"]
            vep_df.rename(columns={vep_columns["raw_score"]: "RAW_SCORE",
                                   vep_columns["rank_score"]: "RANK_SCORE"},
                          inplace=True)
            variant_effect_score_df = pd.concat(
                [variant_effect_score_df, vep_df[VARIANT_EFFECT_SCORE_COLUMNS]])

        self._upsert_repository_file(variant_effect_score_df, task,
                                     VARIANT_EFFECT_SCORE_COLUMNS,
                                     "variant_effect_score.csv",
                                     VARIANT_EFFECT_SCORE_PK_COLUMNS)
        self._upsert_repository_file(variant_df, task,
                                     VARIANT_LABEL_COLUMN_LIST,
                                     "variant_effect_label.csv",
                                     VARIANT_PK_COLUMNS)
