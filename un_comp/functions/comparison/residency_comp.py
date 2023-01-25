from un_comp.functions.comparison.general import get_peid_uid_dict, \
    content_gc, content_rgc

import pandas as pd
from datetime import datetime

import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def residency_comp(source_df, db_no_vessel, uids_dict,
                   nat_formating_dict,
                   db_peid_col, db_uid_col,
                   db_residency_col, db_residency_uid_col,
                   source_uid_col, source_residency_col,
                   **kwargs):
    db_residency = db_no_vessel.loc[db_no_vessel[db_residency_col] != "",
                                    [db_peid_col, db_uid_col,
                                     db_residency_col, db_residency_uid_col]]\
        .drop_duplicates().reset_index(drop=True)
    source_residency = source_df.loc[source_df[source_residency_col] != "",
                                     [source_uid_col, source_residency_col]]

    source_residency_melt = melt_content_list(
        source_residency, source_uid_col, source_residency_col, nat_formating_dict
    ).drop_duplicates().reset_index(drop=True)

    gap_index = content_gc(db_df=db_residency,
                           source_df=source_residency_melt,
                           source_uid_col=source_uid_col,
                           db_uid_col=db_uid_col,
                           source_content_col=source_residency_col,
                           dmi_content_col_format=db_residency_col,
                           uids_dict=uids_dict)

    rev_gap_index = content_rgc(db_df=db_residency,
                                source_df=source_residency_melt,
                                source_uid_col=source_uid_col,
                                db_uid_col=db_uid_col,
                                source_content_col=source_residency_col,
                                dmi_content_col_format=db_residency_col,
                                uids_dict=uids_dict)

    source_gap = source_residency_melt.loc[gap_index, :]
    dmi_gap = db_residency.loc[rev_gap_index, :]

    logger.info(f"Length of dob_dmi dob_comp is: {str(len(db_residency))}")
    logger.info(
        f"Length of dob_pdf_melt dob_comp is: {str(len(source_residency_melt))}")
    logger.info(f"Length of gap_index dob_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index dob_comp is: {str(len(rev_gap_index))}")

    residency_comp_cols = list(source_residency_melt.columns) + \
        ["vs."] + list(db_residency.columns) + ["Validation"]
    residency_comp = pd.DataFrame([], columns=residency_comp_cols)

    dmi_uid = db_no_vessel.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap[db_peid_col] = source_gap[source_uid_col].apply(
        lambda x: peid_uid_dict.get(x, ""))

    residency_comp = pd.concat([residency_comp, source_gap, dmi_gap], sort=False) \
        .fillna("") \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .reset_index(drop=True)

    return residency_comp, source_residency_melt, db_residency


def melt_content_list(source_dob, source_uid_col, source_residency_col,
                      nat_formating_dict):
    out = []
    for _, row in source_dob.iterrows():
        uid = row[source_uid_col]
        content_list = row[source_residency_col]
        if len(content_list) > 0:
            for content in content_list:
                out.append(
                    [uid, format_source_content(content, nat_formating_dict)])
    return pd.DataFrame(out, columns=[source_uid_col, source_residency_col])


def format_source_content(content, nat_formating_dict):
    if content in nat_formating_dict.keys():
        return nat_formating_dict[content]
    else:
        return content
