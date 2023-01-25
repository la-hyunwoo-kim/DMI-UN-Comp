from un_comp.functions.comparison.general import get_peid_uid_dict, \
    content_gc, content_rgc

import pandas as pd
from datetime import datetime

import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def nationality_comp(source_df, db_no_vessel, uids_dict,
                     nat_formatting_dict, 
                     db_peid_col, db_uid_col,
                     db_nationality_col, db_nationality_uid_col,
                     source_uid_col, source_nationality_col,
                     separator=";", **kwargs):
    db_nationality = db_no_vessel.loc[db_no_vessel[db_nationality_col] != "",
                                      [db_peid_col, db_uid_col,
                                       db_nationality_col, db_nationality_uid_col]]\
        .drop_duplicates().reset_index(drop=True)
    source_nationality = source_df.loc[source_df[source_nationality_col] != "",
                                       [source_uid_col, source_nationality_col]]

    source_nationality_melt = melt_content_list(
        source_nationality, source_uid_col, source_nationality_col,
        nat_formatting_dict, separator
    ).drop_duplicates().reset_index(drop=True)

    gap_index = content_gc(db_df=db_nationality,
                           source_df=source_nationality_melt,
                           source_uid_col=source_uid_col,
                           db_uid_col=db_uid_col,
                           source_content_col=source_nationality_col,
                           dmi_content_col_format=db_nationality_col,
                           uids_dict=uids_dict)

    rev_gap_index = content_rgc(db_df=db_nationality,
                                source_df=source_nationality_melt,
                                source_uid_col=source_uid_col,
                                db_uid_col=db_uid_col,
                                source_content_col=source_nationality_col,
                                dmi_content_col_format=db_nationality_col,
                                uids_dict=uids_dict)

    source_gap = source_nationality_melt.loc[gap_index, :]
    dmi_gap = db_nationality.loc[rev_gap_index, :]

    logger.info(f"Length of dob_dmi dob_comp is: {str(len(db_nationality))}")
    logger.info(
        f"Length of dob_pdf_melt dob_comp is: {str(len(source_nationality_melt))}")
    logger.info(f"Length of gap_index dob_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index dob_comp is: {str(len(rev_gap_index))}")

    nat_comp_cols = list(source_nationality_melt.columns) + \
        ["vs."] + list(db_nationality.columns) + ["Validation"]
    nat_comp = pd.DataFrame([], columns=nat_comp_cols)

    dmi_uid = db_no_vessel.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap[db_peid_col] = source_gap[source_uid_col].apply(
        lambda x: peid_uid_dict.get(x, ""))

    nat_comp = pd.concat([nat_comp, source_gap, dmi_gap], sort=False) \
        .fillna("") \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .reset_index(drop=True)

    return nat_comp, source_nationality_melt, db_nationality


def melt_content_list(source_dob, source_uid_col, source_nationality_col,
                      nat_formatting_dict, separator):
    out = []
    for _, row in source_dob.iterrows():
        uid = row[source_uid_col]
        content_list = row[source_nationality_col]
        if len(content_list) > 0:
            for content in content_list:
                out.append(
                    [uid, format_source_content(content, nat_formatting_dict)])
    return pd.DataFrame(out, columns=[source_uid_col, source_nationality_col])


def format_source_content(content, nat_formatting_dict):
    if content in nat_formatting_dict.keys():
        return nat_formatting_dict[content]
    else:
        return content
