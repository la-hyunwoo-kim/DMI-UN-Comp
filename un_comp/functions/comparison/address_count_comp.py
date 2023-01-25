from un_comp.functions.comparison.general import get_peid_uid_dict, \
    content_gc, content_rgc

from tqdm import tqdm
import pandas as pd
from datetime import datetime

import logging
logger = logging.getLogger(__name__)


def compare_address_count(
    un_source, un_db, uids_dict,
    source_uid_col, source_address_count_col,
    db_uid_col, db_peid_col, db_address_cols, db_address_uid_col,
    db_address_count_col, db_residency_col, db_residency_uid_col,
    db_country_col, **kwargs
):
    source_address_count_df = un_source.loc[
        un_source[source_address_count_col] != "",
        [source_uid_col, source_address_count_col]
    ].drop_duplicates().reset_index(drop=True)

    db_address_df = un_db.loc[
        :, db_address_cols +
        [db_peid_col, db_address_uid_col]
    ].drop_duplicates().reset_index(drop=True)

    db_peid_df = un_db.loc[
        :, [db_peid_col, db_uid_col]
    ].drop_duplicates().reset_index(drop=True)

    db_residency_df = un_db.loc[
        :, [db_peid_col,  db_residency_col, db_residency_uid_col]
    ].drop_duplicates().reset_index(drop=True)

    db_address_count_df = get_db_address_count(
        db_address_df, db_residency_df, db_peid_df, db_peid_col,
        db_residency_col, db_country_col, db_address_count_col
    )

    gap_index = content_gc(
        db_df=db_address_count_df,
        source_df=source_address_count_df,
        source_uid_col=source_uid_col,
        db_uid_col=db_uid_col,
        source_content_col=source_address_count_col,
        dmi_content_col_format=db_address_count_col,
        uids_dict=uids_dict)

    rev_gap_index = content_rgc(
        db_df=db_address_count_df,
        source_df=source_address_count_df,
        source_uid_col=source_uid_col,
        db_uid_col=db_uid_col,
        source_content_col=source_address_count_col,
        dmi_content_col_format=db_address_count_col,
        uids_dict=uids_dict)

    source_gap = source_address_count_df.loc[gap_index, :]
    dmi_gap = db_address_count_df.loc[rev_gap_index, :]

    logger.info(
        f"Length of dob_dmi compare_address_count is: {str(len(db_address_count_df))}")
    logger.info(
        f"Length of dob_pdf_melt compare_address_count is: {str(len(source_address_count_df))}")
    logger.info(
        f"Length of gap_index compare_address_count is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index compare_address_count is: {str(len(rev_gap_index))}")

    address_count_comp_cols = list(source_address_count_df.columns) + \
        ["vs."] + list(db_address_count_df.columns) + ["Validation"]
    address_count_comp = pd.DataFrame([], columns=address_count_comp_cols)

    dmi_uid = un_db.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap_kwargs = {
        db_peid_col: source_gap[source_uid_col].apply(
            lambda x: peid_uid_dict.get(x, ""))
        .values
    }
    source_gap = source_gap.assign(**source_gap_kwargs)

    address_count_comp = pd.concat(
        [address_count_comp, source_gap, dmi_gap], sort=False) \
        .fillna("") \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .reset_index(drop=True)

    return address_count_comp, source_address_count_df, db_address_count_df


def get_db_address_count(
    db_address_df, db_residency_df, db_peid_df, db_peid_col,
    db_residency_col, db_country_col, db_address_count_col
):
    address_count_list = []
    peid_list = list(set(db_address_df[db_peid_col].to_list()))
    for peid in peid_list:
        # address_list = db_address_df.loc[
        #     db_address_df[db_peid_col] == peid,
        #     db_address_cols
        # ]

        country_list_raw = db_address_df.loc[
            (db_address_df[db_peid_col] == peid) &
            (db_address_df[db_country_col] != "Not Known") &
            (db_address_df[db_country_col] != ""),
            [db_country_col]
        ].T.values.tolist()
        try:
            country_list = country_list_raw[0]

        except IndexError:
            country_list = []

        address_count = len(country_list)

        residency_list_raw = db_residency_df.loc[
            (db_residency_df[db_peid_col] == peid) &
            (db_residency_df[db_residency_col] != ""),
            [db_residency_col]
        ].T.values.tolist()

        try:
            residency_list = residency_list_raw[0]

        except IndexError:
            residency_list = []

        for country in residency_list:
            if len(country_list) > 0:
                if country not in country_list:
                    address_count += 1

            else:
                address_count += 1

        if address_count > 0:
            address_count_list.append(
                {
                    db_peid_col: peid,
                    db_address_count_col: address_count
                }
            )

    address_count_df = pd.DataFrame(address_count_list)

    out_df = db_peid_df.merge(
        right=address_count_df,
        how="right",
        on=db_peid_col
    )

    return out_df


def get_concat_address(row, db_address_cols):
    address_out = ""
    for address_col in db_address_cols:
        if row[address_col] != "Not Known":
            address_out += row[address_col]

    return address_out
