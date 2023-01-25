from un_comp.functions.comparison.general import get_peid_uid_dict, \
    content_gc, content_rgc

import pandas as pd

import logging
logger = logging.getLogger(__name__)


def compare_pob_count(
    un_source, un_dob, uids_dict,
    source_uid_col, source_pob_count_col,
    db_uid_col, db_peid_col, db_pob_col, db_pob_uid_col,
    db_pob_count_col,  **kwargs
):
    source_pob_count_df = un_source.loc[
        un_source[source_pob_count_col] != "",
        [source_uid_col, source_pob_count_col]
    ].drop_duplicates().reset_index(drop=True)

    db_pob_df = un_dob.loc[
        :, [db_peid_col, db_pob_col, db_pob_uid_col]
    ].drop_duplicates().reset_index(drop=True)

    db_peid_df = un_dob.loc[
        :, [db_peid_col, db_uid_col]
    ].drop_duplicates().reset_index(drop=True)

    db_pob_count_df = get_db_pob_count(
        db_pob_df, db_peid_df, db_uid_col, db_peid_col,
        db_pob_col, db_pob_uid_col, db_pob_count_col
    )

    gap_index = content_gc(
        db_df=db_pob_count_df,
        source_df=source_pob_count_df,
        source_uid_col=source_uid_col,
        db_uid_col=db_uid_col,
        source_content_col=source_pob_count_col,
        dmi_content_col_format=db_pob_count_col,
        uids_dict=uids_dict)

    rev_gap_index = content_rgc(
        db_df=db_pob_count_df,
        source_df=source_pob_count_df,
        source_uid_col=source_uid_col,
        db_uid_col=db_uid_col,
        source_content_col=source_pob_count_col,
        dmi_content_col_format=db_pob_count_col,
        uids_dict=uids_dict)

    source_gap = source_pob_count_df.loc[gap_index, :]
    dmi_gap = db_pob_count_df.loc[rev_gap_index, :]

    logger.info(
        f"Length of dob_dmi compare_pob_count is: {str(len(db_pob_count_df))}")
    logger.info(
        f"Length of dob_pdf_melt compare_pob_count is: {str(len(source_pob_count_df))}")
    logger.info(
        f"Length of gap_index compare_pob_count is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index compare_pob_count is: {str(len(rev_gap_index))}")

    pob_count_comp_cols = list(source_pob_count_df.columns) + \
        ["vs."] + list(db_pob_count_df.columns) + ["Validation"]
    pob_count_comp = pd.DataFrame([], columns=pob_count_comp_cols)

    dmi_uid = un_dob.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap_kwargs = {
        db_peid_col: source_gap[source_uid_col].apply(
            lambda x: peid_uid_dict.get(x, ""))
        .values
    }
    source_gap = source_gap.assign(**source_gap_kwargs)

    pob_count_comp = pd.concat(
        [pob_count_comp, source_gap, dmi_gap], sort=False) \
        .fillna("") \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .reset_index(drop=True)

    return pob_count_comp, source_pob_count_df, db_pob_count_df


def get_db_pob_count(
    db_pob_df, db_peid_df, db_uid_col, db_peid_col,
    db_pob_col, db_pob_uid_col, db_pob_count_col
):
    address_count_list = []
    peid_list = list(set(db_pob_df[db_peid_col].to_list()))
    for peid in peid_list:

        pob_list_raw = db_pob_df.loc[
            (db_pob_df[db_peid_col] == peid) &
            (db_pob_df[db_pob_col] != ""),
            [db_pob_col]
        ].T.values.tolist()
        try:
            pob_list = pob_list_raw[0]

        except IndexError:
            pob_list = []

        address_count = len(pob_list)

        if address_count > 0:
            address_count_list.append(
                {
                    db_peid_col: peid,
                    db_pob_count_col: address_count
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
