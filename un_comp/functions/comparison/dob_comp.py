from un_comp.functions.comparison.general import get_peid_uid_dict, \
    content_gc, content_rgc

import pandas as pd
from datetime import datetime

import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def dob_comp(source_df, db_df, uids_dict,
             source_dob_format, db_dob_format, source_uid_col, source_dob_col,
             db_peid_col, db_uid_col, db_dob_col, db_dob_uid_col, **kwargs
             ):
    source_dob = melt_source_dob(source_df, source_uid_col, source_dob_col)

    db_dob = db_df.loc[db_df[db_dob_col] != "",
                       [db_peid_col, db_uid_col,
                        db_dob_col, db_dob_uid_col]] \
        .drop_duplicates().reset_index(drop=True)

    db_dob_col_format = db_dob_col + "_format"

    db_dob[db_dob_col_format] = db_dob[db_dob_col].apply(
        lambda x: format_db_dob(x, source_dob_format, db_dob_format)
    )

    gap_index = content_gc(db_df=db_dob,
                       source_df=source_dob,
                       source_uid_col=source_uid_col,
                       db_uid_col=db_uid_col,
                       source_content_col=source_dob_col,
                       dmi_content_col_format=db_dob_col_format,
                       uids_dict=uids_dict)

    rev_gap_index = content_rgc(db_df=db_dob,
                            source_df=source_dob,
                            source_uid_col=source_uid_col,
                            db_uid_col=db_uid_col,
                            source_content_col=source_dob_col,
                            dmi_content_col_format=db_dob_col_format,
                            uids_dict=uids_dict)

    source_gap = source_dob.loc[gap_index, :]
    dmi_gap = db_dob.loc[rev_gap_index, :]

    logger.info(f"Length of dob_dmi dob_comp is: {str(len(db_dob))}")
    logger.info(
        f"Length of source_dob dob_comp is: {str(len(source_dob))}")
    logger.info(f"Length of gap_index dob_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index dob_comp is: {str(len(rev_gap_index))}")

    dob_comp_cols = list(source_dob.columns) + \
        ["vs."] + list(db_dob.columns) + ["Validation"]
    dob_comp = pd.DataFrame([], columns=dob_comp_cols)

    dmi_uid = db_df.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap_kwargs = {
        db_peid_col: source_gap[source_uid_col].apply(
            lambda x: peid_uid_dict.get(x, ""))
        .values
    }
    source_gap = source_gap.assign(**source_gap_kwargs)

    dob_comp = pd.concat([dob_comp, source_gap, dmi_gap], sort=False) \
        .fillna("") \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .drop(db_dob_col_format, axis=1) \
        .reset_index(drop=True)

    return dob_comp, source_dob, db_dob


def melt_source_dob(source_df, source_uid_col, source_dob_col):
    melt_list = []
    for i in range(0, source_df.shape[0]):
        source_uid = source_df.loc[i, source_uid_col]
        source_dob = source_df.loc[i, source_dob_col]

        if len(source_dob) > 0:
            for date in source_dob:
                entry = {
                    source_uid_col: source_uid,
                    source_dob_col: date
                }
                melt_list.append(entry)
        else:
            pass
    return pd.DataFrame(melt_list)


def format_db_dob(raw_db_dob, source_dob_format, db_dob_format):
    if len(raw_db_dob) > 8:
        dob_out = datetime.strptime(
            raw_db_dob, db_dob_format).strftime(source_dob_format)
    else:
        dob_out = raw_db_dob
    return dob_out.strip()
