import pandas as pd
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def uid_comp(source_df, db_df, source_uid_col, db_uid_col, db_peid_col, **kwargs):
    dmi_uid = db_df.loc[:, [db_peid_col, db_uid_col]]\
        .drop_duplicates().reset_index(drop=True)

    source_uid = source_df.loc[:, [source_uid_col]]\
        .drop_duplicates().reset_index(drop=True)

    gap_index = uid_gc(source_df=source_uid,
                       db_df=dmi_uid,
                       source_uid_col=source_uid_col,
                       db_uid_col=db_uid_col)
    rev_gap_index = uid_rgc(source_df=source_uid,
                            db_df=dmi_uid,
                            source_uid_col=source_uid_col,
                            db_uid_col=db_uid_col)

    logger.info(f"Length of dmi_uid uid_comp is: {str(len(dmi_uid))}")
    logger.info(f"Length of source_uid uid_comp is: {str(len(source_uid))}")

    logger.info(f"Length of gap_index uid_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index uid_comp is: {str(len(rev_gap_index))}")

    cols = list(source_uid.columns) + ["vs."] + \
        list(dmi_uid.columns) + ["Validation"]

    uid_comp = pd.DataFrame({}, columns=cols)
    source_gap = source_uid.loc[gap_index, :]
    dmi_gap = dmi_uid.loc[rev_gap_index, :]
    uid_comp = pd.concat([uid_comp, source_gap, dmi_gap], sort=False) \
        .fillna("") \
        .drop_duplicates() \
        .reset_index(drop=True)

    profile_count = get_profile_count(dmi_uid, db_peid_col)

    return uid_comp, source_uid, dmi_uid, profile_count


def get_profile_count(dmi_uid, db_peid_col):
    peid_df = dmi_uid.loc[:, db_peid_col] \
        .drop_duplicates().reset_index(drop=True)
    
    return peid_df.shape[0]


def uid_gc(source_df, db_df,
           source_uid_col, db_uid_col):
    db_uid_list = set(db_df[db_uid_col].to_list())
    gap_index = []
    for row in tqdm(source_df.itertuples(), total=source_df.shape[0]):
        i = row.Index
        source_uid = getattr(row, source_uid_col)
        if source_uid not in db_uid_list:
            gap_index.append(i)
    return gap_index


def uid_rgc(source_df, db_df,
            source_uid_col, db_uid_col):
    source_uid_list = set(source_df[source_uid_col].to_list())
    rev_gap_index = []
    for row in tqdm(db_df.itertuples(), total=db_df.shape[0]):
        i = row.Index
        db_uid = getattr(row, db_uid_col)
        if db_uid not in source_uid_list:
            rev_gap_index.append(i)

    return rev_gap_index
