from un_comp.functions.comparison.general import get_peid_uid_dict, \
    content_gc, content_rgc

import pandas as pd
from datetime import datetime

import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def id_comp(source_df, db_df, uids_dict,
            source_uid_col, source_id_col,
            db_peid_col, db_uid_col, db_id_col, db_id_uid_col,
            **kwargs):
    source_id = source_df.loc[source_df[source_id_col] != "",
                              [source_uid_col, source_id_col]]\
        .reset_index(drop=True)

    db_id = db_df.loc[db_df[db_id_col] != "",
                      [db_peid_col, db_uid_col, db_id_col, db_id_uid_col]] \
        .drop_duplicates().reset_index(drop=True)

    source_id_melt = melt_source_id(
        source_id, source_uid_col, source_id_col)

    gap_index, substr_match = id_gc(db_df=db_id,
                                    source_df=source_id_melt,
                                    source_uid_col=source_uid_col,
                                    db_uid_col=db_uid_col,
                                    source_id_col=source_id_col,
                                    dmi_content_col_format=db_id_col,
                                    uids_dict=uids_dict)

    rev_gap_index, substr_match = id_rgc(db_df=db_id,
                                         source_df=source_id_melt,
                                         source_uid_col=source_uid_col,
                                         db_uid_col=db_uid_col,
                                         source_id_col=source_id_col,
                                         dmi_content_col_format=db_id_col,
                                         substr_match=substr_match,
                                         uids_dict=uids_dict)

    source_gap = source_id_melt.loc[gap_index, :]
    dmi_gap = db_id.loc[rev_gap_index, :]

    substr_df_cols = list(source_gap.columns) + ["vs."] + list(dmi_gap.columns)
    substr_df = pd.DataFrame(substr_match, columns=substr_df_cols)

    logger.info(f"Length of dob_dmi id_comp is: {str(len(db_id))}")
    logger.info(
        f"Length of source_id_melt id_comp is: {str(len(source_id_melt))}")
    logger.info(f"Length of gap_index id_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index id_comp is: {str(len(rev_gap_index))}")

    id_comp_cols = list(source_id_melt.columns) + \
        ["vs."] + list(db_id.columns) + ["Validation"]
    id_comp = pd.DataFrame([], columns=id_comp_cols)

    dmi_uid = db_df.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap_kwargs = {
        db_peid_col: source_gap[source_uid_col].apply(
            lambda x: peid_uid_dict.get(x, ""))
        .values
    }
    source_gap = source_gap.assign(**source_gap_kwargs)

    id_comp = pd.concat([id_comp, source_gap, substr_df, dmi_gap], sort=False) \
        .fillna("") \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .reset_index(drop=True)

    return id_comp, source_id_melt, db_id


def melt_source_id(source_df, source_uid_col, source_id_col):
    melted_list = []
    for i in range(0, source_df.shape[0]):
        row_uid = source_df.loc[i, source_uid_col]
        row_content_list = source_df.loc[i, source_id_col]
        for row_content in row_content_list:
            entry = {
                source_uid_col: row_uid,
                source_id_col: row_content
            }
            melted_list.append(entry)
    return pd.DataFrame(melted_list).drop_duplicates().reset_index(drop=True)


def check_substring(name, to_check_namelist):
    """
    Checks if the names match, but in a a different order.
    """
    name_substr = set(name_cleanup(name).split(" "))
    for i, to_check_name in enumerate(to_check_namelist):
        if (name in to_check_name) or (to_check_name in name):
            return i
        to_check_substr = set(name_cleanup(to_check_name).split(" "))
        if name_substr == to_check_substr:
            return i
    return -1


def name_cleanup(name):
    """
    Cleans up name formatting to allow for better name comparison.
    """
    return name.replace(",", "") \
        .replace(".", "") \
        .replace("'", "") \
        .replace("\"", "") \
        .replace("-", "") \
        .strip().upper()


def get_substr_row(source_df, source_row_num, db_df, db_row_num):
    source_row = source_df.iloc[source_row_num, :].tolist()
    db_row = db_df.iloc[db_row_num, :].tolist()
    substr_row = source_row + [""] + db_row
    return substr_row


def check_letters(name, to_check_content_list):
    name_letters = sorted(name_cleanup(name).replace(" ", ""))
    for i, to_check_name in enumerate(to_check_content_list):
        to_check_letters = sorted(name_cleanup(to_check_name).replace(" ", ""))
        if name_letters == to_check_letters:
            return i
    return -1


def id_gc(db_df, source_df,
          source_uid_col, db_uid_col,
          source_id_col, dmi_content_col_format,
          uids_dict,
          substr_match=[], gap_index=[]):
    db_uid_count_dict = db_df.groupby(db_uid_col)[dmi_content_col_format]\
        .apply(list).to_dict()
    db_uid_index_dict = db_df.reset_index().groupby(db_uid_col)['index']\
        .apply(list).to_dict()
    gap_index = []
    for row in tqdm(source_df.itertuples(), total=source_df.shape[0]):
        i = row.Index
        source_uid = getattr(row, source_uid_col)

        uids = uids_dict.get(source_uid, [source_uid])
        sub_db_uid_index_dict = {}
        uid_index_list = []
        for uid in uids:
            uid_index_list += db_uid_index_dict.get(uid, [])
        sub_db_uid_index_dict[source_uid] = uid_index_list

        content_source = getattr(row, source_id_col)
        content_list = []
        for uid in uids:
            content_list += db_uid_count_dict.get(uid, [])
        if content_list != []:
            if content_source not in content_list:
                # print(f"source: {source_uid}, db: {str(content_list)}")
                substr_idx = check_substring(content_source, content_list)
                db_index = sub_db_uid_index_dict[source_uid]
                letter_idx = check_letters(content_source, content_list)

                if substr_idx != -1:
                    substr_row = get_substr_row(
                        source_df, i, db_df, db_index[substr_idx])
                    substr_match.append(substr_row)
                elif letter_idx != -1:
                    substr_row = get_substr_row(
                        source_df, i, db_df, db_index[letter_idx])
                    substr_match.append(substr_row)
                else:
                    gap_index.append(i)

        else:
            gap_index.append(i)
    return gap_index, substr_match


def id_rgc(db_df, source_df,
           source_uid_col, db_uid_col,
           source_id_col, dmi_content_col_format,
           uids_dict,
           substr_match=[], rev_gap_index=[]):
    rev_gap_index = []
    source_uid_count_dict = source_df.groupby(source_uid_col)[source_id_col]\
        .apply(list).to_dict()
    source_uid_index_dict = source_df.reset_index().groupby(source_uid_col)['index']\
        .apply(list).to_dict()
    for row in tqdm(db_df.itertuples(), total=db_df.shape[0]):
        i = row.Index
        db_uid = getattr(row, db_uid_col)

        uids = uids_dict.get(db_uid, [db_uid])
        sub_source_uid_index_dict = {}
        uid_index_list = []
        for uid in uids:
            uid_index_list += source_uid_index_dict.get(uid, [])
        sub_source_uid_index_dict[db_uid] = uid_index_list

        content_db = getattr(row, dmi_content_col_format)
        content_list = []
        for uid in uids:
            content_list += source_uid_count_dict.get(uid, [])
        if content_list != []:
            if content_db not in content_list:
                substr_idx = check_substring(content_db, content_list)
                source_index = source_uid_index_dict[db_uid]
                letter_idx = check_letters(content_db, content_list)
                if substr_idx != -1:
                    substr_row = get_substr_row(
                        source_df, source_index[substr_idx], db_df, i)
                    if substr_row not in substr_match:
                        substr_match.append(substr_row)
                elif letter_idx != -1:
                    substr_row = get_substr_row(
                        source_df, source_index[letter_idx], db_df, i)
                    substr_match.append(substr_row)
                else:
                    rev_gap_index.append(i)
        else:
            rev_gap_index.append(i)
    return rev_gap_index, substr_match
