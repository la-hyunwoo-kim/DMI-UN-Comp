from un_comp.functions.comparison.general import get_peid_uid_dict, \
    name_gc, name_rgc

import pandas as pd

import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def lqaka_comp(
        source_df, db_df, uids_dict,
        name_format_dict, source_uid_col, 
        source_profiletype_col, source_lqaka_col, source_name_col, source_surname_col,
        source_nametype_col, db_peid_col, db_uid_col, db_nametype_col, db_surname_col,
        db_name_col, db_name_uid_col, lqaka_tag, **kwargs
):
    source_name = source_df.loc[source_df[source_lqaka_col] != "",
                                [source_uid_col, source_profiletype_col,
                                    source_lqaka_col]
                                ]

    source_lqaka_melt = melt_source_name(
        source_name, source_uid_col, source_profiletype_col, source_name_col,
        source_nametype_col, source_lqaka_col, lqaka_tag, name_format_dict
    )

    db_lqaka = db_df.loc[
        (db_df[db_nametype_col] == lqaka_tag) | (db_df["ParentNameType"] == "Low Quality AKA"),
        [db_peid_col, db_uid_col, db_nametype_col, db_name_col, db_name_uid_col]
        ] \
        .drop_duplicates().reset_index(drop=True)

    source_name_col_format = source_name_col + "Format"
    db_content_col_format = db_name_col + "Format"

    source_lqaka_melt[source_name_col_format] = source_lqaka_melt[source_name_col].apply(
        lambda x: x.upper())
    db_lqaka[db_content_col_format] = db_lqaka[db_name_col].apply(
        lambda x: x.upper())

    source_lqaka_melt = source_lqaka_melt.drop_duplicates().\
        sort_values([source_uid_col, source_name_col]).reset_index(drop=True)
    db_lqaka = db_lqaka.drop_duplicates().sort_values([db_uid_col, db_name_col]).reset_index(drop=True)

    gap_index, substr_match_gc = name_gc(db_lqaka, source_lqaka_melt,
                                      source_uid_col, db_uid_col,
                                      source_name_col_format,
                                      db_content_col_format,
                                      uids_dict,
                                      substr_match=[])

    rev_gap_index, substr_match_rgc = name_rgc(db_lqaka, source_lqaka_melt,
                                           source_uid_col, db_uid_col,
                                           source_name_col_format,
                                           db_content_col_format,
                                           uids_dict,
                                           substr_match=[])

    source_gap = source_lqaka_melt.iloc[gap_index, :]
    dmi_gap = db_lqaka.iloc[rev_gap_index, :]

    substr_df_cols = list(source_gap.columns) + ["vs."] + list(dmi_gap.columns)
    substr_df_gc = pd.DataFrame(substr_match_gc, columns=substr_df_cols)
    substr_df_rgc = pd.DataFrame(substr_match_rgc, columns=substr_df_cols)

    logger.info(f"Length of db_lqaka lqaka_comp is: {str(len(db_lqaka))}")
    logger.info(
        f"Length of source_lqaka_melt lqaka_comp is: {str(len(source_lqaka_melt))}")
    logger.info(f"Length of gap_index lqaka_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index lqaka_comp is: {str(len(rev_gap_index))}")
    logger.info(
        f"Length of substr_match lqaka_comp_gc is: {str(len(substr_match_gc))}")
    logger.info(
        f"Length of substr_match lqaka_comp_rgc is: {str(len(substr_match_rgc))}")

    lqaka_comp_cols = list(source_gap.columns) + \
        ["vs."] + list(dmi_gap.columns) + ["Validation"]
    lqaka_comp = pd.DataFrame([], columns=lqaka_comp_cols)

    dmi_uid = db_df.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap_kwargs = {
        db_peid_col: source_gap[source_uid_col].apply(
            lambda x: peid_uid_dict.get(x, ""))
        .values
    }
    source_gap = source_gap.assign(**source_gap_kwargs)

    lqaka_comp_gc = pd.concat([lqaka_comp, source_gap, substr_df_gc], sort=False) \
        .fillna("") \
        .drop([db_content_col_format, source_name_col_format], axis=1) \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .drop_duplicates() \
        .reset_index(drop=True)
    
    lqaka_comp_rgc = pd.concat([lqaka_comp, dmi_gap, substr_df_rgc], sort=False) \
        .fillna("") \
        .drop([db_content_col_format, source_name_col_format], axis=1) \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .drop_duplicates() \
        .reset_index(drop=True)

    return lqaka_comp_gc, lqaka_comp_rgc, source_lqaka_melt, db_lqaka


def melt_source_name(
        source_df, source_uid_col, source_profiletype_col, source_name_col,
        source_nametype_col, source_lqaka_col, lqaka_tag, name_format_dict
):
    melt_name_list = []
    for row in source_df.itertuples():
        un_uid = getattr(row, source_uid_col)
        profile_type = getattr(row, source_profiletype_col)
        lqaka_list = getattr(row, source_lqaka_col)
        if lqaka_list != "":
            for lqaka in lqaka_list:
                lqaka = format_name(lqaka, name_format_dict)
                if profile_type == "Individual":
                    lqaka = flip_name_order(lqaka)
                if lqaka != "":
                    melt_name_list.append(
                        {
                            source_uid_col: un_uid,
                            source_profiletype_col: profile_type,
                            source_name_col: lqaka,
                            source_nametype_col: lqaka_tag
                        }
                    )
    return pd.DataFrame(melt_name_list)


def flip_name_order(name):
    if ", " in name:
        surname = name.split(", ")[0].strip()
        given_name = name.split(", ")[1].strip()
        return given_name + " " + surname
    else:
        return name


def format_name(name, name_format_dict):
    for k, v in name_format_dict.items():
        name = name.replace(k, v).strip()
    
    if len(name)>0 and '"' in name:
        if name[0]=='"' and name[-1]=='"':
            name = name.replace('"',"")

    return name.strip()
