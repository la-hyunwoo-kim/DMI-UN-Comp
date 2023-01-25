from un_comp.functions.comparison.general import get_peid_uid_dict, \
    name_gc, name_rgc, surname_gap

import pandas as pd

import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def name_comp(
        source_df, db_df, uids_dict,
        name_format_dict, source_uid_col, source_profiletype_col, source_pn_col,
        source_aka_col, source_lqaka_col, source_name_col, source_nametype_col, source_surname_col, source_aka_surname_col,
        source_osn_col, db_peid_col, db_uid_col, db_nametype_col, db_name_col, db_surname_col,
        db_name_uid_col, pn_tag, aka_tag, lqaka_tag, osn_tag, **kwargs
):
    source_name = source_df.loc[:, [source_uid_col, source_profiletype_col, source_pn_col,
                                    source_aka_col, source_lqaka_col, source_osn_col,
                                    source_surname_col, source_aka_surname_col]]

    source_name_melt = melt_source_name(
        source_name, source_uid_col, source_profiletype_col, source_name_col, source_nametype_col,
        source_pn_col, source_aka_col, source_osn_col, pn_tag, aka_tag,
        osn_tag, name_format_dict, source_surname_col, source_aka_surname_col
    )
    name_stats = {
        "name-count": source_name_melt.shape[0],
        "lqaka-count": source_name_melt.loc[source_name_melt[source_nametype_col]
                                            == lqaka_tag, :] .shape[0],
        "aka-count": source_name_melt.loc[source_name_melt[source_nametype_col]
                                          == aka_tag, :].shape[0],
        "pn-count": source_name_melt.loc[source_name_melt[source_nametype_col]
                                         == pn_tag, :].shape[0],
        "osn-count": source_name_melt.loc[source_name_melt[source_nametype_col]
                                          == osn_tag, :].shape[0]
    }

    db_name = db_df.loc[
        (db_df[db_nametype_col] != lqaka_tag) & (db_df["ParentNameType"] != "Low Quality AKA"),
        [db_peid_col, db_uid_col, db_nametype_col,db_name_col, db_name_uid_col, db_surname_col]
        ] \
        .drop_duplicates().reset_index(drop=True)

    source_name_col_format = source_name_col + "Format"
    db_content_col_format = db_name_col + "Format"

    source_name_melt[source_name_col_format] = source_name_melt[source_name_col].apply(
        lambda x: x.upper())
    db_name[db_content_col_format] = db_name[db_name_col].apply(
        lambda x: x.upper())

    source_name_melt = source_name_melt.drop_duplicates()\
        .sort_values([source_uid_col, source_name_col]).reset_index(drop=True)
    db_name = db_name.drop_duplicates()\
        .sort_values([db_uid_col, db_name_col]).reset_index(drop=True)

    gap_index, substr_match_gc = name_gc(db_name, source_name_melt,
                                      source_uid_col, db_uid_col,
                                      source_name_col_format,
                                      db_content_col_format,
                                      uids_dict,
                                      substr_match=[])

    rev_gap_index, substr_match_rgc = name_rgc(db_name, source_name_melt,
                                           source_uid_col, db_uid_col,
                                           source_name_col_format,
                                           db_content_col_format,
                                           uids_dict,
                                           substr_match=[])
    
    surname_comp, source_surname_melt, db_surname = surname_gap(source_name_melt, db_name, db_df,
                source_uid_col, source_name_col, source_surname_col, source_nametype_col, source_profiletype_col,
                db_uid_col, db_name_col, db_surname_col, db_nametype_col, db_peid_col)

    source_gap = source_name_melt.iloc[gap_index, :]
    dmi_gap = db_name.iloc[rev_gap_index, :]

    substr_df_cols = list(source_gap.columns) + ["vs."] + list(dmi_gap.columns)
    substr_df_gc = pd.DataFrame(substr_match_gc, columns=substr_df_cols)
    substr_df_rgc = pd.DataFrame(substr_match_rgc, columns=substr_df_cols)

    logger.info(f"Length of db_name name_comp is: {str(len(db_name))}")
    logger.info(
        f"Length of source_name_melt name_comp is: {str(len(source_name_melt))}")
    logger.info(f"Length of gap_index name_comp is: {str(len(gap_index))}")
    logger.info(
        f"Length of rev_gap_index name_comp is: {str(len(rev_gap_index))}")
    logger.info(
        f"Length of substr_match name_comp_gc is: {str(len(substr_match_gc))}")
    logger.info(
        f"Length of substr_match name_comp_rgc is: {str(len(substr_match_rgc))}")

    name_comp_cols = list(source_gap.columns) + \
        ["vs."] + list(dmi_gap.columns) + ["Validation"]
    name_comp = pd.DataFrame([], columns=name_comp_cols)

    dmi_uid = db_df.loc[:, [db_peid_col, db_uid_col]].drop_duplicates() \
        .reset_index(drop=True)
    peid_uid_dict = get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col)

    source_gap_kwargs = {
        db_peid_col: source_gap[source_uid_col].apply(
            lambda x: peid_uid_dict.get(x, ""))
        .values
    }
    source_gap = source_gap.assign(**source_gap_kwargs)

    name_comp_gc = pd.concat([name_comp, source_gap, substr_df_gc], sort=False) \
        .fillna("") \
        .drop([db_content_col_format, source_name_col_format, source_surname_col, db_surname_col], axis=1) \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .drop_duplicates() \
        .reset_index(drop=True)

    name_comp_rgc = pd.concat([name_comp,  dmi_gap, substr_df_rgc], sort=False) \
        .fillna("") \
        .drop([db_content_col_format, source_name_col_format, source_surname_col, db_surname_col], axis=1) \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .drop_duplicates() \
        .reset_index(drop=True)

    return name_comp_gc, name_comp_rgc, source_name_melt, db_name, name_stats,\
        surname_comp, source_surname_melt, db_surname


def melt_source_name(
        source_df, source_uid_col, source_profiletype_col, source_name_col,
        source_nametype_col, source_pn_col, source_aka_col,
        source_osn_col, pn_tag, aka_tag, osn_tag, name_format_dict,
        source_surname_col, source_aka_surname_col
):
    melt_name_list = []
    for _, row in source_df.iterrows():
        un_uid = row[source_uid_col]
        pn = row[source_pn_col]
        surname = row[source_surname_col]
        profile_type = row[source_profiletype_col]
        aka_surname = row[source_aka_surname_col]
        if type(pn) is str:
            melt_name_list.append(
                {
                    source_uid_col: un_uid,
                    source_name_col: format_name(pn, name_format_dict),
                    source_nametype_col: pn_tag,
                    source_surname_col: format_name(surname, name_format_dict)
                }
            )
        else:
            for name, surname in zip(pn, surname):
                melt_name_list.append(
                    {
                        source_uid_col: un_uid,
                        source_name_col: format_name(name, name_format_dict),
                        source_nametype_col: pn_tag,
                        source_surname_col: format_name(surname, name_format_dict)
                    }
                )
        aka_list = row[source_aka_col]
        if aka_list != "":
            for aka, surname in zip(aka_list, aka_surname):
                aka = format_name(aka, name_format_dict)
                if aka != "":
                    if profile_type == "Individual":
                        aka = flip_name_order(aka)
                    melt_name_list.append(
                        {
                            source_uid_col: un_uid,
                            source_name_col: aka,
                            source_nametype_col: aka_tag,
                            source_profiletype_col: profile_type,
                            source_surname_col: surname
                        }
                    )

        if row[source_osn_col] != "":
            melt_name_list.append(
                {
                    source_uid_col: un_uid,
                    source_name_col: row[source_osn_col],
                    source_nametype_col: osn_tag,
                    source_profiletype_col: profile_type
                }
            )
    return pd.DataFrame(melt_name_list)


def format_name(name, name_format_dict):
    for k, v in name_format_dict.items():
        name = name.replace(k, v).strip()
    
    if len(name)>0 and '"' in name:
        if name[0]=='"' and name[-1]=='"':
            name = name.replace('"',"")

    return name


def flip_name_order(name):
    if ", " in name:
        surname = name.split(", ")[0].strip()
        given_name = name.split(", ")[1].strip()
        return given_name + " " + surname
    else:
        return name