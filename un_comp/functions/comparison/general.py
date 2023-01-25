from tqdm import tqdm
import pandas as pd

import logging

logger = logging.getLogger(__name__)


def get_peid_uid_dict(dmi_uid, db_uid_col, db_peid_col):
    dictionary = {}
    for _, row in dmi_uid.iterrows():
        dictionary[row[db_uid_col]] = str(row[db_peid_col])
    return dictionary


def get_translate_df(translate_df):
    out = {}
    for i in range(0, translate_df.shape[0]):
        out[translate_df.iloc[i, 0]] = translate_df.iloc[i, 1]
    return out


def get_uids_dict(db_df, db_peid_col, db_uid_col, **kwargs):
    uids_dict = {}
    for peid in set(db_df[db_peid_col]):
        uids = sorted(db_df[db_df[db_peid_col]==peid][db_uid_col].unique())
        for uid in uids:
            uids_dict[uid] = uids
    return uids_dict


def content_gc(db_df, source_df,
               source_uid_col, db_uid_col,
               source_content_col, dmi_content_col_format,
               uids_dict
               ):
    db_uid_count_dict = db_df.groupby(db_uid_col)[dmi_content_col_format] \
        .apply(list).to_dict()
    gap_index = []
    for row in tqdm(source_df.itertuples(), total=source_df.shape[0]):
        i = row.Index
        source_uid = getattr(row, source_uid_col)
        uids = uids_dict.get(source_uid, [source_uid])
        content_source = getattr(row, source_content_col)
        content_list = []
        for uid in uids:
            content_list += db_uid_count_dict.get(uid, [])
        if content_list != []:
            if content_source not in content_list:
                gap_index.append(i)
        else:
            gap_index.append(i)
    return gap_index


def content_rgc(db_df, source_df,
                source_uid_col, db_uid_col,
                source_content_col, dmi_content_col_format,
                uids_dict
                ):
    rev_gap_index = []
    source_uid_count_dict = source_df.groupby(source_uid_col)[source_content_col]\
        .apply(list).to_dict()
    for row in tqdm(db_df.itertuples(), total=db_df.shape[0]):
        i = row.Index
        db_uid = getattr(row, db_uid_col)
        uids = uids_dict.get(db_uid, [db_uid])
        content_db = getattr(row, dmi_content_col_format)
        content_list = []
        for uid in uids:
            content_list += source_uid_count_dict.get(uid, [])
        if content_list != []:
            if content_db not in content_list:
                rev_gap_index.append(i)
        else:
            rev_gap_index.append(i)
    return rev_gap_index

def name_gc(db_df, source_df,
            source_uid_col, db_uid_col,
            source_name_col, dmi_name_col,
            uids_dict,
            substr_match):
    gap_index=[]
    db_uid_count_dict = db_df.groupby(db_uid_col)[dmi_name_col]\
        .apply(list).to_dict()
    db_uid_index_dict = db_df.reset_index().groupby(db_uid_col)['index']\
        .apply(list).to_dict()
    for row in tqdm(source_df.itertuples(), total=source_df.shape[0]):
        i = row.Index
        source_uid = getattr(row, source_uid_col)

        uids = uids_dict.get(source_uid, [source_uid])
        sub_db_uid_index_dict = {}
        uid_index_list = []
        for uid in uids:
            uid_index_list += db_uid_index_dict.get(uid, [])
        sub_db_uid_index_dict[source_uid] = uid_index_list

        source_name = getattr(row, source_name_col)
        name_list = []
        for uid in uids:
            name_list += db_uid_count_dict.get(uid, [])
        if name_list != []:
            if source_name not in name_list:
                substr_idx = check_substring(source_name, name_list)
                db_index = sub_db_uid_index_dict[source_uid]
                letter_idx = check_letters(source_name, name_list)
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


def name_rgc(db_df, source_df,
             source_uid_col, db_uid_col,
             source_name_col, dmi_name_col,
             uids_dict,
             substr_match):
    rev_gap_index=[]
    source_uid_count_dict = source_df.groupby(source_uid_col)[source_name_col]\
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

        db_name = getattr(row, dmi_name_col)
        name_list = []
        for uid in uids:
            name_list += source_uid_count_dict.get(uid, [])
        if name_list != []:
            if db_name not in name_list:
                substr_idx = check_substring(db_name, name_list)
                source_index = sub_source_uid_index_dict[db_uid]
                letter_idx = check_letters(db_name, name_list)
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


def surname_gap(source_name_melt, db_name, db_df,
                source_uid_col, source_name_col, source_surname_col, source_nametype_col, source_profiletype_col,
                db_uid_col, db_name_col, db_surname_col, db_nametype_col, db_peid_col):
    
    source_surname_melt = source_name_melt\
        [(~source_name_melt[source_nametype_col].isin(["Low Quality AKA","OSN"]))\
        & (source_name_melt[source_surname_col] != "") & (source_name_melt[source_surname_col].notna())].reset_index(drop=True)
        
    source_surname_col_format = source_surname_col + "Format"
    source_surname_melt[source_surname_col_format] = source_surname_melt[source_surname_col].apply(
        lambda x: x.upper())
        
    source_name_col_format = source_name_col + "Format"
    source_surname_melt[source_name_col_format] = source_surname_melt[source_name_col].apply(
        lambda x: x.upper())
        
    db_surname = db_name[(~db_name[db_nametype_col].isin(["Low Quality AKA","OSN"])) & (db_name[db_surname_col] != "")].\
                                            drop_duplicates().reset_index(drop=True)

    db_surname_col_format = db_surname_col + "Format"
    db_surname[db_surname_col_format] = db_surname[db_surname_col].apply(
        lambda x: x.upper())

    db_name_col_format = db_name_col + "Format"
    db_surname[db_name_col_format] = db_surname[db_name_col].apply(
        lambda x: x.upper())

    name_comp_cols = list(source_surname_melt.columns) + \
        ["vs."] + list(db_surname.columns) + ["Validation"]
    surname_comp = pd.DataFrame([], columns=name_comp_cols)

    for row in tqdm(source_surname_melt.itertuples(), total=source_surname_melt.shape[0]):
        i = row.Index
        source_uid = getattr(row, source_uid_col)
        source_fullname = getattr(row, source_name_col_format)
        source_surname = getattr(row, source_surname_col_format)
        db_match = db_surname[(db_surname[db_uid_col] == source_uid) & \
                            (db_surname[db_name_col_format] == source_fullname)]
        if db_match.shape[0]==0:
            pass
        else:
            surname_db = db_match[db_surname_col_format].values[0]
            db_index = list(db_match.index)[0]
            if source_surname != surname_db:
                row_to_add = list(source_surname_melt.iloc[i,:].values) + [""] + list(db_surname.iloc[db_index,:].values) + [""]
                surname_comp = pd.concat([surname_comp, pd.DataFrame([row_to_add], columns=name_comp_cols)])

    for row in tqdm(db_surname.itertuples(), total=db_surname.shape[0]):
        i = row.Index
        db_uid = getattr(row, db_uid_col)
        fullname_db = getattr(row, db_name_col_format)
        surname_db = getattr(row, db_surname_col_format)
        source_match = source_surname_melt[(source_surname_melt[source_uid_col] == db_uid) & \
                                        (source_surname_melt[source_name_col_format] == fullname_db)]
        if source_match.shape[0]==0:
            pass
        else:
            surname_source = source_match[source_surname_col_format].values[0]
            source_index = list(source_match.index)[0]
            if surname_db != surname_source:
                row_to_add = list(source_surname_melt.iloc[source_index,:].values) + [""] + list(db_surname.iloc[i,:].values) + [""]
                surname_comp = pd.concat([surname_comp, pd.DataFrame([row_to_add], columns=name_comp_cols)])

    surname_comp = surname_comp.fillna("") \
        .drop([source_name_col_format, source_surname_col_format, source_profiletype_col,
                db_name_col_format, db_surname_col_format], axis=1) \
        .astype({db_peid_col: str}) \
        .sort_values(by=[db_peid_col, source_uid_col, db_uid_col]) \
        .drop_duplicates() \
        .reset_index(drop=True)

    logger.info(f"Length of surname_comp is: {str(len(surname_comp))}")

    return surname_comp, source_surname_melt, db_surname


def name_cleanup(name):
    """
    Cleans up name formatting to allow for better name comparison.
    """
    return name.replace(",", "") \
        .replace(".", "") \
        .replace("'", "") \
        .replace("\"", "") \
        .replace("-", "") \
        .replace(";","")\
        .strip().upper()


def check_substring(name, to_check_namelist):
    """
    Checks if the names match, but in a different order.
    """
    name_substr = set(name_cleanup(name).split(" "))
    for i, to_check_name in enumerate(to_check_namelist):
        if (name in to_check_name) or (to_check_name in name):
            return i
        to_check_substr = set(name_cleanup(to_check_name).split(" "))
        if name_substr == to_check_substr:
            return i
    return -1


def check_letters(name, to_check_name_list):
    name_letters = sorted(name_cleanup(name).replace(" ", ""))
    for i, to_check_name in enumerate(to_check_name_list):
        to_check_letters = sorted(name_cleanup(to_check_name).replace(" ", ""))
        if name_letters == to_check_letters:
            return i
    return -1


def get_substr_row(source_df, source_row_num, db_df, db_row_num):
    source_row = source_df.iloc[source_row_num, :].tolist()
    db_row = db_df.iloc[db_row_num, :].tolist()
    substr_row = source_row + [""] + db_row
    return substr_row


def check_executable(args, pysuite, cur_date, tmp_path, date_latest_executable, cur_time, log_filename,
                    GDriveModule, GmailModule, executable_name, dmi_comp_name, CONFIG):
    if args.google == True and args.email == True:
        google_client = pysuite.Client(client_secret_file="./client-token.json")
        exe_folder_id = CONFIG["executable-folder-id"]
        out_file_details = GDriveModule(
            google_client, exe_folder_id, executable_name, cur_date, tmp_path
        ).get_last_run_drive(exe_folder_id, executable_name, ".zip")
        latest_executable = out_file_details['fileName']
        print(f"Latest executable in G Drive: {latest_executable}")
        logger.info(f"Latest executable in G Drive: {latest_executable}")

        if date_latest_executable != latest_executable.replace(".zip","")[-len(date_latest_executable):]:
            logger.exception(f"The version of script/executable does not match!\n\
                Please use the latest executable - {latest_executable}.")
            if args.google:
                if args.testing:
                    gmail_module = GmailModule(google_client)
                    subject = f"DMI {dmi_comp_name} Error Log " + cur_date
                    body = f"Attached Log File contains the Error Log for {dmi_comp_name} Comparison Result generated at {cur_time}."
                    message = gmail_module.new_message(
                        to=[gmail_module.userId],
                        cc=CONFIG["bcc-email"],
                        subject=subject,
                        plain=body,
                        files=log_filename
                    )
                    res = gmail_module.send(message)
                    logger.info(f"Error Log Gmail response status: {res}")
                else:
                    gmail_module = GmailModule(google_client)
                    subject = f"DMI {dmi_comp_name} Error Log " + cur_date
                    body = f"Attached Log File contains the Error Log for {dmi_comp_name} Comparison Result generated at {cur_time}."
                    message = gmail_module.new_message(
                        to=[gmail_module.userId],
                        cc=CONFIG["cc-email"],
                        subject=subject,
                        plain=body,
                        files=log_filename
                    )
                    res = gmail_module.send(message)
                    logger.info(f"Error Log Gmail response status: {res}")
            raise Exception(f"The version of script/executable does not match!\n\
                Please use the latest executable - {latest_executable}.")

    return out_file_details