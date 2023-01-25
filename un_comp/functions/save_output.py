from typing import Type
from xlrd.biffh import XLRDError

import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from pysuite import SpreadSheetApp

logger = logging.getLogger(__name__)


def convert_columns(source_df):
    out_df = source_df
    for i in range(0, source_df.shape[0]):

        for j in range(0, source_df.shape[1]):

            if type(out_df.iloc[i, j]) is list:
                try:
                    out_df.iloc[i, j] = "; ".join(out_df.iloc[i, j])

                except TypeError as e:
                    print(out_df.iloc[i, j])
                    raise e

    return out_df


def get_last_run(output_path, filename):
    date_list = []
    for file in os.listdir(path=output_path):
        if filename in file:
            date = file.split('-')[1].split('.')[0].strip()
            date_list.append(date)
    if len(date_list) != 0:
        return max(date_list).strip()
    else:
        return None


def update_name_stats(
        google_client, name_stats, kpi_sheet_id,
        kpi_sheet_name, header_row_index,
        name_type_index, profile_count=None):

    spreadsheet_app = SpreadSheetApp(google_client, supportsTeamDrives=True)
    kpi_spreadsheet = spreadsheet_app.open_by_id(kpi_sheet_id)

    kpi_sheet = kpi_spreadsheet.open_by_title(kpi_sheet_name)

    last_index = kpi_sheet.last_index()
    non_blank_range = kpi_sheet.get_values(
        header_row_index, 1, last_index[0], 1)

    last_row = kpi_sheet.get_values(
        len(non_blank_range) + header_row_index - 1,
        0,
        len(non_blank_range) + header_row_index,
        last_index[1]
    )[0]

    last_row_entry = format_last_row(
        last_row, name_stats, name_type_index)

    logger.info(f"last_row_entry: {last_row_entry}")

    kpi_sheet.set_values(
        len(non_blank_range) + header_row_index,
        0,
        last_row_entry
    )

def update_pwc_log(
        google_client, kpi_sheet_id, subject, file_link,
        kpi_sheet_name, header_row_index,
        provenance, profile_count=None):

    spreadsheet_app = SpreadSheetApp(google_client, supportsTeamDrives=True)
    kpi_spreadsheet = spreadsheet_app.open_by_id(kpi_sheet_id)

    pwc_sheet = kpi_spreadsheet.open_by_title(kpi_sheet_name)

    last_index = pwc_sheet.last_index()
    non_blank_range = pwc_sheet.get_values(
        header_row_index, 1, last_index[0], 1)

    last_row_entry = pwc_last_row(
        provenance, subject, file_link)

    logger.info(f"last_row_entry: {last_row_entry}")

    pwc_sheet.set_values(
        len(non_blank_range) + header_row_index,
        0,
        last_row_entry
    )

def pwc_last_row(provenance, subject, file_link):
    cur_date = datetime.utcnow()
    col_1 = '{d.month}/{d.day}/{d.year}'.format(d=cur_date - timedelta(days = cur_date.weekday()))
    col_2 = provenance
    col_3 = subject
    col_4 = file_link
    col_5 = '{d.month}/{d.day}/{d.year} {d.hour}:{d.minute:02}'.format(d=datetime.utcnow())

    return [[col_1, col_2, col_3, col_4, col_5]]

def input_entry(last_row, last_row_entry, entry_count, entry_idx):
    try:
        prev_count = int(last_row[entry_idx])

    except IndexError:
        prev_count = entry_count

    if len(last_row_entry) < entry_idx:
        while(len(last_row_entry) < entry_idx):
            last_row_entry.append("")

    last_row_entry.append(entry_count)

    count_delta = entry_count - prev_count
    last_row_entry.append(count_delta)

    return last_row_entry


def format_last_row(last_row, name_stats, name_type_index):
    dt = datetime.now()
    row_entry = [dt.strftime("%m/%d/%Y")]

    for k, v in name_type_index.items():
        row_entry = input_entry(
            last_row, row_entry, name_stats[k], v)

    return [row_entry]


def save_raw(writer, dataframes, sheet_names):
    if type(dataframes) is list:
        for db, sheet_name in zip(dataframes, sheet_names):
            db.to_excel(writer, sheet_name=sheet_name, index=False)
    elif type(dataframes) is pd.DataFrame:
        dataframes.to_excel(writer, sheet_name=sheet_names, index=False)
    else:
        raise TypeError(
            "Passed input is neither a Pandas DataFrame nor a list of DataFrames")


def update_validation(last_run, cur_comp, sheet_name, function, filename,
                      prev_file_path, kwargs={}):
    if last_run is not None:
        try:
            prev_comp = pd.read_excel(
                prev_file_path, sheet_name=sheet_name,
                dtype="object"
            ).fillna("")

            if kwargs.get("source_content_col") is not None:
                prev_comp_source_dict = create_source_val_dict(
                    prev_comp, **kwargs)
            else:
                prev_comp_source_dict = create_uid_val_dict(
                    prev_comp, **kwargs)

            prev_comp_db_dict = create_uid_val_dict(prev_comp, **kwargs)

            cur_comp['Validation'] = cur_comp.apply(
                lambda x: function(x, prev_comp_source_dict, prev_comp_db_dict, **kwargs), axis=1)

        except XLRDError:
            logger.error(
                f"{sheet_name} is not present in previous comparison.")

    return cur_comp


def count_blank_validation(cur_comp):
    return (cur_comp["Validation"] == "").sum()


def find_valid_uid(row, prev_comp_source_dict, prev_comp_db_dict,
                   source_uid_col, db_content_uid_col):
    try:
        if row[source_uid_col] != "":
            return prev_comp_source_dict[row[source_uid_col]]

        elif row[db_content_uid_col] != "":
            return prev_comp_db_dict[row[db_content_uid_col]]
    except KeyError:
        return ""


def create_source_val_dict(prev_comp, source_uid_col, source_content_col,
                           **kwargs):
    out_dict = {}
    for row in prev_comp.itertuples():
        source_uid = getattr(row, source_uid_col)
        source_content = getattr(row, source_content_col)
        source_valid = row.Validation

        if (source_uid != "") & (source_content != ""):
            if out_dict.get(source_uid) is None:
                out_dict[source_uid] = {
                    source_content: source_valid
                }
            else:
                if out_dict[source_uid].get(source_content) is None:
                    out_dict[source_uid][source_content] = source_valid
                else:
                    pass

    return out_dict


def create_uid_val_dict(prev_comp, db_content_uid_col,
                        **kwargs):
    out_dict = {}
    for row in prev_comp.itertuples():
        if type(db_content_uid_col) is list:
            db_content_uid = get_uid_col(row, db_content_uid_col)

        else:
            db_content_uid = getattr(row, db_content_uid_col)

        db_valid = row.Validation

        if db_content_uid != "":
            if out_dict.get(db_content_uid) is None:
                out_dict[db_content_uid] = db_valid

            else:
                pass

    return out_dict


def get_uid_col(row, uid_cols):
    out_list = []
    for uid_col in uid_cols:
        if type(row) is pd.Series:
            atr = row[uid_col]

        else:
            atr = getattr(row, uid_col)

        if atr == "":
            return ""

        else:
            out_list.append(str(int(atr)))

    return "-".join(out_list)


def find_valid_content(row, prev_comp_source_dict, prev_comp_db_dict,
                       source_uid_col, source_content_col,
                       db_content_uid_col):
    try:
        valid_status = ""
        if type(db_content_uid_col) is list:
            db_content_uid = get_uid_col(row, db_content_uid_col)

        else:
            db_content_uid = row[db_content_uid_col]

        if db_content_uid != "":
            valid_status = prev_comp_db_dict[db_content_uid]

        else:
            valid_status = prev_comp_source_dict[
                row[source_uid_col]
            ][row[source_content_col]]

        return valid_status
    except KeyError:
        return ""


def save_comp(comp_res, source_df, db_df, last_run, writer, filename,
              function, raw_sheet_name, comp_sheet_name, prev_file_path,
              update_val=True, kwargs={}):

    if update_val:
        if len(comp_res) != 0:
            comp_res_updated = update_validation(
                last_run,
                prev_file_path=prev_file_path,
                cur_comp=comp_res,
                sheet_name=comp_sheet_name,
                function=function,
                filename=filename,
                kwargs=kwargs)
        else:
            comp_res_updated = comp_res
    else:
        comp_res_updated = comp_res

    source_df.to_excel(writer, sheet_name=raw_sheet_name, index=False)
    db_df.to_excel(writer, sheet_name=raw_sheet_name,
                   index=False, startcol=source_df.shape[1] + 1)

    comp_res_updated.to_excel(
        writer, sheet_name=comp_sheet_name, index=False)

    blank_validation = count_blank_validation(comp_res_updated)
    return blank_validation


def save_timestamps_df(writer, labels, timestamps):
    prev_time = timestamps[0]
    rest_time = timestamps[1:]
    out = []
    for row in zip(labels, rest_time):
        time_lapsed = row[1] - prev_time
        out.append({
            "Label": row[0],
            "Time Taken": time_lapsed,
            "Units": "seconds"
        })
        prev_time = row[1]
    out.append({
        "Label": "Total Time Taken",
        "Time Taken": timestamps[-1] - timestamps[0],
        "Units": "seconds"
    })
    pd.DataFrame(out).to_excel(
        writer, sheet_name="TimeStamps", index=False
    )
