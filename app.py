from un_comp.modules.SQLModule.classes import SQLModule
from un_comp.modules.EmailModule.classes import GmailModule
from un_comp.modules.ExcelModule.classes import GDriveModule

from un_comp.functions.source_import import get_raw_xml, xml2dict
from un_comp.functions.save_output import get_last_run, save_raw, \
    save_comp, find_valid_uid, find_valid_content, save_timestamps_df, \
    convert_columns, count_blank_validation, update_name_stats, update_pwc_log
from un_comp.functions.source_parse import parse_source_json
from un_comp.functions.comparison.general import get_uids_dict, check_executable
from un_comp.functions.comparison.uid_comp import uid_comp
from un_comp.functions.comparison.name_comp import name_comp
from un_comp.functions.comparison.lqaka_comp import lqaka_comp
from un_comp.functions.comparison.dob_comp import dob_comp
from un_comp.functions.comparison.id_comp import id_comp
from un_comp.functions.comparison.nationality_comp import nationality_comp
from un_comp.functions.comparison.residency_comp import residency_comp
from un_comp.functions.comparison.address_count_comp import compare_address_count
from un_comp.functions.comparison.pob_count_comp import compare_pob_count


import logging
import os
import json
from datetime import datetime
from pandas import ExcelWriter
import pandas as pd
import argparse
import pysuite

parser = argparse.ArgumentParser(
    description="Run UN Gap Check and Reverse Gap Check.")
parser.add_argument("--savesource", "-ss", action="store_true",
                    help="Saves all temporary results in test_out within out file")
parser.add_argument("--skipdl", "-sdl", action="store_true",
                    help="Use previously downloaded XML and SQL pull.")
parser.add_argument("--google", "-gg", action="store_true",
                    help="use Google Credentials to log in")
parser.add_argument("--email", "-em", action="store_true",
                    help="Send out email to recipients.")
parser.add_argument("--testing", "-t", action="store_true",
                    help="Use test-folder and test-email recipients")
args = parser.parse_args()

cur_time = datetime.utcnow().strftime("%Y%m%d %H%M")
cur_date = datetime.utcnow().strftime("%Y%m%d")

log_path = "./log/"
if not os.path.exists(log_path):
    os.mkdir(log_path)

log_filename = f"{log_path}DMI UN - {cur_date}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger(__name__)

out_path = "./out/"
if not os.path.exists(out_path):
    os.mkdir(out_path)

test_out_path = "./out/test-out/"
if not os.path.exists(test_out_path):
    os.mkdir(test_out_path)

tmp_path = "./out/tmp/"
if not os.path.exists(tmp_path):
    os.mkdir(tmp_path)

with open("./data/config.json", "r") as infile:
    CONFIG = json.load(infile)

with open("./data/formatting.json", "r", encoding="utf-8") as infile:
    FORMATTING = json.load(infile)

with open("./data/creds.json", "r") as infile:
    SQL_CREDS = json.load(infile)

with open("./data/var-list.json", "r") as infile:
    VARS = json.load(infile)

date_latest_executable = "20221229"
executable_name = "DMI UN Comp"
dmi_comp_name = "UN"
out_file_details = check_executable(args, pysuite, cur_date, tmp_path, date_latest_executable, cur_time, log_filename,
                GDriveModule, GmailModule, executable_name, dmi_comp_name, CONFIG)
            
sql_module = SQLModule(SQL_CREDS)
sql_path = "./data/DMI_UN.sql"

output_filename = CONFIG["output-filename"]
folder_id = CONFIG["folder-id"]

if not args.testing:
    subject = "DMI UN Comparison " + cur_date
    folder_id = CONFIG["folder-id"]
    kpi_sheet_id = CONFIG["kpi-spreadsheet-id"]

    print("Loading Default Configuration.")
    logger.info("Loading Default Configuration.")
else:
    subject = "DMI UN Test Comparison " + cur_date
    folder_id = CONFIG["test-folder-id"]
    kpi_sheet_id = CONFIG["kpi-spreadsheet-id-test"]

    print("Loading Testing Configuration.")
    logger.info("Loading Testing Configuration.")

if args.email:
    print(f"Email Subject: {subject}")
else:
    print(f"No Email Sent")
    logger.info("No Email Sent")

# Initialize Google Module
if args.google == True:
    google_client = pysuite.Client(
        client_secret_file="./client-token.json")
    g_drive_module = GDriveModule(
        google_client, folder_id, output_filename, cur_date, tmp_path)

print("Initialized all Modules")
logger.info("Initialized all Modules")

if __name__ == "__main__":
    try:
        if args.skipdl:
            un_dmi_df = pd.read_excel(
                f"{test_out_path}un_dmi_df.xlsx").fillna("")
            with open(f"{test_out_path}un_source_df.json", "r") as infile:
                un_source_json = json.load(infile)
            source_last_update = "SOURCE UPDATE DATE TESTING"
        else:
            un_xml = get_raw_xml(CONFIG["un-source-url"])

            sql_module.connect()
            un_dmi_df = sql_module.execute_sql(sql_path, fillna=True)

            un_source_json, source_last_update = xml2dict(
                un_xml, VARS["source_profiletype_col"])

        un_source_df = parse_source_json(un_source_json, **VARS)

        if args.savesource:
            with open(f"{test_out_path}un_source_df.json", "w+") as infile:
                json.dump(un_source_json, infile, indent=4)
            un_dmi_df.to_excel(f"{test_out_path}un_dmi_df.xlsx", index=False)
            print("Source saved!")
            logger.info("Source saved!")

        un_source_df = un_source_df.\
            sort_values([VARS['source_uid_col']]).reset_index(drop=True)
        un_dmi_df = un_dmi_df.drop_duplicates().\
            sort_values([VARS['db_uid_col'], VARS['db_name_col']]).reset_index(drop=True)
        
        uid_comp, source_uid, dmi_uid, profile_count = uid_comp(
            un_source_df, un_dmi_df, **VARS)

        uids_dict = get_uids_dict(un_dmi_df, **VARS)

        name_comp_gc, name_comp_rgc, source_name_melt, db_name, name_stats,\
        surname_comp, source_surname_melt, db_surname = name_comp(
            un_source_df, un_dmi_df, uids_dict, FORMATTING["name-formatting"], **VARS
        )

        lqaka_comp_gc, lqaka_comp_rgc, source_lqaka, db_lqaka = lqaka_comp(
            un_source_df, un_dmi_df, uids_dict, FORMATTING["name-formatting"], **VARS
        )

        dob_comp, source_dob, db_dob = dob_comp(
            un_source_df, un_dmi_df, uids_dict, FORMATTING["source-dob-format"], FORMATTING["db-dob-format"],
            **VARS
        )

        id_comp, source_id, db_id = id_comp(
            un_source_df, un_dmi_df, uids_dict, **VARS
        )

        nat_comp, source_nat, db_nat = nationality_comp(
            un_source_df, un_dmi_df, uids_dict, FORMATTING["country-format"],
            **VARS
        )

        residency_comp, source_residency, db_residency = residency_comp(
            un_source_df, un_dmi_df, uids_dict, FORMATTING["country-format"],
            **VARS
        )

        address_count_comp, source_address_count_df, db_address_count_df = compare_address_count(
            un_source_df, un_dmi_df, uids_dict, **VARS
        )

        pob_count_comp, source_pob_count_df, db_pob_count_df = compare_pob_count(
            un_source_df, un_dmi_df, uids_dict,
            **VARS
        )

        if args.google == True:
            last_run_file = g_drive_module.get_last_run_file()
            if last_run_file is not None:
                last_run = last_run_file["maxDate"]
                prev_file_path = f"{tmp_path}{last_run_file['fileName']}"
            else:
                last_run = None
                prev_file_path = None
            logger.info(f"Last run logged on Drive is {last_run}")
            output_file_path = f"{tmp_path}{output_filename} - {cur_date}.xlsx"
        else:
            last_run = get_last_run(out_path, output_filename)
            prev_file_path = f"{out_path}{output_filename} - {last_run}.xlsx"

            output_file_path = f"{out_path}{output_filename} - {cur_date}.xlsx"

        writer = ExcelWriter(output_file_path, engine="xlsxwriter", options={
            'strings_to_urls': False})

        un_source_df_converted = convert_columns(un_source_df)

        save_raw(writer, [un_source_df_converted, un_dmi_df],
                 ["UN-Source", "UN-DMI"])

        uid_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "db_content_uid_col": VARS["db_peid_col"]
        }
        uid_blank = save_comp(
            uid_comp, source_uid, dmi_uid, last_run, writer,
            output_filename, find_valid_uid, "UID Prep", "UID Comp",
            prev_file_path, kwargs=uid_comp_cols
        )

        name_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_name_col"],
            "db_content_uid_col": VARS["db_name_uid_col"]
        }
        source_name_melt = source_name_melt.drop([VARS["source_surname_col"]], axis=1)
        db_name = db_name.drop([VARS["db_surname_col"]], axis=1)
        name_blank_gc = save_comp(
            name_comp_gc, source_name_melt, db_name, last_run, writer,
            output_filename, find_valid_content, "Name Prep", "Name Comp GC",
            prev_file_path, kwargs=name_comp_cols
        )

        name_blank_rgc = save_comp(
            name_comp_rgc, source_name_melt, db_name, last_run, writer,
            output_filename, find_valid_content, "Name Prep", "Name Comp RGC",
            prev_file_path, kwargs=name_comp_cols
        )

        lqaka_blank_gc = save_comp(
            lqaka_comp_gc, source_lqaka, db_lqaka, last_run, writer,
            output_filename, find_valid_content, "LQAKA Prep", "LQAKA Comp GC",
            prev_file_path, kwargs=name_comp_cols
        )

        lqaka_blank_rgc = save_comp(
            lqaka_comp_rgc, source_lqaka, db_lqaka, last_run, writer,
            output_filename, find_valid_content, "LQAKA Prep", "LQAKA Comp RGC",
            prev_file_path, kwargs=name_comp_cols
        )

        surname_blank = save_comp(
            surname_comp, source_surname_melt, db_surname, last_run, writer,
            output_filename, find_valid_content, "Surname Prep", "Surname Comp",
            prev_file_path, kwargs=name_comp_cols
        )

        dob_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_dob_col"],
            "db_content_uid_col": VARS["db_dob_uid_col"]
        }
        dob_blank = save_comp(
            dob_comp, source_dob, db_dob, last_run, writer,
            output_filename, find_valid_content, "DoB Prep", "DoB Comp",
            prev_file_path, kwargs=dob_comp_cols
        )

        id_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_id_col"],
            "db_content_uid_col": VARS["db_id_uid_col"]
        }
        id_blank = save_comp(
            id_comp, source_id, db_id, last_run, writer,
            output_filename, find_valid_content, "ID Prep", "ID Comp",
            prev_file_path, kwargs=id_comp_cols
        )

        nat_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_nationality_col"],
            "db_content_uid_col": VARS["db_nationality_uid_col"]
        }
        nat_blank = save_comp(
            nat_comp, source_nat, db_nat, last_run, writer,
            output_filename, find_valid_content, "Nationality Prep", "Nationality Comp",
            prev_file_path, kwargs=nat_comp_cols
        )

        residency_comp_col = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_residency_col"],
            "db_content_uid_col": VARS["db_residency_uid_col"]
        }
        residency_blank = save_comp(
            residency_comp, source_residency, db_residency, last_run, writer,
            output_filename, find_valid_content, "Residency Prep", "Residency Comp",
            prev_file_path, kwargs=residency_comp_col
        )

        address_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_address_count_col"],
            "db_content_uid_col": [
                VARS["db_peid_col"], VARS["db_address_count_col"]
            ]
        }
        address_blank = save_comp(
            address_count_comp, source_address_count_df, db_address_count_df, last_run, writer,
            output_filename, find_valid_content, "Address Count Prep", "Address Count Comp",
            prev_file_path, kwargs=address_comp_cols
        )

        pob_comp_cols = {
            "source_uid_col": VARS["source_uid_col"],
            "source_content_col": VARS["source_pob_count_col"],
            "db_content_uid_col": VARS["db_uid_col"]
        }
        pob_blank = save_comp(
            pob_count_comp, source_pob_count_df, db_pob_count_df, last_run, writer,
            output_filename, find_valid_content, "PoB Count Prep", "PoB Count Comp",
            prev_file_path, kwargs=pob_comp_cols
        )

        writer.save()
        writer.close()

        logger.info(f"Saved Output at {output_file_path}")

        if args.google:
            res = g_drive_module.upload_report(output_file_path)

            if args.email:
                gmail_module = GmailModule(google_client)
                receiver, cc, bcc = gmail_module.get_email_details(
                    CONFIG, args)

                if type(res) is dict:
                    # File updates returns a dictionary
                    file_id = res["id"]
                else:
                    # File upload returns GDriveFile object
                    file_id = getattr(res, "fileId")
                file_link = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
                body = f"""
<html>
<head>
 <meta charset=utf-8>
 <style type="text/css">
 th, td {{
  padding: 5px;
  text-align: left;
  border-bottom: 1px solid #ddd;
}}
th {{
  background-color: #000080;
  color: white;
  width:50%
}}
table, th, td {{
  border-collapse: collapse;
}}
tr:nth-child(even) {{
  background-color: #f2f2f2;
}}
 </style>
 </head>
<p>
Hi Team,<br>
<br>
Attached Excel file contains UN Comparison Result generated at {cur_time} UTC.<br>
Source XML used last updated on {source_last_update}<br>
Script was last updated on "{date_latest_executable}".<br>
Latest executable available in Google Drive was "{out_file_details['fileName']}".<br>
<br>
Report link: {file_link}<br>
<br>
1. Name Statistics<br>
<table style="width:80%">
    <tr>
        <th>Name Type</th>
        <th>Name Count</th>
    </tr>
    <tr>
        <td>Primary Name</td>
        <td>{name_stats["pn-count"]}</td>
    </tr>
    <tr>
        <td>Good Quality AKA</td>
        <td>{name_stats["aka-count"]}</td>
    </tr>
    <tr>
        <td>Low Quality AKA</td>
        <td>{name_stats["lqaka-count"]}</td>
    </tr>
    <tr>
        <td>Original Script Name</td>
        <td>{name_stats["osn-count"]}</td>
    </tr>
    <tr>
        <td><b>Total Name Count</b></td>
        <td><b>{name_stats["name-count"]}</b></td>
    </tr>
</table>
<br>
2. Profile Count in DMI:<br>
<table style="width:80%">
    <tr>
        <th>Metric</th>
        <th>Count</th>
    </tr>
    <tr>
        <td>DMI Profile Count</td>
        <td>{profile_count}</td>
    </tr>
</table>
<br>
3. Number of blank Validation Rows for each Field is:<br>
<br>
</p>
<table style="width:80%">
    <tr>
        <th>Comparison Field</th>
        <th>Blank Row Count</th>
    </tr>
    <tr>
        <td>UID Comparison</td>
        <td>{uid_blank} Rows</td>
    </tr>
    <tr>
        <td>Name Comparison GC</td>
        <td>{name_blank_gc} Rows</td>
    </tr>
    <tr>
        <td>Name Comparison RGC</td>
        <td>{name_blank_rgc} Rows</td>
    </tr>
    <tr>
        <td>LQAKA Comparison GC</td>
        <td>{lqaka_blank_gc} Rows</td>
    </tr>
    <tr>
        <td>LQAKA Comparison RGC</td>
        <td>{lqaka_blank_rgc} Rows</td>
    </tr>
    <tr>
        <td>Surname Comparison</td>
        <td>{surname_blank} Rows</td>
    </tr>
    <tr>
        <td>Date of Birth Comparison</td>
        <td>{dob_blank} Rows</td>
    </tr>
    <tr>
        <td>Identification Details Comparison</td>
        <td>{id_blank} Rows</td>
    </tr>
    <tr>
        <td>Nationality/Citizenship Comparison</td>
        <td>{nat_blank} Rows</td>
    </tr>
    <tr>
        <td>Residency/Country of Affiliation Comparison</td>
        <td>{residency_blank} Rows</td>
    </tr>
    <tr>
        <td>Address Count Comparison</td>
        <td>{address_blank} Rows</td>
    </tr>
    <tr>
        <td>PoB Count Comparison</td>
        <td>{pob_blank} Rows</td>
    </tr>
</table>
</html>
                """
                message = gmail_module.new_message(
                    to=receiver,
                    subject=subject,
                    plain="",
                    html=body,
                    cc=cc,
                    bcc=bcc
                )
                res = gmail_module.send(message)
                
                name_stats["profile-count"] = profile_count

                update_name_stats(
                    google_client, name_stats, kpi_sheet_id,
                    CONFIG["un-kpi-sheet-name"],
                    CONFIG["header-row-index"],
                    CONFIG["name_type_index"]
                )

                update_pwc_log(
                    google_client, kpi_sheet_id, subject, file_link,
                    CONFIG["pwc-sheet-name"],
                    CONFIG["header-row-index"],
                    CONFIG["provenance"]
                )

                logger.info(f"Gmail response status: {res}")

            g_drive_module.clean_temp_folder()

    except Exception as e:
        logger.exception(e)
        if args.google:
            if args.email:
                cc = CONFIG["cc-email"]
                if args.testing:
                    cc = CONFIG["bcc-email"]
                gmail_module = GmailModule(google_client)
                subject = "DMI UN Error Log " + cur_date
                body = f"Attached Log File contains the Error Log for UN Comparison Result generated at {cur_time}."
                message = gmail_module.new_message(
                    to=[gmail_module.userId],
                    cc=cc,
                    subject=subject,
                    plain=body,
                    files=log_filename
                )
                res = gmail_module.send(message)
                logger.info(f"Gmail response status: {res}")
        raise e
