import os
import pandas as pd
import warnings
from datetime import datetime
from googleapiclient.http import MediaFileUpload
import pathlib
from pysuite.gdrive import DriveApp, GDriveFile

import logging
logger = logging.getLogger(__name__)


class GDriveModule(DriveApp):
    def __init__(self, google_client, folder_id, filename, cur_date, tmp_path):
        super().__init__(google_client, supportsTeamDrives=True)
        self.folder_id = folder_id
        self.tmp_path = tmp_path
        self.last_run_file = self.get_last_run_drive(folder_id, filename)
        if self.last_run_file != None:
            self.last_run = self.last_run_file["fileName"].split(
                '-')[1].split('.')[0]
            if not os.path.exists(self.tmp_path):
                os.mkdir(self.tmp_path)
            self.prev_file_path = self.download_file(
                self.last_run_file["fileId"], download_path=self.tmp_path)
        else:
            self.last_run = None
            logger.info("last_run is not found")
            print("Previous file is not found.")

    def get_last_run_file(self):
        return self.last_run_file

    def get_prev_file_path(self):
        return self.prev_file_path

    def get_last_run_drive(self, folder_id, file_name, extension=".xlsx"):
        output_file_list = self.list_files(parent_id=folder_id)
        max_date = 0
        out_file_id = None
        for drive_file in output_file_list:
            if (file_name in drive_file.name) & (drive_file.name[-len(extension):] == extension):
                if "-" in drive_file.name:
                    date = int(drive_file.name.split("-")[1].split(extension)[0])
                else:
                    date = int(drive_file.name.split(file_name)[1].split(extension)[0])
                if max_date <= date:
                    out_file_id = drive_file.fileId
                    out_file_name = drive_file.name
                    max_date = date
        if out_file_id is not None:
            out_file_details = {
                "fileId": out_file_id,
                "fileName": out_file_name,
                "maxDate": max_date
            }
            logger.info(
                f"Previous Comparison Result is {out_file_name} - {out_file_id}")
            return out_file_details
        else:
            return None

    def upload_report(self, output_file_path):
        """
        Upload specified outputfile in temp folder.
        """
        found_file = self.check_file_exists(
            os.path.basename(output_file_path), self.folder_id)
        logger.info(
            f"File name: {os.path.basename(output_file_path)}. Found the following files with the same name: {found_file}")

        if found_file is not None:
            file_id = found_file.fileId
            res = self.update_file(output_file_path, file_id)
            logger.info(f"Updated {output_file_path}")
        else:
            res = self.upload_file(
                output_file_path, self.folder_id, convert=False)
            logger.info(f"Uploaded {output_file_path}")
        return res

    def check_file_exists(self, filename, folder_id):
        folder_content = self.list_files(parent_id=folder_id)
        for content in folder_content:
            if content.name == filename:
                return content

        return None

    def clean_temp_folder(self):
        for file_name in os.listdir(self.tmp_path):
            os.remove(self.tmp_path + file_name)
