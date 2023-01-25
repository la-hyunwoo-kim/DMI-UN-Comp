import pyodbc
import requests
import pandas as pd
import json

import logging
logger = logging.getLogger(__name__)


class SQLModule():
    """
    Wraps around the connection details to the database. Instation requres user, password, server, and db details.
    """
    conn = None
    cursor = None

    def __init__(self, config):
        self.config = config
        self.user = config["user"]
        self.password = config["password"]
        self.server = config["server"]
        self.db1 = config["db1"]


    def connect(self):
        """
        Connects to the database using the specified user, password, server, and db details specified during creation.
        """
        self.conn = pyodbc.connect(
            driver='{SQL Server}', host=self.server, database=self.db1, user=self.user, password=self.password)
        self.cursor = self.conn.cursor()


    def execute_sql(self, sql_path, fillna=False):
        """
        Executes the sql_statement and returns the result as a pandas DataFrame.
        """
        fd = open(sql_path, 'r')
        sql_statement = fd.read()
        fd.close()
        logger.info("SQL Statement length: " + str(len(sql_statement)))
        self.cursor.execute(sql_statement)
        rows = self.cursor.fetchall()
        res = []
        cols = []
        cols.append(self.cursor.description)
        col_names = [col_dets[0] for col_dets in list(cols[0])]
        for row in rows:
            res.append(list(row))
        logger.info(f"returned data length: {len(res)}")
        if fillna:
            return pd.DataFrame(res, columns=col_names, dtype='object').fillna("")
        else:
            return pd.DataFrame(res, columns=col_names, dtype='object')
