import os
import pyodbc
import oracledb

import logging

class DBHelper:

    def __init__(self, file=None):

        self.datetime_strfmt='%Y-%m-%d %H:%M:%S'
        # children classes need to init these
        self.con = None
        self.cur = None
        self.db_charset:str
        self.logger:logging.Logger


    def _format_sql_string(self, input:str)->str:
        """ use two single-quotes to properly generate the SQL statement

        :param input: input string to fix
        :type input: str
        :return: inptstirng with doubled single quotes
        :rtype: str
        """
        return input.replace("'","''")
    
    def execute_query(self, query:str):
        # child class must override
        raise NotImplemented

    def get_ref_key(
        self,
        table: str = "tablename",
        pkey_col: str = "columnofprimarykey",
        col: str = "columnname",
        val="entryvalue",
    ):
        """gets the Oracle reference key corresponding to a value

        :param table: The name of the Oracle table, defaults to "tablename"
        :type table: str, optional
        :param pkey_col: The column name that holds the key, defaults to "columnofprimarykey"
        :type pkey_col: str, optional
        :param col: The column that holds the value to match, defaults to "columnname"
        :type col: str, optional
        :param val: The value to match, defaults to "entryvalue"
        :type val: str, optional
        :return: The value found in the pkey column for the entry with the value
        :rtype: _type_
        """
        # sanitize string (double escape single quotes)
        val = self._format_sql_string(val)
        query = f"SELECT {pkey_col} FROM {table} WHERE {col}='{val}'"

        res = self.execute_query(query)

        if len(res) == 1:
            return res[0][0]
        elif len(res) == 0:
            self.logger.error("No match found for query: %s",query)
            raise ValueError
        else:
            self.logger.error("More than one match found for query: %s",query)
            raise ValueError

    def validate_exists(
        self,
        table: str = "tablename",
        col: str = "columnname",
        val:int|str="entryvalue",
    ) -> bool:
        """
        Validate that a value exists (and there is only one) in the DB

        :param pkey_col: The column name that holds the key, defaults to "columnofprimarykey"
        :type pkey_col: str, optional
        :param col: The column that holds the value to match, defaults to "columnname"
        :type col: str, optional
        :param val: The value to match, defaults to "entryvalue"
        :type val: str, optional
        :return: True if the value exists in the DB, False otherwise
        :rtype: bool
        """
        if isinstance(val, str):
            # sanitize string (double escape single quotes)
            val = self._format_sql_string(val)
            query = f"SELECT {col} FROM {table} WHERE {col}='{val}'"
        elif isinstance(val, int):
            query = f"SELECT {col} FROM {table} WHERE {col}={val}"
        else:
            print("type error yo?")
            raise TypeError
        res = self.execute_query(query)

        if len(res) == 1:
            return True
        else:
            self.logger.error("Looking for %s=%s but is not present in table: %s", col, val, table)
            return False

