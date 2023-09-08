import os
import logging
import sqlite3
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


class AndesHelper:

    def __init__(self, sqlite_file=None):
        self.logger = logging.getLogger(__name__)
        # datime format on MS access DB
        self.datetime_strfmt='%Y-%m-%d %H:%M:%S'


        if sqlite_file:
            self.con = sqlite3.connect(sqlite_file)

            print("Successfully connected to SQlite file")

        else:
            self.con = mysql.connector.connect(host=os.getenv('ANDES_HOST', 'la-tele-du-samedi.ent.dfo-mpo.ca'),
                                               port=int(os.getenv('ANDES_PORT', 4321)),
                                               database=os.getenv('ANDES_DB_NAME', 'BD de BikiniBottom'),
                                               user=os.getenv('ANDES_DB_USERNAME','Bob Eponge'),
                                               password=os.getenv('ANDES_DB_USERPASS', 'SQUIDWARD'))
            print("Successfully connected to MySQL Database")

        self.cur = self.con.cursor()

    def _format_sql_string(self, input:str)->str:
        """ use two single-quotes to properly generate the SQL statement


        :param input: input string to fix
        :type input: str
        :return: inptstirng with doubled single quotes
        :rtype: str
        """


        return input.replace("'","''")

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
        print(query)

        self.cur.execute(query)
        res = self.cur.fetchall()
        if len(res) == 1:
            return res[0][0]
        else:
            print("returned more than one match")
            raise ValueError

    def validate_exists(
        self,
        table: str = "tablename",
        col: str = "columnname",
        val:int|str="entryvalue",
    ) -> bool:
        """
        Validate that a value exists (and there is only one) in the Oracle DB

        :param pkey_col: The column name that holds the key, defaults to "columnofprimarykey"
        :type pkey_col: str, optional
        :param col: The column that holds the value to match, defaults to "columnname"
        :type col: str, optional
        :param val: The value to match, defaults to "entryvalue"
        :type val: str, optional
        :return: True if the value exists in the Oracle DB, False otherwise
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
        self.cur.execute(query)
        res = self.cur.fetchall()

        if len(res) == 1:
            return True
        else:
            self.logger.error("Expected to find a %s=%s but is not present in table: %s", col, val, table)

            return False

if __name__ == "__main__":
    andes_db = AndesHelper()
    