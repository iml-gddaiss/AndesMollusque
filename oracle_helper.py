import os
import oracledb
import pyodbc

from dotenv import load_dotenv

load_dotenv()


class OracleHelper:
    def __init__(self, access_file=None):
        if access_file:
            self.con = pyodbc.connect(
                f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_file};"
            )
            print("Successfully connected to Microsoft Access file")

        else:
            host = os.getenv("ORACLE_HOST", "default host")
            port = int(os.getenv("ORACLE_PORT", 123))
            sid = os.getenv("ORACLE_SID", "default sid")
            dsn = oracledb.makedsn(host, port, sid=sid)

            self.con = oracledb.connect(
                user=os.getenv("ORACLE_USER", "default user"),
                password=os.getenv("ORACLE_PASSWORD", "default password"),
                dsn=dsn,
            )
            print("Successfully connected to Oracle Database")

        self.cur = self.con.cursor()

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

        query = f"SELECT {pkey_col} FROM {table} WHERE {col}='{val}'"
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
        pkey_col: str = "columnofprimarykey",
        col: str = "columnname",
        val="entryvalue",
    ) -> bool:
        """
        Validate that a value exists (and there is only one) in the Oracle DB

        :param table: The name of the Oracle table, defaults to "tablename"
        :type table: str, optional
        :param pkey_col: The column name that holds the key, defaults to "columnofprimarykey"
        :type pkey_col: str, optional
        :param col: The column that holds the value to match, defaults to "columnname"
        :type col: str, optional
        :param val: The value to match, defaults to "entryvalue"
        :type val: str, optional
        :return: True if the value exists in the Oracle DB, False otherwise
        :rtype: bool
        """
        try:
            self.get_ref_key(table, pkey_col, col, val)
        except ValueError:
            return False
        else:
            return True


if __name__ == "__main__":
    key_col = "COD_SECTEUR_RELEVE"
    t_name = "SECTEUR_RELEVE_MOLL"
    val_col = "DESC_SECTEUR_RELEVE_F"
    val = "Îles de la Madeleine"

    odb = OracleHelper(access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb")
    res = odb.get_ref_key(t_name, key_col, val_col, val)
    print(res)
