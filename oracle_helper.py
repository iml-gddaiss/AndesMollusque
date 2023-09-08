import os
import pyodbc
import logging
import oracledb

from db_helper import DBHelper
from dotenv import load_dotenv

load_dotenv()


class OracleHelper(DBHelper):

    def __init__(self, access_file=None):
        super().__init__()

        self.logger = logging.getLogger(__name__)
        # datime format on MS access DB
        self.datetime_strfmt='%Y-%m-%d %H:%M:%S'
        self.ms_access:bool
        
        if access_file:
            self.ms_access = True
            self.con = pyodbc.connect(
                f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_file};"
            )
            print("Successfully connected to Microsoft Access file")

        else:
            os.environ.get('PYO_SAMPLES_ORACLE_CLIENT_PATH')

            self.ms_access = False
            # IMLP uses WE8MSWIN1252
            self.db_charset = 'Windows-1252'

            host = os.getenv("ORACLE_HOST", "default host")
            port = int(os.getenv("ORACLE_PORT", 123))
            sid = os.getenv("ORACLE_SID", "default sid")
            dsn = oracledb.makedsn(host, port, sid=sid)
            oracledb.init_oracle_client(lib_dir=r"C:\Oracle\12.2.0_Instant_x64")
            self.con = oracledb.connect(
                user=os.getenv("ORACLE_USER", "default user"),
                password=os.getenv("ORACLE_PASSWORD", "default password"),
                dsn=dsn,

            )
            print("Successfully connected to Oracle Database")
        self.cur=self.con.cursor()


    def execute_query(self, query:str):
        if self.ms_access:
            res = self.cur.execute(query)
            return res.fetchall()
        else:
            self.cur.execute(query)
            return self.cur.fetchall() 
        


if __name__ == "__main__":
    key_col = "COD_SECTEUR_RELEVE"
    t_name = "SECTEUR_RELEVE_MOLL"
    val_col = "DESC_SECTEUR_RELEVE_F"
    val = "Îles de la Madeleine"

    # odb = OracleHelper(access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb")
    odb = OracleHelper()
    res = odb.get_ref_key(t_name, key_col, val_col, val)
    print(res)
