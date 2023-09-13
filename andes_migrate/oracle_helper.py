import os
import pyodbc
import logging
import oracledb

from dotenv import load_dotenv

from andes_migrate.db_helper import DBHelper

load_dotenv()


class OracleHelper(DBHelper):
    def __init__(self, access_file=None):
        super().__init__()

        self.logger = logging.getLogger(__name__)
        # datime format on MS access DB
        self.datetime_strfmt = "%Y-%m-%d %H:%M:%S"
        self.ms_access: bool

        if access_file:
            self.ms_access = True
            self.con = pyodbc.connect(
                f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_file};"
            )
            print("Successfully connected to Microsoft Access file")

        else:
            os.environ.get("PYO_SAMPLES_ORACLE_CLIENT_PATH")

            self.ms_access = False
            # IMLP uses WE8MSWIN1252
            self.db_charset = "Windows-1252"

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
            print("Successfully connected to the Oracle Database")
        self.cur = self.con.cursor()

    def execute_query(self, query: str):
        if self.ms_access:
            res = self.cur.execute(query)
            return res.fetchall()
        else:
            self.cur.execute(query)
            return self.cur.fetchall()

    def _to_oracle_coord(self, coord: float | None) -> float | None:
        """convert to the unique coordinate encoding scheme

        The input coord (in degrees decimal) is encoded and returned as

        {whole_degrees}{whole_minutes}.{decimal_minutes}

        For example, the latitude of 47.155927
        is decomposed into:
        whole_degrees = 47
        whole_minutes = 9
        decimal_minues = 35562

        and yields:
        4709.35562

        """
        if coord:
            degrees = int(coord)
            min_dec = (coord - degrees) * 60
            to_return = degrees * 100 + min_dec
            return to_return
        else:
            return None

    def _from_oracle_coord(self, coord: float | None) -> float | None:
        """convert from the unique coordinate encoding scheme

        The input coord in the oracle encoding of
        {whole_degrees}{whole_minutes}.{decimal_minutes}
        is returnd as (standard) degree decimal value

        For example, the value 4709.35562

        is decomposed into:
        whole_degrees = 47
        whole_minutes = 9
        decimal_minues = 35562

        and returns a latitude of 47.155927

        """
        if coord:
            degrees = int(coord / 100)
            min_dec = (coord - degrees * 100) / 60
            to_return = degrees + min_dec
            return to_return
        else:
            return None


if __name__ == "__main__":
    key_col = "COD_SECTEUR_RELEVE"
    t_name = "SECTEUR_RELEVE_MOLL"
    val_col = "DESC_SECTEUR_RELEVE_F"
    val = "Îles de la Madeleine"

    # odb = OracleHelper(access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb")
    odb = OracleHelper()
    res = odb.get_ref_key(t_name, key_col, val_col, val)
    print(res)
