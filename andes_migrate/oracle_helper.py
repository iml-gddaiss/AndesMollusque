import os
import pyodbc
import logging
import oracledb

from oracledb.exceptions import DatabaseError

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
            try:
                self.cur.execute(query)
            except DatabaseError as exc:
                self.logger.error("Error to executing query: %s", query)
                raise exc

            else:
                return self.cur.fetchall()
            
    def _cod_esp_gen_2_aphia_id(self, code_esp:int) -> int:
        """ convert code espece general to aphia id
        returns mapped aphia

        :param code_esp: code espece general
        :type code_esp: int
        :return: aphia _id
        :rtype: int

        """
        norme_name_str = "AphiaId"
        query = (
            "SELECT ESPECE_NORME.COD_ESPECE "
            "FROM ESPECE_NORME "
            "LEFT JOIN NORME "
            "ON ESPECE_NORME.COD_NORME=NORME.COD_NORME "
            f"WHERE NORME.NOM_NORME='{norme_name_str}' "
            f"AND ESPECE_NORME.COD_ESP_GEN={code_esp}"
        )

        result = self.execute_query(query)
        if not len(result) == 1:
            raise ValueError("Expected only one result.")
        else:
            return int(result[0][0])

    def _aphia_id_2_cod_esp_gen(self, aphia_id:int) -> int:
        """ convert code aphia id to espece general

        :param code_esp: aphia id
        :type code_esp: int
        :return: code espece general
        :rtype: int

        """
        norme_name_str = "AphiaId"
        query = (
            "SELECT ESPECE_NORME.COD_ESP_GEN "
            "FROM ESPECE_NORME "
            "LEFT JOIN NORME "
            "ON ESPECE_NORME.COD_NORME=NORME.COD_NORME "
            f"WHERE NORME.NOM_NORME='{norme_name_str}' "
            f"AND ESPECE_NORME.COD_ESPECE={aphia_id}"
        )

        result = self.execute_query(query)
        if not len(result) == 1:
            raise ValueError("Expected only one result.")
        else:
            return int(result[0][0])


    def _strap_2_cod_esp_gen(self, strap_code:int) -> int:
        """ convert STRAP code to code espece general

        :param code_esp: strap code
        :type code_esp: int
        :return: code espece general
        :rtype: int

        """
        norme_name_str = "STRAP_IML"
        query = (
            "SELECT ESPECE_NORME.COD_ESP_GEN "
            "FROM ESPECE_NORME "
            "LEFT JOIN NORME "
            "ON ESPECE_NORME.COD_NORME=NORME.COD_NORME "
            f"WHERE NORME.NOM_NORME='{norme_name_str}' "
            f"AND ESPECE_NORME.COD_ESPECE={strap_code}"
        )

        result = self.execute_query(query)
        if not len(result) == 1:
            raise ValueError("Expected only one result.")
        else:
            return int(result[0][0])


    @staticmethod
    def convert_nm_2_km(val: float) -> float:
        """convert nautical miles to kilometers

        :param val: value (in nautical miles) to convert
        :type val: float
        :return: converted value (in kilometers)
        :rtype: float
        """
        return val * 1.852
    
    @staticmethod
    def convert_knots_to_kph(val: float) -> float:
        """convert knots to kilometers per hour

        :param val: value (in knots) to convert
        :type val: float
        :return: converted value (in km/h)
        :rtype: float
        """
        return OracleHelper.convert_nm_2_km(val)


    @staticmethod
    def _to_oracle_coord(coord: float | None) -> float | None:
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

    @staticmethod
    def _from_oracle_coord( coord: float | None) -> float | None:
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
