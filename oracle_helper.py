import os
import oracledb
import pyodbc

from dotenv import load_dotenv

load_dotenv()

class OracleHelper:

    def __init__(self, access_file=None):
        if access_file:
            self.con = pyodbc.connect(f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_file};")
            print("Successfully connected to Microsoft Access file")

        else:
            host = os.getenv('ORACLE_HOST')
            port = os.getenv('ORACLE_PORT')
            sid = os.getenv('ORACLE_SID')
            dsn = oracledb.makedsn(host, port, sid=sid)

            self.con = oracledb.connect(
                user=os.getenv('ORACLE_USER'),
                password=os.getenv('ORACLE_PASSWORD'),
                dsn=dsn
            )
            print("Successfully connected to Oracle Database")

        self.cur = self.con.cursor()

    def get_ref_key(self, table, key_col, val_col, val):
        '''
        get a key from a reference table
        '''
        query = f"SELECT {key_col} FROM {table} WHERE {val_col}='{val}'"
        self.cur.execute(query)
        res = self.cur.fetchall()
        if len(res)==1:
            return res[0][0]
        else:
            print("returned more than one match")
            raise ValueError


if __name__ == "__main__":

    key_col = 'COD_SECTEUR_RELEVE'
    t_name = 'SECTEUR_RELEVE_MOLL'
    val_col = 'DESC_SECTEUR_RELEVE_F'
    val ='Îles de la Madeleine'

    odb = OracleHelper(access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb")
    res = odb.get_ref_key(t_name, key_col, val_col, val)
    print(res)