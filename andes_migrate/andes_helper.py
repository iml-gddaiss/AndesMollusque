import os
import logging
import sqlite3
import mysql.connector

from mysql.connector.errors import ProgrammingError
from dotenv import load_dotenv

load_dotenv()


class AndesHelper:

    def __init__(self, sqlite_file=None):
        self.logger = logging.getLogger(__name__)
        # datime format on MS access DB
        self.datetime_strfmt='%Y-%m-%d %H:%M:%S'
        self.sqlite:bool

        if sqlite_file:
            self.sqlite = True
            self.con = sqlite3.connect(sqlite_file)

            print("Successfully connected to SQlite file")

        else:
            self.sqlite = False
            self.con = mysql.connector.connect(host=os.getenv('ANDES_HOST', 'la-tele-du-samedi.ent.dfo-mpo.ca'),
                                               port=int(os.getenv('ANDES_PORT', 4321)),
                                               database=os.getenv('ANDES_DB_NAME', 'BD de BikiniBottom'),
                                               user=os.getenv('ANDES_DB_USERNAME','Bob Eponge'),
                                               password=os.getenv('ANDES_DB_USERPASS', 'SQUIDWARD'))
            print("Successfully connected to MySQL Database")

        self.cur = self.con.cursor()

    def execute_query(self, query:str):
        
        if self.sqlite:
            res = self.cur.execute(query)
            return res.fetchall()
        else:
            try:
                self.cur.execute(query)
            except ProgrammingError as exc:
                self.logger.error("Error to executing query: %s", query)
                raise exc

            else:
                return self.cur.fetchall()
             


if __name__ == "__main__":
    andes_db = AndesHelper()
    