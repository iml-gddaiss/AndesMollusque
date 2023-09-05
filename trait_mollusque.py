import logging
import sqlite3
import datetime
from typing import Any

from project_mollusque import ProjetMollusque
from peche_sentinelle import TablePecheSentinelle
from decorators import log_results, validate_string

logging.basicConfig(level=logging.INFO)


class TraitMollusque(TablePecheSentinelle):

    def __init__(self, con, proj:ProjetMollusque):
        super().__init__()
        self.proj:ProjetMollusque = proj
        self.con = con
        self.cur = self.con.cursor()
        self.pk = None
        self.espece = None
        self.data = {}

        # this may have to be modified to include milisecs
        self.andes_datetime_format = "%Y-%m-%d %H:%M:%S"

    def get_cod_source_info(self) ->int:
        """
        COD_SOURCE_INFO INT
        """
        return self.proj.get_cod_source_info()

    def get_no_releve(self) ->int:
        """
        NO_RELEVE
        """
        return self.proj.get_no_releve()

    def get_code_nbpc(self)->str:
        """
         COD_NBPC
        """
        return self.proj.get_cod_nbpc()

    def get_ident_no_trait(self) ->int:
        """
        IDENT_NO_TRAIT
        """
        query = f"SELECT shared_models_cruise.notes \
            FROM shared_models_cruise \
            WHERE shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)

        result = res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]

        to_return = int(to_return)
        return to_return


if __name__ == "__main__":
    con = sqlite3.connect("db.sqlite3")

    proj = ProjetMollusque(con)
    proj.init_mission_pk("IML-2023-011")
    proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")

    trait = TraitMollusque(con, proj)

    trait.get_cod_source_info()
    trait.get_no_releve()
    trait.get_code_nbpc()
    trait.get_

    # trait.validate()

    # COD_ZONE_GEST_MOLL
    # COD_SECTEUR_RELEVE
    # COD_STRATE
    # NO_STATION
    # COD_TYP_TRAIT
    # COD_RESULT_OPER
    # DATE_DEB_TRAIT
    # DATE_FIN_TRAIT
    # HRE_DEB_TRAIT
    # HRE_FIN_TRAIT
    # COD_TYP_HEURE
    # COD_FUSEAU_HORAIRE
    # COD_METHOD_POS
    # LAT_DEB_TRAIT
    # LAT_FIN_TRAIT
    # LONG_DEB_TRAIT
    # LONG_FIN_TRAIT
    # LATLONG_P
    # DISTANCE_POS
    # DISTANCE_POS_P
    # VIT_TOUAGE
    # VIT_TOUAGE_P
    # DUREE_TRAIT
    # DUREE_TRAIT_P
    # TEMP_FOND
    # TEMP_FOND_P
    # PROF_DEB
    # PROF_DEB_P
    # PROF_FIN
    # PROF_FIN_P
    # REM_TRAIT_MOLL
    # NO_CHARGEMENT
