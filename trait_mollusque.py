import logging
import sqlite3
import datetime
from unidecode import unidecode

from project_mollusque import ProjetMollusque
from peche_sentinelle import TablePecheSentinelle
from andes_helper import AndesHelper
from decorators import log_results, validate_string, validate_int

logging.basicConfig(level=logging.INFO)


class TraitMollusque(TablePecheSentinelle):
    def __init__(self, andes_db: AndesHelper, proj: ProjetMollusque):
        super().__init__()
        self.andes_db = andes_db
        self.proj: ProjetMollusque = proj
        self.proj_pk = proj.pk
        self.data = {}

        self.list_set_pk = []
        self.set_pk_idx: int | None = None
        self.set_pk: int | None = None

        self._init_set_list()
        # this may have to be modified to include milisecs
        self.andes_datetime_format = "%Y-%m-%d %H:%M:%S"

    def _init_set_list(self):
        """init a list of sets (just pKeys) from Andes"""
        query = f"SELECT shared_models_set.id \
                FROM shared_models_set \
                WHERE shared_models_set.cruise_id={self.proj_pk} \
                ORDER BY shared_models_set.id ASC;"

        result = self.andes_db.execute_query(query)
        self._assert_not_empty(result)

        self.list_set_pk:list = result
        self.set_pk_idx = 0
        self.set_pk = self.list_set_pk[self.set_pk_idx][0]

    def get_cod_source_info(self) -> int:
        """
        COD_SOURCE_INFO INTEGER
        """
        return self.proj.get_cod_source_info()

    def get_no_releve(self) -> int:
        """
        NO_RELEVE INTEGER
        """
        return self.proj.get_no_releve()

    def get_code_nbpc(self) -> str:
        """
        COD_NBPC VARCHAR(6)
        """
        return self.proj.get_cod_nbpc()

    @log_results
    def get_ident_no_trait(self) -> int:
        """
        IDENT_NO_TRAIT INTEGER INT
        """
        if self.set_pk:
            return self.set_pk
        else:
            raise ValueError

    @validate_int()
    @log_results
    def get_cod_zone_gest_moll(self) -> int:
        """
        Identification de la zone de gestion de la pêche aux mollusques tel que défini dans la table ZONE_GEST_MOLL
        COD_ZONE_GEST_MOLL INTEGER(32)
        """
        zone = self.proj.zone

        key = self.reference_data.get_ref_key(
            table="ZONE_GEST_MOLL",
            pkey_col="COD_ZONE_GEST_MOLL",
            col="ZONE_GEST_MOLL",
            val=zone,
        )
        return key

    @validate_int()
    @log_results
    def get_cod_secteur_releve(self) -> int:
        """
        COD_SECTEUR_RELEVE INTEGER

        Identification de la zone géographique de déroulement du relevé tel que défini dans la table SECTEUR_RELEVE_MOLL

        CONTRAINTE

        La valeur du champ shared_models_cruise.area_of_operation (FR: Région échantillonée)
        doit absolument correspondres avec la description présente dans la table SECTEUR_RELEVE_MOLL:

        1 -> Côte-Nord
        4 -> Îles de la Madeleine
        """

        query = f"SELECT shared_models_cruise.area_of_operation \
                FROM shared_models_cruise \
                WHERE shared_models_cruise.id={self.proj.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        secteur:str = result[0][0]

        # first char is stripped of accents in uppercase
        secteur = unidecode(secteur[0].upper())

        key = self.reference_data.get_ref_key(
            table="SECTEUR_RELEVE_MOLL",
            pkey_col="COD_SECTEUR_RELEVE",
            col="SECTEUR_RELEVE",
            val=secteur,
        )
        return key

    @validate_int()
    @log_results
    def get_no_station(self) -> int:
        """
        NO_STATION
        Numéro de la station en fonction du protocole d'échantillonnage

        ANDES: shared_models_newstation.name
        """
        query = f"SELECT shared_models_newstation.name \
                FROM shared_models_set \
                LEFT JOIN shared_models_newstation \
                    ON shared_models_set.new_station_id = shared_models_newstation.id \
                WHERE shared_models_set.id={self.set_pk};"

        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        # extract all non-numerical chacters
        to_return = ''.join(c for c in to_return if c.isnumeric())
        return to_return


if __name__ == "__main__":
    # andes_db = AndesHelper("db.sqlite3")
    andes_db = AndesHelper()

    proj = ProjetMollusque(andes_db)
    proj.init_mission_pk("IML-2023-011")
    proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")

    trait = TraitMollusque(andes_db, proj)

    trait.get_cod_source_info()
    trait.get_no_releve()
    trait.get_code_nbpc()
    trait.get_ident_no_trait()
    trait.get_cod_zone_gest_moll()
    trait.get_cod_secteur_releve()
    trait.get_no_station()

    # trait.validate()

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
