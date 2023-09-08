import logging
import sqlite3
import datetime

from project_mollusque import ProjetMollusque
from peche_sentinelle import TablePecheSentinelle
from decorators import log_results, validate_string, validate_int

logging.basicConfig(level=logging.INFO)


class TraitMollusque(TablePecheSentinelle):
    def __init__(self, con, proj: ProjetMollusque):
        super().__init__()
        self.proj: ProjetMollusque = proj
        self.con = con
        self.cur = self.con.cursor()
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
        res = self.cur.execute(query)

        result = res.fetchall()
        self._assert_not_empty(result)

        self.list_set_pk = result
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
        doit absolument correspondres avec la descritption présente dans la table SECTEUR_RELEVE_MOLL:

        1 -> Côte-Nord
        4 -> Îles de la Madeleine
        """

        query = f"SELECT shared_models_cruise.area_of_operation FROM shared_models_cruise where shared_models_cruise.id={self.proj.pk};"
        res = self.cur.execute(query)
        result = res.fetchall()
        self._assert_one(result)
        secteur = result[0][0]

        key = self.reference_data.get_ref_key(
            table="SECTEUR_RELEVE_MOLL",
            pkey_col="COD_SECTEUR_RELEVE",
            col="DESC_SECTEUR_RELEVE_F",
            val=secteur,
        )
        return key


if __name__ == "__main__":
    con = sqlite3.connect("db.sqlite3")

    proj = ProjetMollusque(con)
    proj.init_mission_pk("IML-2023-011")
    proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")

    trait = TraitMollusque(con, proj)

    trait.get_cod_source_info()
    trait.get_no_releve()
    trait.get_code_nbpc()
    trait.get_ident_no_trait()
    trait.get_cod_zone_gest_moll()
    trait.get_cod_secteur_releve()

    # trait.validate()

    #
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
