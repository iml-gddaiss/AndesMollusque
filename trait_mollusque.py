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
        """ init a list of sets (just pKeys) from Andes"""

        query = f"SELECT shared_models_set.id \
                FROM shared_models_set \
                WHERE shared_models_set.cruise_id={self.proj_pk} \
                ORDER BY shared_models_set.id ASC;"

        result = self.andes_db.execute_query(query)
        self._assert_not_empty(result)

        self.list_set_pk:list = result
        self.set_pk_idx = 0
        self.set_pk = self.list_set_pk[self.set_pk_idx][0]

    def _get_current_set_pk(self) -> int:
        """
        Return the Andes primary key of the current set 
        """
        if self.set_pk_idx is not None and self.list_set_pk:
            return self.list_set_pk[self.set_pk_idx][0]
        else:
            raise ValueError

    def _increment_current_set(self):
        """ focus on next set
        """
        if self.set_pk_idx and self.list_set_pk:
            if self.set_pk_idx <len(self.list_set_pk)-1 :
                self.set_pk_idx += 1
            else:
                raise StopIteration

    @validate_int()
    def get_cod_source_info(self) -> int:
        """ COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait du projet.
        """

        return self.proj.get_cod_source_info()

    @validate_int()
    def get_no_releve(self) -> int:
        """ NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait du projet.
        """
        return self.proj.get_no_releve()

    def get_code_nbpc(self) -> str:
        """  COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait du projet.
        """
        return self.proj.get_cod_nbpc()

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """ IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Andes
        -----
        shared_models.set.set_number
        """
        set_pk = self._get_current_set_pk()
        query = f"SELECT shared_models_set.set_number \
                FROM shared_models_set \
                WHERE shared_models_set.id={set_pk};"

        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @validate_int(not_null=False)
    @log_results
    def get_cod_zone_gest_moll(self) -> int| None:
        """ COD_ZONE_GEST_MOLL  COD_TYP_TRAIT INTEGER / NUMBER(5,0)
        Identification de la zone de gestion de la pêche aux mollusques tel que défini dans la table ZONE_GEST_MOLL
        
        Pas présente dans Andes, doit être initialisé via le parametre `zone` du projet.
        """
        zone = self.proj.zone

        key = self.reference_data.get_ref_key(
            table="ZONE_GEST_MOLL",
            pkey_col="COD_ZONE_GEST_MOLL",
            col="ZONE_GEST_MOLL",
            val=zone,
        )
        return key

    @validate_int(not_null=False)
    @log_results
    def get_cod_secteur_releve(self) -> int| None:
        """ COD_SECTEUR_RELEVE INTEGER/NUMBER(5,0) 
        Identification de la zone géographique de déroulement du relevé tel que défini dans la table SECTEUR_RELEVE_MOLL

        CONTRAINTE

        La premiere lettre du champ shared_models_cruise.area_of_operation (FR: Région échantillonée)
        doit absolument correspondres avec la valeur SECTEUR_RELEVE de la table SECTEUR_RELEVE_MOLL:

        1 -> C (Côte-Nord)
        4 -> I (Îles de la Madeleine)

        Bien q'il suffit de mettre seulement la premiere lettre, il est conseiller de mettre
        le nom en entier pour la lisibilité.
    
        """

        query = f"SELECT shared_models_cruise.area_of_operation \
                FROM shared_models_cruise \
                WHERE shared_models_cruise.id={self.proj.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        secteur:str = result[0][0]

        # first char stripped of accents and cast to uppercase
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
        """ NO_STATION  INTEGER/NUMBER(5,0)
        Numéro de la station en fonction du protocole d'échantillonnage
        
        ANDES: shared_models_newstation.name
        The station name is stripped of non-numerical characters
        to generate no_station(i.e., NR524 -> 524)
        """
        query = f"SELECT shared_models_newstation.name \
                FROM shared_models_set \
                LEFT JOIN shared_models_newstation \
                    ON shared_models_set.new_station_id = shared_models_newstation.id \
                WHERE shared_models_set.id={self._get_current_set_pk()};"

        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        # extract all non-numerical chacters
        to_return = ''.join(c for c in to_return if c.isnumeric())
        return to_return
    
    @validate_int()
    @log_results
    def get_cod_type_trait(self) -> int:
        """  COD_TYP_TRAIT INTEGER / NUMBER(5,0)
        Identification du type de trait tel que décrit dans la table TYPE_TRAIT

        Andes
        -----
        shared_models.stratificationtype.code
        via la mission 
        shared_models.cruise.stratification_type_id

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        """
        # this query does not work because of a mistmatch between reference tables
        # query = f"SELECT shared_models_stratificationtype.code \
        #         FROM shared_models_cruise \
        #         LEFT JOIN shared_models_stratificationtype \
        #             ON shared_models_cruise.stratification_type_id = shared_models_stratificationtype.id \
        #         WHERE shared_models_cruise.id={self.proj.pk};"

        # manual hack
        # we take the french description and match with Oracle\
        # the match isn't even verbatim, we we need a manual map
        andes_2_oracle_map = {
            'Échantillonnage aléatoire': 'Aléatoire simple',
            'Station fixe': 'Station fixe'
        }

        query = f"SELECT shared_models_stratificationtype.description_fra \
                FROM shared_models_cruise \
                LEFT JOIN shared_models_stratificationtype \
                    ON shared_models_cruise.stratification_type_id = shared_models_stratificationtype.id \
                WHERE shared_models_cruise.id={self.proj.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        desc = result[0][0]

        key = self.reference_data.get_ref_key(
            table="TYPE_TRAIT",
            pkey_col="COD_TYP_TRAIT",
            col="DESC_TYP_TRAIT_F",
            val=andes_2_oracle_map[desc],
        )
        return key

    @validate_int()
    @log_results
    def get_cod_result_oper(self) -> int:
        """  COD_RESULT_OPER INTEGER / NUMBER(5,0)
        Résultat de l'activité de pêche tel que défini dans la table COD_RESULT_OPER

        Andes
        -----
        shared_models_setresult.code

        The Andes codes seem to match the first six from the Oracle database,
        except for hte mistmatch between code 5 and 6.
        see issue https://github.com/dfo-gulf-science/andes/issues/1237
        
        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        """
        query = f"SELECT shared_models_setresult.code \
                FROM shared_models_set \
                LEFT JOIN shared_models_setresult \
                    ON shared_models_set.set_result_id = shared_models_setresult.id \
                WHERE shared_models_set.id={self._get_current_set_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]

        # this ia a weird one...
        # see issue #1237
        andes_2_oracle_map = {
            '1': 1,
            '2': 2,
            '3': 3,
            '4': 4,
            '5': 6,
            '6': 5,
        }
        to_return = andes_2_oracle_map[str(to_return)]
        return(to_return)





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
    trait.get_cod_type_trait()
    trait.get_cod_result_oper()
    
    # trait.validate()

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
