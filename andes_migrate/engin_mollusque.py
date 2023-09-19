import logging
import datetime
from unidecode import unidecode

from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.table_peche_sentinelle import TablePecheSentinelle
from andes_migrate.andes_helper import AndesHelper
from andes_migrate.decorators import (
    deprecate,
    log_results,
    validate_string,
    validate_int,
)

logging.basicConfig(level=logging.INFO)


class EnginMollusque(TablePecheSentinelle):
    def __init__(self, trait: TraitMollusque):
        super().__init__()
        self.trait: TraitMollusque = trait

        self.andes_db = trait.andes_db
        self.data = {}

    def populate_data(self):
        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["COD_ENG_GEN"] = self.get_cod_eng_gen()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["NO_ENGIN"] = self.get_no_engin()
        self.data["CODE_NBPC"] = self.get_code_nbpc()
        self.data["COD_TYP_PANIER"] = self.get_cod_type_panier()
        # self.data["NO_CHARGEMENT"] = self.get_()
        # self.data["LONG_FUNE"] = self.get_()
        # self.data["LONG_FUNE_P"] = self.get_()
        # self.data["NB_PANIER"] = self.get_()
        # self.data["REMPLISSAGE"] = self.get_()
        # self.data["REMPLISSAGE_P"] = self.get_()
        # self.data["REM_ENGIN_MOLL"] = self.get_()

    @validate_int()
    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait du trait
        """

        return self.trait.get_cod_source_info()

    @validate_int()
    @log_results
    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Andes: shared_models_set.gear_type_id-> shared_models.geartype.code
        """
        set_pk = self.trait._get_current_set_pk()

        query = f"SELECT shared_models_geartype.code \
                FROM shared_models_set \
                LEFT JOIN shared_models_geartype \
                ON shared_models_set.gear_type_id = shared_models_geartype.id \
                WHERE shared_models_set.id={set_pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @validate_int()
    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait du trait
        """
        return self.trait.get_no_releve()

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait du trait
        """
        return self.trait.get_ident_no_trait()

    @validate_int()
    @log_results
    def get_no_engin(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Numéro identifiant l'engin utilisé tel que défini dans la table NO_ENGIN

        Andes: shared_models_set.gear_type_id-> shared_models.geartype.code
        """
        set_pk = self.trait._get_current_set_pk()
        query = f"SELECT shared_models_set.winch_code \
                FROM shared_models_set \
                WHERE shared_models_set.id={set_pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @log_results
    def get_code_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait du trait
        """
        return self.trait.get_cod_nbpc()

    @log_results
    def get_cod_type_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER

        1 -> Panier standard
        2 -> Panier doublé

        Ces infos sont pas utiles car ils sont encodé dans sous cod_eng_gen
        Ils sont conservées pour des raisons historiques.

        This function returns the key for:
            - 1->'Panier standard' if 'Drague Digby (4 paniers non doublés)'
            - 2->'Panier doublé' if 'Drague Digby (4 paniers doublés)'
            - 3->'Aucun' (or '0->Pas de panier dans l’engin') for anyhing else

        This function raises ValueError an ambiguous case (needing manual intervention):
            - "Drague (non spécifiée)",
            - "Drague Digby 4 paniers (2 doublés-2 non doublés)",
            - "Drague Digby 3 paniers avec et sans doublure",
            - "Drague Digby 2 paniers",

        """

        cod_engin = self.get_cod_eng_gen()
        query = (
            "SELECT ENGIN_GENERAL.NOM_ENG_F "
            "FROM ENGIN_GENERAL "
            f"WHERE ENGIN_GENERAL.COD_ENG_GEN={cod_engin}"
        )
        result = self.reference_data.execute_query(query)
        self._assert_one(result)
        gear_name = result[0][0]
        self.logger.info("Need to find the code for %s", gear_name)

        # cases where automatic assignment needs manual intervention
        ambiguous_cases = [
            "Drague (non spécifiée)",
            "Drague Digby 4 paniers (2 doublés-2 non doublés)",
            "Drague Digby 3 paniers avec et sans doublure",
            "Drague Digby 2 paniers",
        ]
        if gear_name == "Drague Digby (4 paniers doublés)":
            # this means paniers doublées
            self.logger.info("Matched for %s", "Panier doublé")
            key_result = self.reference_data.get_ref_key(
                table="TYPE_PANIER",
                pkey_col="COD_TYP_PANIER",
                col="DESC_TYP_PANIER_F",
                val="Panier doublé",
            )

        elif gear_name == "Drague Digby (4 paniers non doublés)":
            # this means Panier standard
            self.logger.info("Matched for %s", "Panier standard")
            key_result = self.reference_data.get_ref_key(
                table="TYPE_PANIER",
                pkey_col="COD_TYP_PANIER",
                col="DESC_TYP_PANIER_F",
                val="Panier standard",
            )

        elif gear_name in ambiguous_cases:
            self.logger.error(
                "Incapable d'assigner une valeur automatique pour COD_TYP_PANIER"
            )
            raise ValueError

        else:
            # normaly this means a null result,
            if self.reference_data.ms_access:
                # none value in ACCESS db
                value_for_none = "Aucun"
            else:
                # none value in PSE db
                value_for_none = "Pas de panier dans l'engin"
            self.logger.info("Matched for %s", value_for_none)
            key_result = self.reference_data.get_ref_key(
                table="TYPE_PANIER",
                pkey_col="COD_TYP_PANIER",
                col="DESC_TYP_PANIER_F",
                val=value_for_none,
            )

        to_return = key_result
        return to_return
