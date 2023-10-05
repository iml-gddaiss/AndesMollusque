import logging

from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.table_peche_sentinelle import TablePecheSentinelle
from andes_migrate.decorators import (
    AndesCodeLookup,
    Deprecated,
    HardCoded,
    tag,
    log_results,
    validate_string,
    validate_int,
)

# logging.basicConfig(level=logging.INFO)


class EnginMollusque(TablePecheSentinelle):
    """
    Object model representing the ENGIN_MOLLUSQUE table
    """

    def __init__(self, trait: TraitMollusque, *args, **kwargs):
        super().__init__(*args, ref=trait.reference_data, **kwargs)
        self.trait: TraitMollusque = trait
        self.table_name = "ENGIN_MOLLUSQUE"

        self.andes_db = trait.andes_db
        self.data = {}

        self._init_rows()

    def _init_rows(self):
        """ There is always one ENGIN_MOLLUSQUE per set.
        This function inits a dummy list

        """
        self._row_list = [None]
        self._row_idx = 0

    def populate_data(self):
        """Populate data: run all getters"""

        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["COD_ENG_GEN"] = self.get_cod_eng_gen()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["NO_ENGIN"] = self.get_no_engin()
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["COD_TYP_PANIER"] = self.get_cod_typ_panier()
        self.data["NO_CHARGEMENT"] = self.get_no_chargement()
        self.data["LONG_FUNE"] = self.get_long_fune()
        self.data["LONG_FUNE_P"] = self.get_long_fune_p()
        self.data["NB_PANIER"] = self.get_nb_panier()
        self.data["REMPLISSAGE"] = self.get_remplissage()
        self.data["REMPLISSAGE_P"] = self.get_remplissage_p()
        # self.data["REM_ENGIN_MOLL"] = self.get_rem_engin_moll()

    @validate_int()
    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait du trait::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_cod_source_info`

        """

        return self.trait.get_cod_source_info()

    @validate_int()
    @tag(AndesCodeLookup)
    @log_results
    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Andes: shared_models_set.gear_type_id-> shared_models.geartype.code

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        Code lookup

        """
        set_pk = self.trait._get_current_row_pk()

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

        Extrait du trait::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_no_releve`

        """
        return self.trait.get_no_releve()

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait du trait::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_ident_no_trait`
        """
        return self.trait.get_ident_no_trait()

    @validate_int()
    @tag(AndesCodeLookup)
    @log_results
    def get_no_engin(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Numéro identifiant l'engin utilisé tel que défini dans la table NO_ENGIN

        Andes: shared_models_set.gear_type_id-> shared_models.geartype.code

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        Code lookup

        """

        set_pk = self.trait._get_current_row_pk()
        query = (
            "SELECT shared_models_set.winch_code "
            "FROM shared_models_set "
            "LEFT JOIN shared_models_set_operations "
            "ON shared_models_set_operations.set_id = shared_models_set.id "
            "LEFT JOIN shared_models_operation "
            "ON shared_models_operation.id = shared_models_set_operations.operation_id "

            f"WHERE shared_models_set.id={set_pk} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @log_results
    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait du trait::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_cod_nbpc`
        """
        return self.trait.get_cod_nbpc()

    @validate_int()
    @tag(AndesCodeLookup)
    @log_results
    def get_cod_typ_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER

            - 1 -> Panier standard
            - 2 -> Panier doublé

        Ces infos sont pas utiles car ils sont encodé dans sous cod_eng_gen
        Ils sont conservées pour des raisons historiques.

        This function returns the key for:
            - 1->'Panier standard' if 'Drague Digby (4 paniers non doublés)'
            - 2->'Panier doublé' if 'Drague Digby (4 paniers doublés)'
            - 3->'Aucun' (or '0->Pas de panier dans l’engin') for anyhing else

        This function raises ValueError for an ambiguous case (needing manual intervention):
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

    @validate_int(not_null=False)
    @log_results
    def get_no_chargement(self) -> float | int | None:
        """NO_CHARGEMENT DOUBLE / NUMBER
        Numéro de l'activité de chargement de données dans la base Oracle

        Uses Projet, self.proj.get_no_chargement (via trait member)

        """
        return self.trait.get_no_chargement()

    @log_results
    def get_long_fune(self) -> float | None:
        """LONG_FUNE DOUBLE / NUMBER
        Longueur des funes lors de la réalisation du trait; unité mètre

        For scallops, it is assumed to be 3x depth 
        
        The Andes associated field is shared_models_set.trawl_cable_length
        """
        set_pk = self.trait._get_current_row_pk()
        query = f"SELECT shared_models_set.trawl_cable_length \
                FROM shared_models_set \
                WHERE shared_models_set.id={set_pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @tag(HardCoded)
    @log_results
    def get_long_fune_p(self) -> float | None:
        """LONG_FUNE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Long_Fune"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty
        For the moment, Andes does not log this data.
        """
        return self._hard_coded_result(None)

    @validate_int(not_null=False)
    @tag(HardCoded)
    @log_results
    def get_nb_panier(self) -> int | None:
        """NB_PANIER INTEGER / NUMBER(5,0)
        Nombre total de paniers dans la drague

        For the moment, Andes does not log this data.
        It is assumed to be 4 unless specified in set comments
        """
        # HACK what to do here?
        return self._hard_coded_result(4)

    @log_results
    def get_remplissage(self) -> float | None:
        """REMPLISSAGE DOUBLE / NUMBER
        Indique, en valeur de %, le remplissage de la drague

        The Andes associated field is shared_models_set.fill_percent

        """
        set_pk = self.trait._get_current_row_pk()
        query = f"SELECT shared_models_set.fill_percent \
                FROM shared_models_set \
                WHERE shared_models_set.id={set_pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @tag(HardCoded)
    @log_results
    def get_remplissage_p(self) -> float | None:
        """REMPLISSAGE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée "Remplissage"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty
        For the moment, Andes does not log this data (there is an implied uncertainty of about 5%).

        """

        return self._hard_coded_result(None)

    @validate_string(max_len=255, not_null=False)
    @tag(HardCoded, Deprecated)
    @log_results
    def get_rem_engin_moll(self) -> str | None:
        """REM_ENGIN_MOLL VARCHAR(255) / VARCHAR2(255)
        Remarque sur les opérations au niveau de l''engin

        All historical values are null except for a dozen in relevé 16.
        These comment can easily be moved to Set comment and the col can be removed.
       
        This IMLP col is not in Access

        """

        return self._hard_coded_result(None)
