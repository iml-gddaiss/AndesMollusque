import logging
import numpy as np

from andes_migrate.capture_mollusque import CaptureMollusque
from andes_migrate.table_peche_sentinelle import TablePecheSentinelle
from andes_migrate.decorators import (
    NotAndes,
    Computed,
    HardCoded,
    Deprecated,
    AndesCodeLookup,
    tag,
    log_results,
    validate_string,
    validate_int,
)

# logging.basicConfig(level=logging.INFO)


class FreqLongMollusque(TablePecheSentinelle):
    """
    Object model representing the FREQ_LONG_MOLLUSQUE table
    """

    def __init__(self, capture: CaptureMollusque, *args, no_moll_init=0, **kwargs):
        super().__init__(*args, ref=capture.reference_data, **kwargs)

        self.capture: CaptureMollusque = capture

        self.table_name = "FREQ_LONG_MOLLUSQUE"
        self.andes_db = capture.andes_db
        self.data = {}
        self._init_rows()
        self.no_moll = no_moll_init

    def _init_rows(self):
        """Initialisation method
        This queries the Andes DB and creates a list of row entries to be added to the current table

        After running this methods initialises the following attribute:
        self._row_list
        self._row_idx (hopefully to self._row_idx=0)

        self._row_list will be populated with the associated Andes catch ids for the current set
        self._row_idx will start at 0
        For this class, the row will contain a tuple: (specimen_id, length, basket_id)

        """
        # HACK, change this using lookup
        observation_length_type_id = 7
        query = (
            "SELECT ecosystem_survey_specimen.id, ecosystem_survey_observation.observation_value, ecosystem_survey_basket.id "
            "FROM ecosystem_survey_catch "
            "LEFT JOIN ecosystem_survey_basket "
            "ON ecosystem_survey_catch.id=ecosystem_survey_basket.catch_id "
            "LEFT JOIN ecosystem_survey_specimen "
            "ON ecosystem_survey_specimen.basket_id = ecosystem_survey_basket.id "
            "LEFT JOIN ecosystem_survey_observation "
            "ON ecosystem_survey_observation.specimen_id=ecosystem_survey_specimen.id "
            "LEFT JOIN shared_models_observationtypecategory "
            "ON shared_models_observationtypecategory.observation_type_id=ecosystem_survey_observation.id  "
            f"WHERE ecosystem_survey_catch.id={self.capture._get_current_row_pk()} "
            f"AND ecosystem_survey_observation.observation_type_id={observation_length_type_id} "
        )
        result = self.andes_db.execute_query(query)

        # a list of all the catch pk's (need to unpack a bit)
        self._row_list = result
        self._row_idx = 0

    def get_current_specimen(self):
        if self._row_idx is not None and self._row_list:
            # need to adjust becuse the iterator is already looking forward..
            adjusted_row_idx = self._row_idx-1
            return self._row_list[adjusted_row_idx][0]
        else:
            raise ValueError

    def get_current_length(self):
        if self._row_idx is not None and self._row_list:
            # need to adjust becuse the iterator is already looking forward..
            adjusted_row_idx = self._row_idx-1
            return self._row_list[adjusted_row_idx][1]
        else:
            raise ValueError

    def get_current_basket_id(self):
        if self._row_idx is not None and self._row_list:
            # need to adjust becuse the iterator is already looking forward..
            adjusted_row_idx = self._row_idx-1
            return self._row_list[adjusted_row_idx][2]
        else:
            raise ValueError

    def populate_data(self):
        """Populate data: run all getters"""
        self.data["COD_ESP_GEN"] = self.get_cod_esp_gen()
        self.data["COD_ENG_GEN"] = self.get_cod_eng_gen()
        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["COD_TYP_PANIER"] = self.get_cod_typ_panier()
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["NO_ENGIN"] = self.get_no_engin()
        self.data["VALEUR_LONG_MOLL"] = self.get_valeur_long_moll()
        self.data['NO_MOLLUSQUE'] = self.get_no_mollusque()
        self.data["COD_TYP_LONG"] = self.get_cod_typ_long()
        self.data["VALEUR_LONG_MOLL_P"] = self.get_valeur_long_moll_p()
        self.data["COD_TYP_ETAT"] = self.get_cod_typ_etat()
        self.data['NO_CHARGEMENT'] = self.get_no_chargement()
        self.data['COD_TECH_MESURE_LONG'] = self.get_cod_tech_mesure_long()

    def get_cod_esp_gen(self) -> int:
        """COD_ESP_GEN INTEGER / NUMBER(5,0)
        Identification de l'espèce capturée tel que défini dans la table ESPECE_GENERAL

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CapturenMollusque.get_cod_esp_gen`

        """
        return self.capture.get_cod_esp_gen()

    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CapturenMollusque.get_cod_eng_gen`

        """
        return self.capture.get_cod_eng_gen()

    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_cod_source_info`

        """
        return self.capture.get_cod_source_info()

    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CAptureMollusque.get_no_releve`
        """
        return self.capture.get_no_releve()

    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_ident_no_trait`
        """
        return self.capture.get_ident_no_trait()

    def get_cod_typ_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_cod_type_panier`

        """

        return self.capture.get_cod_typ_panier()

    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_cod_nbpc`

        """
        return self.capture.get_cod_nbpc()

    def get_no_engin(self) -> int:
        """NO_ENGIN INTEGER/ NUMBER(5,0)
        Numéro identifiant l'engin utilisé tel que défini dans la table NO_ENGIN

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_no_engin`

        """

        return self.capture.get_no_engin()

    @log_results
    def get_valeur_long_moll(self) -> float | None:
        """VALEUR_LONG_MOLL DOUBLE / NUMBER
        Valeur associée au type de longueur évalué, unité mm

        Andes: ecosystem_survey_observation.observation_value
        for length observation in the current scope (catch)

        """
        # the specimen length is found in the _row_list
        self.get_current_length()

    @validate_int()
    @log_results
    def get_no_mollusque(self) -> int :
        """NO_MOLLUSQUE INTEGER / NUMBER(5,0)
        Numéro séquentiel attribué à l'individus mesuré

        """
        adjusted_row_idx = self._row_idx-1

        return self.no_moll + adjusted_row_idx
        # raise NotImplementedError

    @tag(AndesCodeLookup)
    @validate_int()
    @log_results
    def get_cod_typ_long(self) -> int:
        """COD_TYP_LONG INTEGER / NUMBER(5,0)
        Identification du type de longueur évaluée tel que défini dans la table TYPE_LONGUEUR

        18  -> Hauteur coquille
        Hauteur de la coquille - Charnière (Apex) vers extérieur

        Andes: Could get the observation type and make a lookup?
        """
        # TODO copy verbatim values for the Andes observation and match
        observation_name = "Hauteur coquille"
        length_code = self.reference_data.get_ref_key(
            table="TYPE_LONGUEUR",
            pkey_col="COD_TYP_LONG",
            col="NOM_TYP_LONG_F",
            val=observation_name,
        )
        return length_code

    @tag(NotAndes)
    @log_results
    def get_valeur_long_moll_p(self) -> float | None:
        """VALEUR_LONG_MOLL_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Valeur_Long_Moll"

        Hard-coded: This function always returns 0.01

        """
        return self._hard_coded_result(0.01)

    @tag(AndesCodeLookup)
    @validate_string(max_len=2)
    @log_results
    def get_cod_typ_etat(self) -> str:
        """COD_TYP_ETAT VARCHAR(5) / VARCHAR2(5)
        Spécification sur l'état du mollusque mesuré tel que défini dans la table TYPE_ETAT_MOLL

        Get from Andes basket size-class
        Andes: shared_models_sizeclass.description_fra via ecosystem_survey_basket.size_class
        A match to is made using the description_fra

        """
        query = (
            "SELECT shared_models_sizeclass.description_fra "
            "FROM ecosystem_survey_basket "
            "LEFT JOIN shared_models_sizeclass  "
            "ON ecosystem_survey_basket.size_class = shared_models_sizeclass.code  "
            "LEFT JOIN shared_models_cruise  "
            "ON shared_models_cruise.sampling_protocol_id = shared_models_sizeclass.sampling_protocol_id  "
            f"WHERE shared_models_cruise.id={self.capture.engin.trait.proj._get_current_row_pk()} "
            f"AND ecosystem_survey_basket.id={self.get_current_basket_id()} "
        )

        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        andes_desc = result[0][0]

        code = self.reference_data.get_ref_key(
            table="TYPE_ETAT_MOLL",
            pkey_col="COD_TYP_ETAT",
            col="DESC_TYP_ETAT_F",
            val=andes_desc,
        )
        self.logger.info("Andes size-class: %s matched with Oracle code: %s",andes_desc, code)
        return code

    @log_results
    def get_no_chargement(self) -> float|None:
        """NO_CHARGEMENT DOUBLE / NUMBER
        Numéro de l'activité de chargement de données dans la base Oracle

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_no_engin`

        """
        return self.capture.get_no_chargement()

    @tag(HardCoded, NotAndes)
    @validate_int(min_val=1, max_val=6)
    @log_results
    def get_cod_tech_mesure_long(self):
        """COD_TECH_MESURE_LONG INTERGER / NUMBER(5,0)
        Technique de mesure utilisée tel que défini dans la table TECHNIQUE_MESURE_LONG

        1 -> Vernier électronique (longueurs seulement)
        2 -> Vernier électronique (longueurs et poids)
        3 -> Vernier analogique
        4 -> Saisie manuelle (binoculaire)
        5 -> Correction manuelle vernier
        6 -> Planche à mesurer incurvée longitudinale

        Andes: This data type is not logged, perhaps it should?
        Hard-coded: This function always returns 1 
        """
        return self._hard_coded_result(1)
