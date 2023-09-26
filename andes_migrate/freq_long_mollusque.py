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

logging.basicConfig(level=logging.INFO)


class FreqLongMollusque(TablePecheSentinelle):
    """
    Object model representing the FREQ_LONG_MOLLUSQUE table
    """

    def __init__(self, capture: CaptureMollusque):
        super().__init__(ref=capture.reference_data)

        self.capture: CaptureMollusque = capture
        self.andes_db = capture.andes_db
        self.data = {}

        self._init_rows()

    def _init_rows(self):
        """Initialisation method
        This queries the Andes DB and creates a list of row entries to be added to the current table

        After running this methods initialises the following attribute:
        self._row_list
        self._row_idx (hopefully to self._row_idx=0)

        self._row_list will be populated with the associated Andes catch ids for the current set
        self._row_idx will start at 0

        """
        observation_length_type_id = 0
        query = (
            "SELECT ecosystem_survey_specimen.id, ecosystem_survey_observation.observation_value "
            "FROM ecosystem_survey_catch "
            "LEFT JOIN ecosystem_survey_basket "
            "ON ecosystem_survey_catch.id=ecosystem_survey_basket.catch_id "
            "LEFT JOIN ecosystem_survey_specimen "
            "ON ecosystem_survey_specimen.basket_id = ecosystem_survey_basket.id "
            "LEFT JOIN ecosystem_survey_observation "
            "ON ecosystem_survey_observation.specimen_id=ecosystem_survey_specimen.id  "
            "LEFT JOIN shared_models_observationtypecategory "
            "ON shared_models_observationtypecategory.observation_type_id=ecosystem_survey_observation.id  "
            f"WHERE ecosystem_survey_catch.id={self.capture._get_current_row_pk()} "
            f"AND ecosystem_survey_observation.observation_type_id='{observation_length_type_id}' "
        )

        result = self.andes_db.execute_query(query)

        # a list of all the catch pk's (need to unpack a bit)
        self._row_list = [catch[0] for catch in result]
        self._row_idx = 0

    def populate_data(self):
        """Populate data: run all getters"""
        self.data['COD_ESP_GEN'] = self.get_cod_esp_gen()
        self.data['COD_ENG_GEN'] = self.get_cod_eng_gen()
        self.data['COD_SOURCE_INFO'] = self.get_cod_source_info()
        self.data['NO_RELEVE'] = self.get_no_releve()
        self.data['IDENT_NO_TRAIT'] = self.get_ident_no_trait()
        self.data['COD_TYP_PANIER'] = self.get_cod_typ_panier()
        self.data['COD_NBPC'] = self.get_cod_nbpc()
        self.data['NO_ENGIN'] = self.get_no_engin()
        # self.data['VALEUR_LONG_MOLL'] = self.get_()
        # self.data['NO_MOLLUSQUE'] = self.get_()
        # self.data['COD_TYP_LONG'] = self.get_()
        # self.data['VALEUR_LONG_MOLL_P'] = self.get_()
        # self.data['COD_TYP_ETAT'] = self.get_()
        # self.data['NO_CHARGEMENT'] = self.get_()
        # self.data['COD_TECH_MESURE_LONG'] = self.get_()


    @validate_int()
    @log_results
    def get_cod_esp_gen(self) -> int:
        """COD_ESP_GEN INTEGER / NUMBER(5,0)
        Identification de l'espèce capturée tel que défini dans la table ESPECE_GENERAL

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CapturenMollusque.get_cod_esp_gen`

        """
        return self.capture.get_cod_esp_gen()

    
    @validate_int()
    @log_results
    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CapturenMollusque.get_cod_eng_gen`

        """
        return self.capture.get_cod_source_info()

    @validate_int()
    @log_results
    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_cod_source_info`

        """

        return self.capture.get_cod_source_info()
    
    @validate_int()
    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CAptureMollusque.get_no_releve`
        """
        return self.capture.get_no_releve()

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_ident_no_trait`
        """
        return self.capture.get_ident_no_trait()

    @validate_int()
    @log_results
    def get_cod_typ_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_cod_type_panier`

        """

        return self.capture.get_cod_typ_panier()

    @log_results
    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_cod_nbpc`

        """
        return self.capture.get_cod_nbpc()
    
    @log_results
    def get_no_engin(self) -> int:
        """NO_ENGIN INTEGER/ NUMBER(5,0)
        Numéro identifiant l'engin utilisé tel que défini dans la table NO_ENGIN

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_no_engin`

        """

        return self.capture.get_no_engin()
