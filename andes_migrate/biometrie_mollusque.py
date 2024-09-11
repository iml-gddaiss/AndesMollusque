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


class BiometrieMollusque(TablePecheSentinelle):
    """
    Object model representing the BIOMETRIE_MOLLUSQUE table

    WARNING: only good for getting the weight of whelk eggs, other features (actual biometry) are not implemented
    """

    def __init__(self, capture: CaptureMollusque, *args, no_moll_init=0, **kwargs):
        super().__init__(*args, ref=capture.reference_data, **kwargs)

        self.capture: CaptureMollusque = capture

        self.table_name = "BIOMETRIE_MOLLUSQUE"
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

        # HACK! only choose baskets that are WHELK EGGs size-class (shared_models_sizeclass.code=3)
        # 3 -> oeufs buccin
        target_size_class = 3

        # DOUBLE HACK! only choose whelkes species (aphia_id=138878)
        target_aphia_id = 138878

        query = (
            "SELECT basket_wt_kg "
            "FROM ecosystem_survey_catch  "
            "LEFT JOIN ecosystem_survey_basket  "
            "ON ecosystem_survey_catch.id=ecosystem_survey_basket.catch_id "
            "LEFT JOIN shared_models_species "
            "ON ecosystem_survey_catch.species_id = shared_models_species.id "
            f"WHERE ecosystem_survey_catch.id={self.capture._get_current_row_pk()} "
            f"AND shared_models_species.aphia_id = {target_aphia_id} "
            f"AND ecosystem_survey_basket.size_class = {target_size_class} "
        )
        result = self.andes_db.execute_query(query)

        # a list of all the catch pk's (need to unpack a bit)
        self._row_list = result
        self._row_idx = 0

    def populate_data(self):
        """Populate data: run all getters"""
        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["COD_ENG_GEN"] = self.get_cod_eng_gen()
        self.data["COD_TYP_PANIER"] = self.get_cod_typ_panier()
        self.data["NO_ENGIN"] = self.get_no_engin()
        self.data["COD_ESP_GEN"] = self.get_cod_esp_gen()
        self.data['NO_MOLLUSQUE'] = self.get_no_mollusque()

        self.data['COD_SEXE'] = self.get_cod_sexe()
        self.data['VOLUME_GONADE'] = self.get_volume_gonade()
        self.data['VOLUME_GONADE_P'] = self.get_volume_gonade_p()
        self.data['NO_CHARGEMENT'] = self.get_no_chargement()

    @tag(HardCoded)
    def get_cod_esp_gen(self) -> int:
        """COD_ESP_GEN INTEGER / NUMBER(5,0)
        Identification de l'espèce capturée tel que défini dans la table ESPECE_GENERAL

        hard-coded to whelk eggs: 2151
        """
        return self._hard_coded_result(2151)

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

    @tag(HardCoded)
    @validate_int()
    def get_cod_sexe(self) -> int:
        """COD_SEXE INTEGER / NUMBER(5,0)
        Sexe de l''organisme tel que décrit dans la table SEXE

        Hard-coded: This function always returns 9
        9 -> inconnu / indéterminé
        """
        
        return self._hard_coded_result(9)

    @tag(HardCoded)
    def get_volume_gonade(self) -> float|None:
        """VOLUME_GONADE FLOAT / NUMBER
        Volume occupé par les gonades unité ml

        Hard-coded: This function always returns None

        """
        return self._hard_coded_result(None)

    @tag(HardCoded)
    def get_volume_gonade_p(self) -> float|None:
        """VOLUME_GONADE_P FLOAT / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Volume_Gonade"

        Hard-coded: This function always returns None

        """
        return self._hard_coded_result(None)

    @validate_int()
    @log_results
    def get_no_mollusque(self) -> int :
        """NO_MOLLUSQUE INTEGER / NUMBER(5,0)
        Numéro séquentiel attribué à l'individus mesuré

        """
        adjusted_row_idx = self._row_idx-1

        return self.no_moll + adjusted_row_idx
        # raise NotImplementedError

    @log_results
    def get_no_chargement(self) -> float|None:
        """NO_CHARGEMENT DOUBLE / NUMBER
        Numéro de l'activité de chargement de données dans la base Oracle

        Extrait de la capture ::func:`~andes_migrate.capture_mollusque.CaptureMollusque.get_no_engin`

        """
        return self.capture.get_no_chargement()
