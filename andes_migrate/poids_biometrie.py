import logging
import numpy as np

from andes_migrate.biometrie_mollusque import BiometrieMollusque
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


class PoidsBiometrie(TablePecheSentinelle):
    """
    Object model representing the POIDS_BIOMETRIE table

    WARNING: only good for getting the weight of whelk eggs, other features (actual biometry) are not implemented
    """

    def __init__(self, biometrie: BiometrieMollusque, *args, **kwargs):
        super().__init__(*args, ref=biometrie.reference_data, **kwargs)

        self.biometrie: BiometrieMollusque = biometrie

        self.table_name = "POIDS_BIOMETRIE"
        self.andes_db = biometrie.andes_db
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
            f"WHERE ecosystem_survey_catch.id={self.biometrie.capture._get_current_row_pk()} "
            f"AND shared_models_species.aphia_id = {target_aphia_id} "
            f"AND ecosystem_survey_basket.size_class = {target_size_class} "
        )
        result = self.andes_db.execute_query(query)

        # a list of all the catch pk's (need to unpack a bit)
                # a list of all the catch pk's (need to unpack a bit)
        self._row_list = [basket_wt_kg[0] for basket_wt_kg in result]

        self._row_list = result
        self._row_idx = 0

    def get_current_weight(self):
        if self._row_idx is not None and self._row_list:
            # need to adjust becuse the iterator is already looking forward..
            adjusted_row_idx = self._row_idx-1
            return self._row_list[adjusted_row_idx][0]
        else:
            raise ValueError

    def populate_data(self):
        """Populate data: run all getters"""
        self.data["COD_ESP_GEN"] = self.get_cod_esp_gen()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["COD_ENG_GEN"] = self.get_cod_eng_gen()
        self.data["COD_TYP_PANIER"] = self.get_cod_typ_panier()
        self.data["NO_ENGIN"] = self.get_no_engin()

        self.data['NO_MOLLUSQUE'] = self.get_no_mollusque()

        self.data['COD_TYP_PDS'] = self.get_cod_typ_pds()
        self.data['VALEUR_PDS_MOLL'] = self.get_valeur_pds_moll()
        self.data['VALEUR_PDS_MOLL_P'] = self.get_valeur_pds_moll_p()
        self.data['NO_CHARGEMENT'] = self.get_no_chargement()

    def get_cod_esp_gen(self) -> int:
        """COD_ESP_GEN INTEGER / NUMBER(5,0)
        Identification de l'espèce capturée tel que défini dans la table ESPECE_GENERAL

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_cod_esp_gen`

        """
        return self.biometrie.get_cod_esp_gen()

    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_cod_eng_gen`

        """
        return self.biometrie.get_cod_eng_gen()

    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_cod_source_info`

        """
        return self.biometrie.get_cod_source_info()

    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_no_releve`
        """
        return self.biometrie.get_no_releve()

    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_ident_no_trait`
        """
        return self.biometrie.get_ident_no_trait()

    def get_cod_typ_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_cod_type_panier`

        """

        return self.biometrie.get_cod_typ_panier()

    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_cod_nbpc`

        """
        return self.biometrie.get_cod_nbpc()

    def get_no_engin(self) -> int:
        """NO_ENGIN INTEGER/ NUMBER(5,0)
        Numéro identifiant l'engin utilisé tel que défini dans la table NO_ENGIN

        Extrait de la capture ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_no_engin`

        """

        return self.biometrie.get_no_engin()

    @tag(HardCoded)
    @validate_int()
    def get_cod_typ_pds(self) -> int:
        """ COD_TYP_PDS INTEGER / NUMBER(5,0)
        Identification du type de poids évalué tel que défini dans la table TYPE_POIDS

        Hard-coded: This function always returns 1
        1 -> poids_total_frais
        """
        
        return self._hard_coded_result(1)

    def get_valeur_pds_moll(self) -> float|None:
        """ VALEUR_PDS FLOAT / NUMBER
        Valeur associée au type de poids évalué, unité gramme

        """
        return self.get_current_weight()*1000 

    @tag(HardCoded)
    def get_valeur_pds_moll_p(self) -> float|None:
        """ VALEUR_PDS_P FLOAT / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Valeur_Pds_Moll"

        Hard-coded: This function always returns None

        """
        return self._hard_coded_result(0.1)

    @validate_int()
    @log_results
    def get_no_mollusque(self) -> int :
        """NO_MOLLUSQUE INTEGER / NUMBER(5,0)
        Numéro séquentiel attribué à l'individus mesuré

        Extrait de la biometrie ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_no_mollusque`

        """

        return self.biometrie.get_no_mollusque()
        # raise NotImplementedError

    @log_results
    def get_no_chargement(self) -> float|None:
        """NO_CHARGEMENT DOUBLE / NUMBER
        Numéro de l'activité de chargement de données dans la base Oracle

        Extrait de la biometrie ::func:`~andes_migrate.biometrie_mollusque.BiometrieMollusque.get_no_chargement`

        """
        return self.biometrie.get_no_chargement()
