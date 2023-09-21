import logging
from andes_migrate.engin_mollusque import EnginMollusque

from andes_migrate.table_peche_sentinelle import TablePecheSentinelle
from andes_migrate.decorators import (
    log_results,
    validate_string,
    validate_int,
)

logging.basicConfig(level=logging.INFO)


class CaptureMollusque(TablePecheSentinelle):
    """ 
    Object model representing the CAPTURE_MOLLUSQUE table
    """

    def __init__(self, engin: EnginMollusque):
        super().__init__()
        self.engin: EnginMollusque = engin

        self.andes_db = engin.andes_db
        self.data = {}

    def populate_data(self):
        """_summary_
        """
        self.data['COD_SOURCE_INFO'] = self.get_cod_source_info()
        self.data['COD_ENG_GEN'] = self.get_cod_eng_gen()
        self.data['NO_RELEVE'] = self.get_no_releve()
        self.data['COD_ESP_GEN'] = self.get_cod_esp_gen()
        self.data['IDENT_NO_TRAIT'] = self.get_ident_no_trait()
        self.data['COD_TYP_PANIER'] = self.get_cod_type_panier()
        self.data['COD_NBPC'] = self.get_cod_nbpc()
        # self.data['FRACTION_PECH'] = self.get_
        # self.data['NO_ENGIN'] = self.get_
        # self.data['FRACTION_ECH'] = self.get_
        # self.data['COD_DESCRIP_CAPT'] = self.get_
        # self.data['FRACTION_ECH_P'] = self.get_
        # self.data['COD_TYP_MESURE'] = self.get_
        # self.data['NBR_CAPT'] = self.get_
        # self.data['FRACTION_PECH_P'] = self.get_
        # self.data['NBR_ECH'] = self.get_
        # self.data['PDS_CAPT'] = self.get_
        # self.data['PDS_CAPT_P'] = self.get_
        # self.data['PDS_ECH'] = self.get_
        # self.data['PDS_ECH_P'] = self.get_
        # self.data['NO_CHARGEMENT'] = self.get_
        # self.data['COD_ABONDANCE_EPIBIONT'] = self.get_
        # self.data['COD_COUVERTURE_EPIBIONT'] = self.get_
        # self.data['REM_CAPT_MOLL'] = self.get_

    @validate_int()
    @log_results
    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait du trait
        """

        return self.engin.get_cod_source_info()

    @validate_int()
    @log_results
    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Andes: shared_models_set.gear_type_id-> shared_models.geartype.code
        """
        return self.engin.get_cod_eng_gen()

    @validate_int()
    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait du trait
        """
        return self.engin.get_no_releve()

    @validate_int()
    @log_results
    def get_cod_esp_gen(self) -> int:
        """ COD_ESP_GEN INTEGER / NUMBER(5,0)
        Identification de l'espèce capturée tel que défini dans la table ESPECE_GENERAL

        """
        return to_return

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait du trait
        """
        return self.engin.get_ident_no_trait()

    @log_results
    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait du trait
        """
        return self.engin.get_cod_nbpc()

    @validate_int()
    @log_results
    def get_cod_type_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER
        """
        return self.engin.get_cod_type_panier()

