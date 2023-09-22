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
        super().__init__(ref=engin.reference_data)

        self.engin: EnginMollusque = engin
        self.andes_db = engin.andes_db
        self.data = {}

        self._init_rows()

    def _init_rows(self):
        """Initialisation method
        This queries the Andes DB and creaters a list of row entries to be added to the current table

        After running this methods initialises the following attribute:
        self._row_list
        self._row_idx (hopefully to self._row_idx=0)

        self._row_list will be populated with the associated Andes catch ids for the current set
        self._row_idx will start at 0

        """
        query = (
            "SELECT ecosystem_survey_catch.id "
            "FROM ecosystem_survey_catch "
            f"WHERE ecosystem_survey_catch.set_id={self.engin.trait._get_current_row_pk()} "
            "ORDER BY ecosystem_survey_catch.id ASC;"
        )

        result = self.andes_db.execute_query(query)
        self._assert_not_empty(result)

        # a list of all the catch pk's (need to unpack a bit)
        self._row_list = [catch[0] for catch in result]
        self._row_idx = 0

    def populate_data(self):
        """Populate data: run all getters"""

        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["COD_ENG_GEN"] = self.get_cod_eng_gen()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["COD_ESP_GEN"] = self.get_cod_esp_gen()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["COD_TYP_PANIER"] = self.get_cod_type_panier()
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["FRACTION_PECH"] = self.get_fraction_peche()
        self.data["NO_ENGIN"] = self.get_no_engin()
        self.data["FRACTION_ECH"] = self.get_fraction_ech()
        self.data['COD_DESCRIP_CAPT'] = self.get_cod_descrip_capt()
        self.data['FRACTION_ECH_P'] = self.get_fraction_ech_p()
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

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_cod_source_info`

        """

        return self.engin.get_cod_source_info()

    @validate_int()
    @log_results
    def get_cod_eng_gen(self) -> int:
        """COD_ENG_GEN INTEGER / NUMBER(5,0)
        Identification de l'engin de pêche utilisé tel que défini dans la table ENGIN_GENERAL

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_cod_eng_gen`

        """
        return self.engin.get_cod_eng_gen()

    @validate_int()
    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_no_releve`
    """
        return self.engin.get_no_releve()

    @validate_int()
    @log_results
    def get_cod_esp_gen(self) -> int:
        """COD_ESP_GEN INTEGER / NUMBER(5,0)
        Identification de l'espèce capturée tel que défini dans la table ESPECE_GENERAL

        Associating this code to the andes species will require jumping through a number
        of hoops.
        On the Andes side, the mission has a `code_collection` that defines the regional
        code vernacular (eg., `STRAP` vs `RVAN`). Extracting a code will necessitate going
        throught the mapping table (unless we assume vernacular was already applied).

        On the Oracle side there is a similar challenge: Species are internally stored with
        `COD_ESP_GEN` that is eventually mapped to a species code using the NORM (again,
        typically `IML_STRAP` or `RVAN`).

        The link is initially made with the mapped values (ex., Andes.id->-strap =
        Oracle.id->strap) but then reversed to extract the needed `COD_ESP_GEN`.

        If a species cannot be mapped to a pre-existing `COD_ESP_GEN`, an item is added to
        the tasks that need to be made prior to commiting the data.


        """
        query = (
            "SELECT shared_models_species.id, shared_models_species.aphia_id, shared_models_species.code "
            "FROM ecosystem_survey_catch "
            "LEFT JOIN shared_models_species "
            "ON shared_models_species.id=ecosystem_survey_catch.species_id "
            f"WHERE ecosystem_survey_catch.id={self._get_current_row_pk()}"
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        andes_id = result[0][0]
        andes_aphia_id = result[0][1]
        andes_code = result[0][2]
        # print( andes_id, andes_aphia_id, andes_code)

        # try to match an aphia_id
        self.logger.info(
            "Will try to match to Oracle using aphia_id=%s", andes_aphia_id
        )
        norme_name_str = "AphiaId"
        query = (
            "SELECT ESPECE_NORME.COD_ESP_GEN "
            "FROM ESPECE_NORME "
            "LEFT JOIN NORME "
            "ON ESPECE_NORME.COD_NORME=NORME.COD_NORME "
            f"WHERE NORME.NOM_NORME='{norme_name_str}' "
            f"AND ESPECE_NORME.COD_ESPECE={andes_aphia_id}"
        )

        result = self.reference_data.execute_query(query)
        try:
            self._assert_one(result)
        except ValueError:
            self.logger.warn(
                "did not match to Oracle using aphia_id=%s", andes_aphia_id
            )
        else:
            to_return = result[0][0]
            self.logger.info(
                "Found matching aphia id, returning COD_ESP_GEN=%s", to_return
            )
            # return to_return

        # try to match with (assumed) strap code
        self.logger.info("Will try to match to Oracle assuming strap=%s", andes_code)
        norme_name_str = "STRAP_IML"
        query = (
            "SELECT ESPECE_NORME.COD_ESP_GEN "
            "FROM ESPECE_NORME "
            "LEFT JOIN NORME "
            "ON ESPECE_NORME.COD_NORME=NORME.COD_NORME "
            f"WHERE NORME.NOM_NORME='{norme_name_str}' "
            f"AND ESPECE_NORME.COD_ESPECE={andes_code}"
        )

        result = self.reference_data.execute_query(query)
        try:
            self._assert_one(result)
        except ValueError:
            self.logger.warn(
                "did not match to Oracle using code=%s (assuming strap)", andes_code
            )
            raise (ValueError)
        else:
            to_return = result[0][0]
            self.logger.info(
                "Found matching strap code, returning COD_ESP_GEN=%s", to_return
            )
            return to_return

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_ident_no_trait`
        """
        return self.engin.get_ident_no_trait()

    @validate_int()
    @log_results
    def get_cod_type_panier(self) -> int:
        """COD_TYP_PANIER INTEGER / NUMBER(5,0)
        Identification du type de panier utilisé tel que défini dans la table TYPE_PANIER

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_cod_type_panier`

        """

        return self.engin.get_cod_type_panier()

    @log_results
    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_cod_nbpc`

        """
        return self.engin.get_cod_nbpc()

    @log_results
    def get_fraction_peche(self) -> float:
        """FRACTION_PECH DOUBLE / NUMBER
        Fraction de panier qui a effectivement pêché. Nb paniers qui ont pêché sur nb paniers installés

        N.B. the description seems wrong, it's not the fraction of baskets, rather the number of baskets
        Oracle Optimisation: this datatype should be INTEGER

        Andes: This is currently not recorded in Andes.
        Hard-coded: This functino always returns 4

        """

        to_return = self._hard_coded_result(4)
        return to_return

    @log_results
    def get_no_engin(self) -> int:
        """NO_ENGIN INTEGER/ NUMBER(5,0)
        Numéro identifiant l'engin utilisé tel que défini dans la table NO_ENGIN

        Extrait de l'engin ::func:`~andes_migrate.engin_mollusque.EnginMollusque.get_no_engin`

        """

        return self.engin.get_no_engin()

    @log_results
    def get_fraction_ech(self)-> float:
        """FRACTION_ECH DOUBLE / NUMBER
        Fraction échantillonné. Nb paniers échantillonnés sur Nb paniers total de l'engin de pêche

        Andes: This is currently not recorded in Andes.

        Hard-coded: This function always returns 4

        """

        to_return = self._hard_coded_result(4)
        return to_return

    @validate_int(not_null=False)
    @log_results
    def get_cod_descrip_capt(self) -> int| None:
        """COD_DESCRIP_CAPT INTEGER / NUMBER(5,0)
        Description qualitative de la capture tel que définie dans la table DESCRIP_CAPT_MOLL

        0 -> Espèce absente dans la capture
        1 -> Espèce présente dans la capture
        2 -> Espèce abondante dans la capture
        3 -> Espèce très abondante dans la capture

        Andes: shared_models_relativeabundancecategory.code
         via ecosystem_survey_catch.relative_abundance_category_id 

        N.B. An effort has to be made to ensure the categories match between Andes and Oracle

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        Code lookup
        """
        query = (
            "SELECT shared_models_relativeabundancecategory.code "
            "FROM ecosystem_survey_catch "
            "LEFT JOIN shared_models_relativeabundancecategory "
            "ON shared_models_relativeabundancecategory.id=ecosystem_survey_catch.relative_abundance_category_id "
            f"WHERE ecosystem_survey_catch.id={self._get_current_row_pk()}"
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        to_return = result[0][0]
        return to_return

    @log_results
    def get_fraction_ech_p(self) -> float | None:
        """FRACTION_ECH_P DOUBLE / NUMBER 
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Fraction_Ech"

        Andes: This is currently not recorded in Andes.

        Hard-coded: This function always returns None
        """
        return self._hard_coded_result(None)
    

    