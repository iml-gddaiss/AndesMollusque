

from andes_migrate.andes_helper import AndesHelper
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.table_peche_sentinelle import TablePecheSentinelle


class BiometriePetoncle(TablePecheSentinelle):

    def __init__(self, andes_db: AndesHelper, proj: ProjetMollusque, collection_name:str, *args, **kwargs):
        # super().__init__(*args, **kwargs)
        super().__init__(*args, ref=proj.reference_data, **kwargs)

        self.andes_db = andes_db
        self.proj: ProjetMollusque = proj
        self.collection_name:str = collection_name 
        # self.table_name = "TRAIT_MOLLUSQUE"

        # call this form outside
        self._init_rows()

    def populate_data(self):
        """Populate data: run all getters"""
        # secteur	trait	no	taille	poids_vif	poids_muscle	poids_gonade	poids_visceres	sexe	espece	comment
        if self.collection_name == 'Conserver le spécimen (Biométrie Centre)':
            secteur = "Centre"
        if self.collection_name == 'Conserver le spécimen (Biométrie Ouest)':
            secteur = "Ouest"

        self.data["id_specimen"] = self._get_current_row_pk()
        self.data["secteur"] = secteur
        self.data["trait"] = self.get_ident_no_trait()
        self.data["no"] = self.get_observation("Code Collection coquille")
        self.data["taille"] = self.get_observation("Longueur (non officiel)")
        self.data["poids_vif"] = self.get_observation("Poids vif")
        self.data["poids_muscle"] = self.get_observation("Poids du muscle")
        self.data["poids_gonade"] = self.get_observation("Poids des gonades")
        self.data["poids_visceres"] = self.get_observation("Poids des viscères")
        self.data["poids_gonade"] = self.get_observation("Poids des gonades")
        self.data["sexe"] = self.get_observation("Sexe")
        self.data["comment"] = self.get_comment()

    def __next__(self):
        """
        Increment to focus on next row
        """
        # print(self.table_name)
        # print(f"{self._row_idx} of {len(self._row_list)}")
        # print()
        if self._row_idx is not None and self._row_list is not None:
            if self._row_idx < len(self._row_list):
                # increment first,  it'l be adjusted in _get_current_row_pk()
                self._row_idx += 1
                self.populate_data()

                # self.write_row()
                return self.data
            else:
                raise StopIteration
        else:
            self.logger.error("Row data not initialise, did you run _init_rows()?")
            raise ValueError


    def _init_rows(self):
        """Initialisation method
        This queries the Andes DB and creates a list of row entries to be added to the current table

        After running this methods initialises the following attribute:
        self._row_list
        self._row_idx (hopefully to self._row_idx=0)

        self._row_list will be populated with the specimen ids belonging in the collection
        self._row_idx will start at 0

        """
        query = (
                 "SELECT specimen_id "
                 "FROM ecosystem_survey_observation "
                 "LEFT JOIN ecosystem_survey_specimen "
                 "ON ecosystem_survey_observation.specimen_id = ecosystem_survey_specimen.id "
                 "LEFT JOIN ecosystem_survey_basket "
                 "ON ecosystem_survey_specimen.basket_id = ecosystem_survey_basket.id "
                 "LEFT JOIN ecosystem_survey_catch "
                 "ON ecosystem_survey_basket.catch_id=ecosystem_survey_catch.id  "
                 "LEFT JOIN shared_models_set "
                 "ON shared_models_set.id=ecosystem_survey_catch.set_id "
                 "LEFT JOIN shared_models_observationtype "
                 "ON shared_models_observationtype.id=observation_type_id "
                 f"WHERE shared_models_set.cruise_id = {self.proj._get_current_row_pk()} "
                 f"AND (shared_models_observationtype.nom ='{self.collection_name}' AND observation_value=1) "
        )
        result = self.andes_db.execute_query(query)
        self._assert_not_empty(result)
        self._row_list = [specimen[0] for specimen in result]
        self._row_idx = 0

    # @validate_int()
    # @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Andes
        -----
        shared_models.set.set_number
        """
        specimen_pk = self._get_current_row_pk()
        query = (
            "SELECT shared_models_set.set_number "
            "FROM ecosystem_survey_specimen "
            "LEFT JOIN ecosystem_survey_basket "
            "ON ecosystem_survey_specimen.basket_id=ecosystem_survey_basket.id "
            "LEFT JOIN ecosystem_survey_catch "
            "ON ecosystem_survey_basket.catch_id=ecosystem_survey_catch.id "
            "LEFT JOIN shared_models_set "
            "ON shared_models_set.id=ecosystem_survey_catch.set_id "
            f"WHERE ecosystem_survey_specimen.id={specimen_pk} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return


    def get_observation(self, name_fr):
        specimen_pk = self._get_current_row_pk()
        query = (
            "SELECT observation_value "
            "FROM ecosystem_survey_observation "
            "LEFT JOIN shared_models_observationtype "
            "ON ecosystem_survey_observation.observation_type_id=shared_models_observationtype.id "
            f"WHERE ecosystem_survey_observation.specimen_id={specimen_pk} "
            f"AND shared_models_observationtype.nom='{name_fr}' "
        )
        result = self.andes_db.execute_query(query)
        try:
            self._assert_one(result)
            to_return = result[0][0]
        except ValueError:
            print(specimen_pk, name_fr,result)
            to_return = None
        return to_return
    
    def get_comment(self):
        specimen_pk = self._get_current_row_pk()
        query = (
             "SELECT ecosystem_survey_specimen.comment "
             "FROM ecosystem_survey_specimen "
            f"WHERE ecosystem_survey_specimen.id={specimen_pk} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        if to_return is None:
            to_return=""
        to_return.replace("\n", " ")
        to_return.replace("\r", " ")
        return to_return






