import logging
import datetime

from andes_migrate.andes_helper import AndesHelper
from andes_migrate.peche_sentinelle import TablePecheSentinelle
from andes_migrate.decorators import log_results, validate_string, validate_int

logging.basicConfig(level=logging.INFO)


class ProjetMollusque(TablePecheSentinelle):
    def __init__(self, andes_db):
        super().__init__()

        self.andes_db = andes_db

        self.pk = None
        self.espece = None
        self.data = {}

        # this may have to be modified to include milisecs
        self.andes_datetime_format = "%Y-%m-%d %H:%M:%S"

    def validate(self):
        # use zones are compatible with code_source_info
        cod_source_info = self.get_cod_source_info()
        if self.zone in ["16E", "16F"] and not cod_source_info == 18:
            raise ValueError(
                f"La zones {self.zone} est incompatible avec le COD_SOURCE_INFO: {cod_source_info}"
            )
        if self.zone in ["20"] and not cod_source_info == 19:
            raise ValueError(
                f"La zone {self.zone} est incompatible avec le COD_SOURCE_INFO: {cod_source_info}"
            )

        # year compatible with start and end dates
        year = self.get_annee()
        # year start
        date_start = datetime.datetime.strptime(
            self.get_date_deb_project(), self.andes_datetime_format
        )
        if not year == date_start.year:
            raise ValueError(
                f"La date debut {date_start.year} est incompatible avec l'année: {year}"
            )
        # year end
        date_end = datetime.datetime.strptime(
            self.get_date_fin_project(), self.andes_datetime_format
        )
        if not year == date_end.year:
            raise ValueError(
                f"La date fin {date_start.year} est incompatible avec l'année: {year}"
            )

        # start is before end
        if not date_start < date_end:
            raise ValueError(
                f"La date debut {date_start} est apres la date fin {date_end}"
            )

    def init_mission_pk(self, mission_number):
        """
        mission_number (str)
        """
        query = f"SELECT id  FROM shared_models_cruise where mission_number='{mission_number}'"

        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        self.pk = result[0][0]

    def init_input(
        self,
        zone: str = "defaultzone",
        no_releve: int = 0,
        no_notif: str = "IML-2000-001",
        espece: str = "pétoncle",
    ):
        """
        zone (str): 16E, 16F ou 20

        """

        if espece not in ["pétoncle", "buccin", "Mactre de Stimpson"]:
            raise ValueError(
                "espece doit etre un de pétoncle, buccin ou Mactre de Stimpson"
            )
        self.espece = espece

        if zone not in ["16E", "16F", "20"]:
            raise ValueError("zone doit etre un de 16E, 16F ou 20")
        self.zone = zone

        if not no_releve:
            raise ValueError("Le num. relevé doit etre obtenu par un DBA de la DAISS")
        self.no_releve = no_releve

        if not no_notif:
            raise ValueError("Le num. notif (Ex. IML-2000-023) doit etre fourni")
        self.no_notification = no_notif
        self.init_mission_pk(no_notif)

    def populate_data(self):
        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["NO_RELEVE"] = self.no_releve
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["ANNEE"] = self.get_annee()
        self.data["COD_SERIE_HIST"] = self.get_cod_serie_hist()
        self.data["COD_TYP_STRATIF"] = self.get_cod_type_stratif()
        self.data["DATE_DEB_PROJET"] = self.get_date_deb_project()
        self.data["DATE_FIN_PROJET"] = self.get_date_fin_project()
        self.data["NO_NOTIF_IML"] = self.get_no_notif_iml()
        self.data["CHEF_MISSION"] = self.get_chef_mission()
        self.data["SEQ_PECHEUR"] = self.get_seq_pecheur()
        self.data["DUREE_TRAIT_VISEE"] = self.get_duree_trait_visee()
        self.data["DUREE_TRAIT_VISEE_P"] = self.get_duree_trait_visee_p()
        self.data["VIT_TOUAGE_VISEE"] = self.get_vit_touage_visee()
        self.data["VIT_TOUAGE_VISEE_P"] = self.get_vit_touage_visee_p()
        self.data["DIST_CHALUTE_VISEE"] = self.get_dist_chalute_visee()
        self.data["DIST_CHALUTE_VISEE_P"] = self.get_dist_chalute_visee_p()
        self.data["NOM_EQUIPE_NAVIRE"] = self.get_nom_equip_navire()
        self.data["NOM_SCIENCE_NAVIRE"] = self.get_nom_science_navire()
        self.data["REM_PROJ_MOLL"] = self.get_rem_projet_moll()
        self.data["NO_CHARGEMENT"] = self.get_no_chargement()

    @validate_int()
    @log_results
    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0),
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        CONTRAINTE
        La valeur du champ shared_models_cruise.description (FR: Description)
        doit absolument correspondres avec la description présente dans la table SOURCE_INFO:

        18 -> Évaluation de stocks IML - Pétoncle Minganie
        19 -> Évaluation de stocks IML - Pétoncle I de M (Access)
        19 -> Évaluation de stocks IML - Pétoncle Îles-de-la-Madeleine (Oracle)
        """
        query = f"SELECT description FROM shared_models_cruise where id = {self.pk}"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        description = result[0][0]
        # # HACK hard-code for dev, remove to test
        # description = "Évaluation de stocks IML - Pétoncle Îles-de-la-Madeleine"

        key = self.reference_data.get_ref_key(
            table="Source_Info",
            pkey_col="COD_SOURCE_INFO",
            col="DESC_SOURCE_INFO_F",
            val=description,
        )
        to_return = key

        # if area_of_operation == 'I de M' or area_of_operation == 'Iles de la Madeleine':
        #     to_return = 19
        #     self.logger.info("Found 'I de M', using code, %s", to_return)
        # elif area_of_operation == 'Minganie':
        #     to_return = 18
        #     self.logger.info("Found 'Minganie', using code, %s", to_return)
        # else:
        #     raise ValueError("Source Info, Le champ de la mission Andes'Région échantillonnée' n'est pas reconnu: %s", area_of_operation)

        if self.reference_data.validate_exists(
            table="Source_Info", col="COD_SOURCE_INFO", val=to_return
        ):
            return to_return
        else:
            raise ValueError

    @validate_int()
    @log_results
    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        """
        # this has to be supplied as input using self.init_input
        return int(self.no_releve)

    @validate_string(max_len=6)
    @log_results
    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        """
        query = f"SELECT shared_models_vessel.nbpc, \
                                        shared_models_vessel.name \
                                 FROM shared_models_cruise \
                                 LEFT JOIN shared_models_vessel \
                                 ON shared_models_cruise.vessel_id=shared_models_vessel.id \
                                 WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        to_return = result[0][0]

        self.logger.info("Found NBPC: %s,for %s ", to_return, result[0][1])

        # typecast val
        to_return = str(to_return)

        if self.reference_data.validate_exists(
            table="Navire", col="COD_NBPC", val=to_return
        ):
            return to_return
        else:
            raise ValueError

    @validate_int(min_val=1970, max_val=2100)
    @log_results
    def get_annee(self) -> int:
        """ANNEE INTEGER / NUMBER(4,0)
        Année de réalisation du projet

        """
        query = f"SELECT season FROM shared_models_cruise where shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        to_return = result[0][0]

        # typecast val
        to_return = int(to_return)
        return to_return

    @validate_int()
    @log_results
    def get_cod_serie_hist(self) -> int:
        """COD_SERIE_HIST INTEGER /NUMBER(5,0) NOT NULL,
        Identification du type de série auquel les données sont liées tel que défini dans la table INDICE_SUIVI_ABONDANCE

        15 -> Indice d'abondance zone 16E - pétoncle
        16 -> Indice d'abondance zone 16F - pétoncle
        18 -> Indice d'abondance zone 20 - pétoncle

        Valid description should be one of
            Indice d'abondance zone 16E - pétoncle
            Indice d'abondance zone 16F - pétoncle
            Indice d'abondance zone 20 - pétoncle
            Indice d'abondance buccin
            Indice d'abondance Mactre de Stimpson
            Indice d'abondance homard Îles-de-la-Madeleine
        """
        # TODO: use COD_SERIE_HIST as datainput
        if self.espece == "pétoncle":
            desc = f"Indice d'abondance zone {self.zone} - {self.espece}"
        else:
            desc = f"Indice d'abondance {self.espece}"

        key = self.reference_data.get_ref_key(
            table="Indice_Suivi_Etat_Stock",
            pkey_col="COD_SERIE_HIST",
            col="DESC_SERIE_HIST_F",
            val=desc,
        )
        to_return = key
        to_return = int(to_return)

        if self.reference_data.validate_exists(
            table="Indice_Suivi_Etat_Stock", col="COD_SERIE_HIST", val=to_return
        ):
            return to_return
        else:
            raise ValueError

    @validate_int()
    @log_results
    def get_cod_type_stratif(self) -> int:
        """COD_TYP_STRATIF INTEGER / NUMBER(5,0)NOT NULL,

        Identification du type de stratification utilisé durant l'activité tel que défini dans la table TYPE_STRATIFICATION

        7 -> Station fixe
        8 -> Échantillonnage aléatoire simple

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988

        """
        # res = self.cur.execute(f"SELECT shared_models_stratificationtype.code FROM shared_models_stratificationtype ;")

        query = f"SELECT \
                    shared_models_stratificationtype.code, \
                    shared_models_stratificationtype.description_fra \
                FROM shared_models_cruise \
                LEFT JOIN shared_models_stratificationtype ON shared_models_cruise.stratification_type_id=shared_models_stratificationtype.id \
                WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        self.logger.info("%s est %s", result[0][0], result[0][1])
        to_return = result[0][0]
        if self.reference_data.validate_exists(
            table="Type_Stratification", col="COD_TYP_STRATIF", val=to_return
        ):
            return to_return
        else:
            raise ValueError

    @log_results
    def get_date_deb_project(self) -> str | None:
        """DATE_DEB_PROJET TIMESTAMP / DATE
        Date de début du projet format AAAA-MM-JJ

        TODO: verify datetime format
        """

        query = f"SELECT start_date FROM shared_models_cruise where id = {self.pk}"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        to_return = result[0][0]
        dt = datetime.datetime.strptime(str(to_return), self.andes_datetime_format)
        to_return = datetime.datetime.strftime(dt, self.reference_data.datetime_strfmt)
        return to_return

    @log_results
    def get_date_fin_project(self) -> str | None:
        """DATE_FIN_PROJET TIMESTAMP / DATE
        Date de début du projet format AAAA-MM-JJ

        TODO: verify datetime format
        """

        query = f"SELECT end_date FROM shared_models_cruise where id = {self.pk}"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)

        to_return = result[0][0]
        dt = datetime.datetime.strptime(str(to_return), self.andes_datetime_format)
        to_return = datetime.datetime.strftime(dt, self.reference_data.datetime_strfmt)

        return to_return

    @validate_string(max_len=12)
    @log_results
    def get_no_notif_iml(self) -> str | None:
        """NO_NOTIF_IML VARCHAR(12) / VARCHAR2(12)
        Numéro de notification de recherche émis par l'Institut Maurice-Lamontagne

        """
        query = f"SELECT mission_number FROM shared_models_cruise where id = {self.pk}"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @validate_string(max_len=50, not_null=False)
    @log_results
    def get_chef_mission(self) -> str | None:
        """CHEF_MISSION VARCHAR(50) / VARCHAR2(50)
        Nom du chef de mission

        """
        query = f"SELECT chief_scientist FROM shared_models_cruise where id = {self.pk}"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @validate_int(not_null=False)
    @log_results
    def get_seq_pecheur(self) -> int | None:
        """SEQ_PECHEUR INTEGER / NUMBER(10,0)
        Numéro unique pour l'identification du pêcheur tel que défini dans la table PECHEUR

        Champ de type SEQ

        """
        # query = f"SELECT shared_models_set.bridge \
        #           FROM shared_models_set \
        #           WHERE shared_models_set.cruise_id={self.pk};"
        # res = self.cur.execute(query)

        # result = res.fetchall()
        # make sure bridge is the same for all sets,
        # this is a bit silly since there can be crew changes, but it's how the ProjetMollusque table is designed
        # thus to satisfy this, use the generic "Capitain Leim" bridge name.
        # It's a good idea to set it as the default value for set.bridge in Andes.
        # self._assert_all_equal(result)

        # if result[0][2] == "Leim":
        #     # seq_pecher for "capitain Leim"
        #     to_return = 151
        to_return = self._seq_result()
        return to_return

    @log_results
    def get_duree_trait_visee(self) -> float | None:
        """DUREE_TRAIT_VISEE DOUBLE / NUMBER
        Durée anticipée pour la réalisation d'un trait, unité minute

        descript: targeted set duration
        units: minutes

        """

        query = f"SELECT shared_models_cruise.targeted_trawl_duration \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]

        to_return = float(to_return)
        return to_return

    @log_results
    def get_duree_trait_visee_p(self) -> float | None:
        """DUREE_TRAIT_VISEE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Durée_Trait_Visee"

        units: minutes
        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty
        """
        # hard-code this
        to_return = self._hard_coded_result(0.1)
        to_return = float(to_return)
        return to_return

    @log_results
    def get_vit_touage_visee(self) -> float | None:
        """VIT_TOUAGE_VISEE DOUBLE / NUMBER
        Vitesse anticipée du navire pour la réalisation d'un trait, unité noeud (mille marin)

        units: knots

        """

        query = f"SELECT shared_models_cruise.targeted_trawl_speed \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        to_return = float(to_return)
        return to_return

    @log_results
    def get_vit_touage_visee_p(self) -> float | None:
        """VIT_TOUAGE_VISEE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Vit_Touage_Visee"

        units: knots
        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        """
        # hard-code this
        to_return = self._hard_coded_result(0.1)
        to_return = float(to_return)
        return to_return

    @log_results
    def get_dist_chalute_visee(self) -> float | None:
        """DIST_CHALUTE_VISEE DOUBLE / NUMBER
        Distance anticipée parcourue par l'engin de pêche, unité mètre

        units: meters

        N.B Andes does not permit setting this value, but will rather calculate it.

        """

        # speed_kph = self.convert_knots_to_kph(self.get_vit_touage_visee())
        # time_h = self.get_duree_trait_visee() / 60.0
        # dist_m = speed_kph * time_h * 1000

        query = f"SELECT shared_models_cruise.targeted_trawl_duration \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        to_return = float(to_return)
        return to_return

    @log_results
    def get_dist_chalute_visee_p(self) -> float | None:
        """DIST_CHALUTE_VISEE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Dsitance_Chalute_Visee"

        units: meters
        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        """
        # hard-code this
        to_return = self._hard_coded_result(1.0)
        to_return = float(to_return)
        return to_return

    @log_results
    def get_rapport_fune_visee(self) -> float | None:
        """RAPPORT_FUNE_VISEE DOUBLE / NUMBER
        Rapport de longueur de fune sur profondeur visée

        """

        # hard-code this
        to_return = self._hard_coded_result(2.0)
        to_return = float(to_return)
        return to_return

    @log_results
    def get_rapport_fune_visee_p(self) -> float | None:
        """RAPPORT_FUNE_VISEE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Rappport_Fune_Visee"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        """
        # hard-code this
        to_return = self._hard_coded_result(0.1)
        to_return = float(to_return)
        return to_return

    @validate_string(max_len=250, not_null=False)
    @log_results
    def get_nom_equip_navire(self) -> str | None:
        """NOM_EQUIPE_NAVIRE VARCHAR(250) / VARCHAR2(250)
        Noms des membres d'équipage

        """
        raise NotImplementedError

    @validate_string(max_len=250, not_null=False)
    @log_results
    def get_nom_science_navire(self) -> str | None:
        """NOM_SCIENCE_NAVIRE VARCHAR(250) / VARCHAR2(250)
        Noms des membres de ''équipe scientifique

        """
        query = f"SELECT shared_models_cruise.samplers \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]

        to_return = str(to_return)
        return to_return

    @validate_string(max_len=255, not_null=False)
    @log_results
    def get_rem_projet_moll(self) -> str | None:
        """REM_PROJET_MOLL VARCHAR(255) / VARCHAR2(500)
        Remarque sur le projet

        N.B. max length mistmatch between Orale and MSAccess

        """
        query = f"SELECT shared_models_cruise.notes \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]

        to_return = str(to_return)
        return to_return

    @validate_int(not_null=False)
    @log_results
    def get_no_chargement(self) -> int | None:
        """NO_CHARGEMENT INTEGER / NUMBER
        Numéro de l'activité de chargement de données dans la base Oracle

        N.B. Datatype mistmatch between Oracle and MSAccess

        """

        raise NotImplementedError


if __name__ == "__main__":
    andes_db = AndesHelper()

    proj = ProjetMollusque(andes_db)
    proj.init_mission_pk("IML-2023-011")
    proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")

    proj.get_cod_source_info()
    proj.get_cod_nbpc()
    proj.get_annee()
    proj.get_cod_serie_hist()
    proj.get_cod_type_stratif()
    proj.get_date_deb_project()
    proj.get_date_fin_project()
    proj.get_no_notif_iml()
    proj.get_chef_mission()
    # proj.get_seq_pecheur()
    proj.get_duree_trait_visee()
    proj.get_duree_trait_visee_p()
    proj.get_vit_touage_visee()
    proj.get_vit_touage_visee_p()
    proj.get_dist_chalute_visee()
    proj.get_dist_chalute_visee_p()
    # proj.get_nom_equip_navire()
    proj.get_nom_science_navire()
    proj.get_rem_projet_moll()
    # proj.get_no_chargement()
    # proj.validate()
