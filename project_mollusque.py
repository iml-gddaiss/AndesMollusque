import logging
import sqlite3
import datetime
from time import strftime
import pandas as pd

from oracle_helper import OracleHelper
from peche_sentinelle import PecheSentinelle

def log_results(f):
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        args[0].logger.info("%s -> %s", f.__name__, res)
        return res
    return wrapper

def validate_string_len(max_len=0):
    def decorator(f):
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            if max_len and len(res) <= max_len:
                args[0].logger.info("string variable is within the allowed length of VARCHAR(%s) ",  max_len)
            else:
                args[0].logger.error("string variable is NOT within the allowed length of VARCHAR(%s) ",  max_len)
                raise ValueError
            return res
        return wrapper
    return decorator


logging.basicConfig(level=logging.INFO)


class ProjetMollusque(PecheSentinelle):
    # CREATE TABLE PROJET_MOLLUSQUE (

    def __init__(self, con):
        self.logger = logging.getLogger(__name__)

        self.con = con
        self.cur = self.con.cursor()
        self.pk = None
        self.espece = None

        self.reference_data = OracleHelper(
            access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb"
        )
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
        res = self.cur.execute(query)
        result = res.fetchall()
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

    @log_results
    def get_cod_source_info(self) -> int:
        """
        COD_SOURCE_INFO INTEGER NOT NULL,

        CONSTRAINT SYS_PK_13524 PRIMARY KEY (COD_SOURCE_INFO,NO_RELEVE,COD_NBPC)
        These must match the andes cruise description
        18 -> Évaluation de stocks IML - Pétoncle Minganie
        19 -> Évaluation de stocks IML - Pétoncle I de M
        """
        query = f"SELECT description FROM shared_models_cruise where id = {self.pk}"
        res = self.cur.execute(query)
        result = res.fetchall()
        self._assert_one(result)

        description = result[0][0]
        # HACK hard-code for dev, remove to test
        description = "Évaluation de stocks IML - Pétoncle I de M"

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

        # typecast val
        to_return = int(to_return)
        if self.reference_data.validate_exists(
            table="Source_Info", col="COD_SOURCE_INFO", val=to_return
        ):
            return to_return
        else:
            raise ValueError

    @log_results
    def get_no_releve(self) -> int:
        """
        NO_RELEVE INTEGER NOT NULL,

        CONSTRAINT SYS_PK_13524 PRIMARY KEY (COD_SOURCE_INFO,NO_RELEVE,COD_NBPC)
        """
        # this has to be supplied as input using self.init_input
        return int(self.no_releve)

    @validate_string_len(max_len=6)
    @log_results
    def get_cod_nbpc(self) -> str:
        """
        COD_NBPC VARCHAR(6) NOT NULL,

        CONSTRAINT SYS_PK_13524 PRIMARY KEY (COD_SOURCE_INFO,NO_RELEVE,COD_NBPC)

        """
        query = f"SELECT shared_models_vessel.nbpc, \
                                        shared_models_vessel.name \
                                 FROM shared_models_cruise \
                                 LEFT JOIN shared_models_vessel \
                                 ON shared_models_cruise.vessel_id=shared_models_vessel.id \
                                 WHERE shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)
        result = res.fetchall()
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

    @log_results
    def get_annee(self) -> int:
        """
        ANNEE INTEGER,

        """
        query = f"SELECT season FROM shared_models_cruise where shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)
        result = res.fetchall()
        self._assert_one(result)

        to_return = result[0][0]

        # typecast val
        to_return = int(to_return)
        return to_return

    @log_results
    def get_cod_serie_hist(self) -> int:
        """
        COD_SERIE_HIST INTEGER NOT NULL,

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

    @log_results
    def get_cod_type_stratif(self) -> int:
        """
        COD_TYP_STRATIF INTEGER NOT NULL,

        7 -> Station fixe
        8 -> Échantillonnage aléatoire simple

        """
        # res = self.cur.execute(f"SELECT shared_models_stratificationtype.code FROM shared_models_stratificationtype ;")

        res = self.cur.execute(
            f"SELECT \
                                    shared_models_stratificationtype.code, \
                                    shared_models_stratificationtype.description_fra \
                                FROM shared_models_cruise \
                                LEFT JOIN shared_models_stratificationtype ON shared_models_cruise.stratification_type_id=shared_models_stratificationtype.id \
                                WHERE shared_models_cruise.id={self.pk};"
        )
        result = res.fetchall()
        self._assert_one(result)

        self.logger.info("%s est %s", result[0][0], result[0][1])
        to_return = result[0][0]
        to_return = int(to_return)
        if self.reference_data.validate_exists(
            table="Type_Stratification", col="COD_TYP_STRATIF", val=to_return
        ):
            return to_return
        else:
            raise ValueError

    @log_results
    def get_date_deb_project(self) -> str:
        """
        DATE_DEB_PROJET TIMESTAMP,
        """

        res = self.cur.execute(
            f"SELECT start_date FROM shared_models_cruise where id = {self.pk}"
        )
        result = res.fetchall()
        self._assert_one(result)

        to_return = result[0][0]
        dt = datetime.datetime.strptime(to_return, self.andes_datetime_format)
        to_return = datetime.datetime.strftime(dt, self.reference_data.datetime_strfmt)
        return to_return

    @log_results
    def get_date_fin_project(self):
        """
        DATE_FIN_PROJET TIMESTAMP,
        """

        res = self.cur.execute(
            f"SELECT end_date FROM shared_models_cruise where id = {self.pk}"
        )
        result = res.fetchall()
        self._assert_one(result)

        to_return = result[0][0]
        dt = datetime.datetime.strptime(to_return, self.andes_datetime_format)
        to_return = datetime.datetime.strftime(dt, self.reference_data.datetime_strfmt)
        return to_return

    @validate_string_len(max_len=12)
    @log_results
    def get_no_notif_iml(self) -> str:
        """
        NO_NOTIF_IML VARCHAR(12),

        """
        res = self.cur.execute(
            f"SELECT mission_number FROM shared_models_cruise where id = {self.pk}"
        )
        result = res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @validate_string_len(max_len=50)
    @log_results
    def get_chef_mission(self) -> str:
        """
        CHEF_MISSION VARCHAR(50),

        """
        res = self.cur.execute(
            f"SELECT chief_scientist FROM shared_models_cruise where id = {self.pk}"
        )
        result = res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @log_results
    def get_seq_pecheur(self) -> int:
        """
        SEQ_PECHEUR INTEGER,

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
    def get_duree_trait_visee(self) -> float:
        """
        DUREE_TRAIT_VISEE DOUBLE,

        descript: targeted set duration
        units: minutes

        """

        query = f"SELECT shared_models_cruise.targeted_trawl_duration \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)

        result = res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]

        to_return = float(to_return)
        return to_return


    @log_results
    def get_duree_trait_visee_p(self) -> float:
        """
        DUREE_TRAIT_VISEE_P DOUBLE,

        description: precision on DUREE_TRAIT_VISEE
        units: minutes
        """
        # hard-code this
        to_return = self._hard_coded_result(0.1)
        to_return = float(to_return)
        return to_return


    @log_results
    def get_vit_touage_visee(self) -> float:
        """
        VIT_TOUAGE_VISEE DOUBLE,

        description: target trawl speed
        units: knots

        """

        query = f"SELECT shared_models_cruise.targeted_trawl_speed \
                  FROM shared_models_cruise \
                  WHERE shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)

        result = res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]
        to_return = float(to_return)
        return to_return


    @log_results
    def get_vit_touage_visee_p(self) -> float:
        """
        VIT_TOUAGE_VISEE_P DOUBLE
        
        description: precision on VIT_TOUAGE_VISEE
        units: knots

        """
        # hard-code this
        to_return = self._hard_coded_result(0.1)
        to_return = float(to_return)    
        return to_return


    @log_results
    def get_dist_chalute_visee(self) -> float:
        """
        DIST_CHALUTE_VISEE DOUBLE

        description: trawl distance 
        units: meters
        
        Andes does not save this in the DB, as it depends on the value for speed and time.
        """

        speed_kph = self.convert_knots_to_kph(self.get_vit_touage_visee())
        time_h = self.get_duree_trait_visee()/60.
        dist_m = speed_kph*time_h*1000

        # round to given precision
        precision = self.get_dist_chalute_visee_p()
        # TO-DO only implemented for one precision value
        # add implementation as needed.
        if (precision == 1.0):
            dist_m = round(dist_m, 0)
        else:
            raise NotImplementedError("Please implement rounding rule")
        to_return = float(dist_m)
        return to_return

    @log_results
    def get_dist_chalute_visee_p(self) -> float:
        """
        DIST_CHALUTE_VISEE_P DOUBLE,

        """
        # hard-code this
        to_return = self._hard_coded_result(1.0)
        to_return = float(to_return) 
        return to_return


    @log_results
    def get_rapport_fune_visee(self) -> float:
        """
        RAPPORT_FUNE_VISEE DOUBLE,
        """

        # hard-code this
        to_return = self._hard_coded_result(2.0)
        to_return = float(to_return) 
        return to_return


    @log_results
    def get_rapport_fune_visee_p(self) -> float:
        """
        RAPPORT_FUNE_VISEE_P DOUBLE

        """
        # hard-code this
        to_return = self._hard_coded_result(0.1)
        to_return = float(to_return) 
        return to_return


    @validate_string_len(max_len=250)
    @log_results
    def get_non_equip_navire(self) -> str:
        """
        NOM_EQUIPE_NAVIRE VARCHAR(250)

        """
        res = self.cur.execute(
            f"SELECT mission_number FROM shared_models_cruise where id = {self.pk}"
        )
        result = res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    # NOM_SCIENCE_NAVIRE VARCHAR(250),
    # REM_PROJET_MOLL VARCHAR(255),
    # NO_CHARGEMENT INTEGER,


if __name__ == "__main__":
    con = sqlite3.connect("db.sqlite3")

    proj = ProjetMollusque(con)
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
    proj.get_seq_pecheur()
    proj.get_duree_trait_visee()
    proj.get_duree_trait_visee_p()
    proj.get_vit_touage_visee()
    proj.get_vit_touage_visee_p()
    proj.get_dist_chalute_visee()
    proj.get_dist_chalute_visee_p()

    # proj.validate()
