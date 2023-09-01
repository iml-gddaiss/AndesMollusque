
import logging
import sqlite3
import datetime
import pandas as pd
import numpy as np

from oracle_helper import OracleHelper

def log_results(f):
    def wrapper(*args):
        res = f(*args)
        args[0].logger.info("%s -> %s", f.__name__, res)
        # print(f"{f.__name__} -> {res}")
        return res
    return wrapper

logging.basicConfig(level=logging.INFO)

class ProjetMollusque:
# CREATE TABLE PROJET_MOLLUSQUE (

    def __init__(self, con):
        self.logger = logging.getLogger(__name__)

        self.con = con
        self.cur = self.con.cursor()
        self.pk = None

        self.reference_data = OracleHelper(access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb")

        self._datetime_format = '%Y-%m-%d %H:%M:%S'

    def _assert_one(self, result):
        if (len(result)==0):
            raise ValueError("recieved no result.")

        elif (len(result)>1):
            raise ValueError("recieved more than one result.")

        elif (not len(result)==1):
            raise ValueError("Expected only one result.")

    def _assert_all_equal(self, result):
        print("checking")
        if (len(result)==0):
            raise ValueError("recieved no result.")

        # case single
        # elif (len(result)==1):
        #     return
        df = pd.DataFrame(result)
        print(df)
        # (df[0] ==df)
        

    def validate(self):

        # use zones are compatible with code_source_info
        cod_source_info = self.get_cod_source_info()
        if self.zone in ['16E', '16F'] and not cod_source_info==18:
            raise ValueError(f"La zones {self.zone} est incompatible avec le COD_SOURCE_INFO: {cod_source_info}")
        if self.zone in ['20'] and not cod_source_info==19:
            raise ValueError(f"La zone {self.zone} est incompatible avec le COD_SOURCE_INFO: {cod_source_info}")
        
        # year compatible with start and end dates
        year = self.get_annee()
        # year start 
        date_start = datetime.datetime.strptime(self.get_date_deb_project(), self._datetime_format)
        if not year==date_start.year:
            raise ValueError(f"La date debut {date_start.year} est incompatible avec l'année: {year}")
        # year end
        date_end = datetime.datetime.strptime(self.get_date_fin_project(), self._datetime_format)
        if not year==date_end.year:
            raise ValueError(f"La date fin {date_start.year} est incompatible avec l'année: {year}")

        # start is before end
        if not date_start < date_end:
            raise ValueError(f"La date debut {date_start} est apres la date fin {date_end}")


    def init_mission_pk(self, mission_number) -> int:
        '''
        mission_number (str)
        '''
        query = f"SELECT id  FROM shared_models_cruise where mission_number='{mission_number}'"
        res = self.cur.execute(query)
        result = res.fetchall()
        self._assert_one(result)

        self.pk = result[0][0]

    def init_input(self, zone:str=None, no_releve:int=None, no_notif:str=None):
        '''
        zone (str): 16E, 16F ou 20

        '''

        if (not zone in ['16E', '16F', '20']) :
            raise ValueError('zone doit etre un de 16E, 16F ou 20')
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
        '''
        COD_SOURCE_INFO INTEGER NOT NULL,

        CONSTRAINT SYS_PK_13524 PRIMARY KEY (COD_SOURCE_INFO,NO_RELEVE,COD_NBPC)
        These must match the andes cruise description
        18 -> Évaluation de stocks IML - Pétoncle Minganie
        19 -> Évaluation de stocks IML - Pétoncle I de M
        '''
        query = f"SELECT description FROM shared_models_cruise where id = {self.pk}"
        res = self.cur.execute(query)
        result=res.fetchall()
        self._assert_one(result)

        description = result[0][0]
        # HACK hard-code for dev, remove to test
        description = "Évaluation de stocks IML - Pétoncle I de M"

        key = self.reference_data.get_ref_key('Source_Info', 'COD_SOURCE_INFO', 'DESC_SOURCE_INFO_F', description)
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

        if self.reference_data.validate_exist('Source_Info', 'COD_SOURCE_INFO', 'DESC_SOURCE_INFO_F', description)
            return to_return
        else:
            raise ValueError
        
    @log_results    
    def get_no_releve(self) -> int:
        '''
        NO_RELEVE INTEGER NOT NULL,

        CONSTRAINT SYS_PK_13524 PRIMARY KEY (COD_SOURCE_INFO,NO_RELEVE,COD_NBPC)
        '''
        # this has to be supplied as input using self.init_input
        return int(self.no_releve)

    @log_results    
    def get_cod_nbpc(self, validate=True) -> str:
        '''
        COD_NBPC VARCHAR(6) NOT NULL,

        CONSTRAINT SYS_PK_13524 PRIMARY KEY (COD_SOURCE_INFO,NO_RELEVE,COD_NBPC)

        '''
        query = f"SELECT shared_models_vessel.nbpc, \
                                        shared_models_vessel.name \
                                 FROM shared_models_cruise \
                                 LEFT JOIN shared_models_vessel \
                                 ON shared_models_cruise.vessel_id=shared_models_vessel.id \
                                 WHERE shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)
        result=res.fetchall()
        self._assert_one(result)

        to_return=result[0][0]
        self.logger.info(f"Found NBPC: %s,for %s ", to_return, result[0][1])

        # typecast val
        to_return = str(to_return)

        if self.reference_data.validate_exist('Source_Info', 'COD_SOURCE_INFO', 'DESC_SOURCE_INFO_F', description):
            return to_return

        return to_return

    @log_results    
    def get_annee(self) ->int:
        '''
        ANNEE INTEGER,

        '''
        query = f"SELECT season FROM shared_models_cruise where shared_models_cruise.id={self.pk};"
        res = self.cur.execute(query)
        result=res.fetchall()
        self._assert_one(result)

        to_return = result[0][0]

        # typecast val
        to_return = int(to_return)
        return to_return

    @log_results    
    def get_cod_serie_hist(self) ->int:
        '''
        COD_SERIE_HIST INTEGER NOT NULL,

        15 -> Indice d'abondance zone 16E - pétoncle
        16 -> Indice d'abondance zone 16F - pétoncle
        18 -> Indice d'abondance zone 20 - pétoncle

        '''
        
        
        if self.zone=='16E':
            to_return = 15
        elif self.zone=='16F':
            to_return = 16
        elif self.zone =='20':
            to_return = 18
        else:
            raise ValueError("Impossible de determine le COD_SERIE_HIST, vérifier la zone")
        return int(to_return)


    @log_results
    def get_cod_type_stratif(self) ->int:
        '''
        COD_TYP_STRATIF INTEGER NOT NULL,

        7 -> Station fixe
        8 -> Échantillonnage aléatoire simple

        '''
        # res = self.cur.execute(f"SELECT shared_models_stratificationtype.code FROM shared_models_stratificationtype ;")

        res = self.cur.execute(f"SELECT \
                                    shared_models_stratificationtype.code, \
                                    shared_models_stratificationtype.description_fra \
                                FROM shared_models_cruise \
                                LEFT JOIN shared_models_stratificationtype ON shared_models_cruise.stratification_type_id=shared_models_stratificationtype.id \
                                WHERE shared_models_cruise.id={self.pk};")
        result=res.fetchall()
        self._assert_one(result)

        self.logger.info("%s est %s", result[0][0], result[0][1])
        to_return = result[0][0]
        return to_return


    @log_results
    def get_date_deb_project(self):
        '''
        DATE_DEB_PROJET TIMESTAMP,
        '''

        res = self.cur.execute(f"SELECT start_date FROM shared_models_cruise where id = {self.pk}")
        result=res.fetchall()
        self._assert_one(result)

        to_return = result[0][0]
        return to_return

    @log_results
    def get_date_fin_project(self):
        '''
        DATE_FIN_PROJET TIMESTAMP,
        '''

        res = self.cur.execute(f"SELECT end_date FROM shared_models_cruise where id = {self.pk}")
        result=res.fetchall()
        self._assert_one(result)

        to_return = result[0][0]
        return to_return

    @log_results
    def get_no_notif_iml(self):
        '''
        NO_NOTIF_IML VARCHAR(12),

        '''
        res = self.cur.execute(f"SELECT mission_number FROM shared_models_cruise where id = {self.pk}")
        result=res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @log_results
    def get_chef_mission(self):
        '''
        CHEF_MISSION VARCHAR(50),

        '''
        res = self.cur.execute(f"SELECT chief_scientist FROM shared_models_cruise where id = {self.pk}")
        result=res.fetchall()
        self._assert_one(result)
        to_return = result[0][0]
        return to_return


    @log_results
    def get_seq_pecheur(self):
        '''
        SEQ_PECHEUR INTEGER,

        '''
        query = f"SELECT shared_models_set.bridge \
                  FROM shared_models_set \
                  WHERE shared_models_set.cruise_id={self.pk};"
        res = self.cur.execute(query)

        result=res.fetchall()
        # make sure bridge is the same for all sets,
        # this is a bit silly since there can be crew changes, but it's how the ProjetMollusque table is designed
        # thus to satisfy this, use the generic "Capitain Leim" bridge name.
        # It's a good idea to set it as the default value for set.bridge in Andes.
        self._assert_all_equal(result)
        result=res.fetchall()
        

        # if result[0][2] == "Leim":
        #     # seq_pecher for "capitain Leim"
        #     to_return = 151
        to_return = 151
        return to_return


    # DUREE_TRAIT_VISEE DOUBLE,
    # DUREE_TRAIT_VISEE_P DOUBLE,
    # VIT_TOUAGE_VISEE DOUBLE,
    # VIT_TOUAGE_VISEE_P DOUBLE,
    # DIST_CHALUTE_VISEE DOUBLE,
    # DIST_CHALUTE_VISEE_P DOUBLE,
    # RAPPORT_FUNE_VISEE DOUBLE,
    # RAPPORT_FUNE_VISEE_P DOUBLE,
    # NOM_EQUIPE_NAVIRE VARCHAR(250),
    # NOM_SCIENCE_NAVIRE VARCHAR(250),
    # REM_PROJET_MOLL VARCHAR(255),
    # NO_CHARGEMENT INTEGER,


if __name__ == "__main__":
    con = sqlite3.connect("db.sqlite3")

    proj = ProjetMollusque(con)
    proj.init_mission_pk("IML-2023-011")
    proj.init_input(zone='20',
                    no_releve=34,
                    no_notif='IML-2023-011'
                    )
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
    # proj.validate()
    


