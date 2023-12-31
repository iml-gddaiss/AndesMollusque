import logging
import datetime
from unidecode import unidecode
from andes_migrate.oracle_helper import OracleHelper

from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.table_peche_sentinelle import TablePecheSentinelle
from andes_migrate.andes_helper import AndesHelper
from andes_migrate.decorators import (
    AndesCodeLookup,
    Deprecated,
    HardCoded,
    tag,
    deprecate,
    log_results,
    validate_string,
    validate_int,
)

# logging.basicConfig(level=logging.INFO)


class TraitMollusque(TablePecheSentinelle):
    """
    Object model representing the TRAIT_MOLLUSQUE table
    """

    def __init__(self, andes_db: AndesHelper, proj: ProjetMollusque, *args, **kwargs):
        # super().__init__(*args, **kwargs)
        super().__init__(*args, ref=proj.reference_data, **kwargs)

        self.andes_db = andes_db
        self.proj: ProjetMollusque = proj
        self.table_name = "TRAIT_MOLLUSQUE"

        self._init_rows()

    def _init_rows(self):
        """Initialisation method
        This queries the Andes DB and creates a list of row entries to be added to the current table

        After running this methods initialises the following attribute:
        self._row_list
        self._row_idx (hopefully to self._row_idx=0)

        self._row_list will be populated with the associated Andes set ids for the current mission
        self._row_idx will start at 0

        """
        query = (
            "SELECT shared_models_set.id "
            "FROM shared_models_set "
            f"WHERE shared_models_set.cruise_id={self.proj._get_current_row_pk()} "
            "ORDER BY shared_models_set.id ASC "
        )

        result = self.andes_db.execute_query(query)
        self._assert_not_empty(result)

        # a list of all the Set pk's (need to unpack a bit)
        self._row_list = [set[0] for set in result]
        self._row_idx = 0

    def populate_data(self):
        """Populate data: run all getters"""

        self.data["COD_SOURCE_INFO"] = self.get_cod_source_info()
        self.data["NO_RELEVE"] = self.get_no_releve()
        self.data["COD_NBPC"] = self.get_cod_nbpc()
        self.data["IDENT_NO_TRAIT"] = self.get_ident_no_trait()
        self.data["COD_ZONE_GEST_MOLL"] = self.get_cod_zone_gest_moll()
        self.data["COD_SECTEUR_RELEVE"] = self.get_cod_secteur_releve()
        self.data["COD_STRATE"] = self.get_cod_strate()
        self.data["NO_STATION"] = self.get_no_station()
        self.data["COD_TYP_TRAIT"] = self.get_cod_typ_trait()
        self.data["COD_RESULT_OPER"] = self.get_cod_result_oper()
        self.data["DATE_DEB_TRAIT"] = self.get_date_deb_trait()
        self.data["DATE_FIN_TRAIT"] = self.get_date_fin_trait()
        self.data["HRE_DEB_TRAIT"] = self.get_hre_deb_trait()
        self.data["HRE_FIN_TRAIT"] = self.get_hre_fin_trait()
        self.data["COD_TYP_HEURE"] = self.get_cod_typ_heure()
        self.data["COD_FUSEAU_HORAIRE"] = self.get_cod_fuseau_horaire()
        self.data["LAT_DEB_TRAIT"] = self.get_lat_deb_trait()
        self.data["LAT_FIN_TRAIT"] = self.get_lat_fin_trait()
        self.data["LONG_DEB_TRAIT"] = self.get_long_deb_trait()
        self.data["LONG_FIN_TRAIT"] = self.get_long_fin_trait()
        self.data["LATLONG_P"] = self.get_latlong_p()
        self.data["DISTANCE_POS"] = self.get_distance_pos()
        self.data["DISTANCE_POS_P"] = self.get_distance_pos_p()
        self.data["VIT_TOUAGE"] = self.get_vit_touage()
        self.data["VIT_TOUAGE_P"] = self.get_vit_touage_p()
        self.data["DUREE_TRAIT"] = self.get_duree_trait()
        self.data["DUREE_TRAIT_P"] = self.get_duree_trait_p()
        self.data["TEMP_FOND"] = self.get_temp_fond()
        self.data["TEMP_FOND_P"] = self.get_temp_fond_p()
        self.data["PROF_DEB"] = self.get_prof_deb()
        self.data["PROF_DEB_P"] = self.get_prof_deb_p()
        self.data["PROF_FIN"] = self.get_prof_fin()
        self.data["PROF_FIN_P"] = self.get_prof_fin_p()
        self.data["REM_TRAIT_MOLL"] = self.get_rem_trait_moll()
        self.data["NO_CHARGEMENT"] = self.get_no_chargement()
        # self.data["DATE_HRE_DEB_TRAIT"] = self.get_date_heure_deb_trait()
        # self.data["DATE_HRE_FIN_TRAIT"] = self.get_date_heure_fin_trait()
        # self.data["SALINITE_FOND"] = self.get_salinite_fond()
        # self.data["SALINITE_FOND_P"] = self.get_salinite_fond_p()
        # self.data["COD_TYP_ECH_TRAIT"] = self.get_cod_typ_ech_trait()

    @validate_int()
    def get_cod_source_info(self) -> int:
        """COD_SOURCE_INFO INTEGER / NUMBER(5,0)
        Identification de la source d'information tel que défini dans la table SOURCE_INFO

        Extrait du projet ::func:`~andes_migrate.projet_mollusque.ProjetMollusque.get_cod_source_info`
        """

        return self.proj.get_cod_source_info()

    @validate_int()
    def get_no_releve(self) -> int:
        """NO_RELEVE INTEGER / NUMBER(5,0)
        Numéro séquentiel du relevé

        Extrait du projet ::func:`~andes_migrate.projet_mollusque.ProjetMollusque.get_no_releve`
        """
        return self.proj.get_no_releve()

    @log_results
    def get_cod_nbpc(self) -> str:
        """COD_NBPC VARCHAR(6) / VARCHAR2(6)
        Numéro du navire utilisé pour réaliser le relevé tel que défini dans la table NAVIRE

        Extrait du projet ::func:`~andes_migrate.projet_mollusque.ProjetMollusque.get_cod_nbpc`
        """
        return self.proj.get_cod_nbpc()

    @validate_int()
    @log_results
    def get_ident_no_trait(self) -> int:
        """IDENT_NO_TRAIT INTEGER / NUMBER(5,0)
        Numéro séquentiel d'identification du trait

        Andes
        -----
        shared_models.set.set_number
        """
        set_pk = self._get_current_row_pk()
        query = (
            "SELECT shared_models_set.set_number "
            "FROM shared_models_set "
            f"WHERE shared_models_set.id={set_pk} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @validate_int(not_null=False)
    @log_results
    def get_cod_zone_gest_moll(self) -> int | None:
        """COD_ZONE_GEST_MOLL  COD_TYP_TRAIT INTEGER / NUMBER(5,0)
        Identification de la zone de gestion de la pêche aux mollusques tel que défini dans la table ZONE_GEST_MOLL

        Pas présente dans Andes, doit être initialisé via le parametre `zone` du projet.
        """
        zone = self.proj.zone

        key = self.reference_data.get_ref_key(
            table="ZONE_GEST_MOLL",
            pkey_col="COD_ZONE_GEST_MOLL",
            col="ZONE_GEST_MOLL",
            val=zone,
        )
        return key

    @validate_int(not_null=False)
    @log_results
    def get_cod_secteur_releve(self) -> int | None:
        """COD_SECTEUR_RELEVE INTEGER/NUMBER(5,0)
        Identification de la zone géographique de déroulement du relevé tel que défini dans la table SECTEUR_RELEVE_MOLL

        CONTRAINTE

        La premiere lettre du champ shared_models_cruise.area_of_operation (FR: Région échantillonée)
        doit absolument correspondres avec la valeur SECTEUR_RELEVE de la table SECTEUR_RELEVE_MOLL:

        1 -> C (Côte-Nord)
        4 -> I (Îles de la Madeleine)

        Bien q'il suffit de mettre seulement la premiere lettre, il est conseiller de mettre
        le nom en entier pour la lisibilité.

        """

        query = (
            "SELECT shared_models_cruise.area_of_operation "
            "FROM shared_models_cruise "
            f"WHERE shared_models_cruise.id={self.proj._get_current_row_pk()} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        secteur: str = result[0][0]

        # first char stripped of accents and cast to uppercase
        secteur = unidecode(secteur[0].upper())

        key = self.reference_data.get_ref_key(
            table="SECTEUR_RELEVE_MOLL",
            pkey_col="COD_SECTEUR_RELEVE",
            col="SECTEUR_RELEVE",
            val=secteur,
        )
        return key

    @tag(HardCoded)
    @validate_int(not_null=False)
    @log_results
    def get_cod_strate(self) -> int | None:
        """COD_STRATE INTEGER / NUMBER(5,0)
        Identification de la strate où se réalise le trait tel que défini dans la table TYPE_STRATE_MOLL

        For the moment, this function returns None
        More discussions are needed to decide how to determine the stratum
        perhaps station.strate
        """
        # a faster way is to get shared_models_set.stratum directly ?
        # but that would be too easy, instead  do the following...

        query = (
            "SELECT shared_models_newstation.name "
            "FROM shared_models_set "
            "LEFT JOIN shared_models_newstation "
            "ON shared_models_set.new_station_id=shared_models_newstation.id "
            f"WHERE shared_models_set.id={self._get_current_row_pk()} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        station_name = str(result[0][0])
                           
        cod_secteur_releve = self.get_cod_secteur_releve()
        # 1 -> Côte-Nord
        if cod_secteur_releve == 1:
            # Need to get the station name, since it contains the cod_strate

            # use the first character as the strate value (need to fetch key)
            secteur = station_name[0]
            key = self.reference_data.get_ref_key(
                table="TYPE_STRATE_MOLL",
                pkey_col="COD_STRATE",
                col="STRATE",
                val=secteur,
                optional_query="AND COD_SECTEUR_RELEVE=1"
            )
            return key
        # IdM
        elif cod_secteur_releve == 4:
            from andes_migrate.ref_data.strat_pet_idm import strat_dict 
            # strip non-numeric characters            
            station_number = "".join(c for c in station_name if c.isdigit())
            if int(station_number) in strat_dict["Étang-du-Nord"]:
                secteur = "EN"
            elif int(station_number) in strat_dict["Dix-Milles"]:
                secteur = "DM"
            elif int(station_number) in strat_dict["Chaîne-de-la-Passe"]:
                secteur = "CP"
            else: 
                self.logger.error("Cannot find strat for station: %s", station_number)
                raise ValueError()
            key = self.reference_data.get_ref_key(
                table="TYPE_STRATE_MOLL",
                pkey_col="COD_STRATE",
                col="STRATE",
                val=secteur,
                optional_query="AND COD_SECTEUR_RELEVE=4"
            )
            return key
        else:
            self.logger.error("secteur %s not implemented", cod_secteur_releve)
            raise ValueError

    @validate_int()
    @log_results
    def get_no_station(self) -> int:
        """NO_STATION  INTEGER/NUMBER(5,0)
        Numéro de la station en fonction du protocole d'échantillonnage

        ANDES: shared_models_newstation.name
        The station name is stripped of non-numerical characters
        to generate no_station(i.e., NR524 -> 524)

        """

        query = f"SELECT shared_models_newstation.name \
                FROM shared_models_set \
                LEFT JOIN shared_models_newstation \
                    ON shared_models_set.new_station_id = shared_models_newstation.id \
                WHERE shared_models_set.id={self._get_current_row_pk()};"

        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        # extract all non-numerical chacters
        to_return = "".join(c for c in to_return if c.isnumeric())
        return to_return

    @tag(AndesCodeLookup)
    @validate_int()
    @log_results
    def get_cod_typ_trait(self) -> int:
        """COD_TYP_TRAIT INTEGER / NUMBER(5,0)
        Identification du type de trait tel que décrit dans la table TYPE_TRAIT

        Andes
        -----
        shared_models.stratificationtype.code
        via la mission
        shared_models.cruise.stratification_type_id

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988
        Code lookup

        """
        # this query does not work because of a mistmatch between reference tables
        # query = f"SELECT shared_models_stratificationtype.code \
        #         FROM shared_models_cruise \
        #         LEFT JOIN shared_models_stratificationtype \
        #             ON shared_models_cruise.stratification_type_id = shared_models_stratificationtype.id \
        #         WHERE shared_models_cruise.id={self.proj.pk};"

        # manual hack
        # we take the french description and match with Oracle\
        # the match isn't even verbatim, we we need a manual map

        # first, need to know what kind of set operations this is
        # One could lookup directly the operation with the set_id, but this seems more proper...
        query = (
            "SELECT shared_models_operation.name "
            "FROM shared_models_set "
            "LEFT JOIN shared_models_set_operations "
            "ON shared_models_set_operations.set_id = shared_models_set.id "
            "LEFT JOIN shared_models_operation "
            "ON shared_models_operation.id = shared_models_set_operations.operation_id "
            f"WHERE shared_models_set.id={self._get_current_row_pk()} "
        )
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        operation = result[0][0]

        # For CTD, this is a direct lookup
        if operation == 'CTD':
            desc = 'Océanographie seulement'
            key = self.reference_data.get_ref_key(
                table="TYPE_TRAIT",
                pkey_col="COD_TYP_TRAIT",
                col="DESC_TYP_TRAIT_F",
                val=desc,
            )
            return key

        # For Fishing, need to lookup stratificatointype
        elif operation == 'Fishing':
            query = (
                f"SELECT shared_models_stratificationtype.description_fra "
                "FROM shared_models_cruise "
                "LEFT JOIN shared_models_stratificationtype "
                "ON shared_models_cruise.stratification_type_id = shared_models_stratificationtype.id "
                f"WHERE shared_models_cruise.id={self.proj._get_current_row_pk()} "
            )
            result = self.andes_db.execute_query(query)
            self._assert_one(result)
            desc = result[0][0]
            
            andes_2_oracle_map = {
                "Échantillonnage aléatoire": "Aléatoire simple",
                "Station fixe": "Station fixe",
            }

            key = self.reference_data.get_ref_key(
                table="TYPE_TRAIT",
                pkey_col="COD_TYP_TRAIT",
                col="DESC_TYP_TRAIT_F",
                val=andes_2_oracle_map[desc],
            )
            return key
        else:
            self.logger.error("Cannot determine COD_TYP_TRAIT")
            raise ValueError

    @tag("codelookup")
    @validate_int()
    @log_results
    def get_cod_result_oper(self) -> int:
        """COD_RESULT_OPER INTEGER / NUMBER(5,0)
        Résultat de l'activité de pêche tel que défini dans la table COD_RESULT_OPER

        Andes
        -----
        shared_models_setresult.code

        The Andes codes seem to match the first six from the Oracle database,
        except for hte mistmatch between code 5 and 6.
        see issue https://github.com/dfo-gulf-science/andes/issues/1237

        This one would be good to have linked with a regional code lookup
        https://github.com/dfo-gulf-science/andes/issues/988
        Code lookup

        """
        query = f"SELECT shared_models_setresult.code \
                FROM shared_models_set \
                LEFT JOIN shared_models_setresult \
                    ON shared_models_set.set_result_id = shared_models_setresult.id \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]

        # this ia a weird one...
        # see issue #1237
        andes_2_oracle_map = {
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
            "5": 6,
            "6": 5,
        }
        to_return = andes_2_oracle_map[str(to_return)]
        return to_return

    @tag(HardCoded, Deprecated)
    @deprecate(successor="DATE_HEURE_DEB_TRAIT")
    @validate_string(max_len=19, not_null=False)  # allow 19 chars instead of 10
    @log_results
    def get_date_deb_trait(self) -> str | None:
        """DATE_DEB_TRAIT DATE
        Date du début du trait, format AAAA-MM-JJ

        This field is deprecated in favour of DATE_HEURE_DEB_TRAIT
        This function redirects to ::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_date_heure_deb_trait`

        """
        return self.get_date_heure_deb_trait()

    @tag(HardCoded, Deprecated)
    @deprecate(successor="DATE_HEURE_FIN_TRAIT")
    @validate_string(max_len=19, not_null=False)  # allow 19 chars instead of 10
    @log_results
    def get_date_fin_trait(self) -> str | None:
        """DATE_FIN_TRAIT DATE
        Date de la fin du trait, format AAAA-MM-JJ

        This field is deprecated in favour of DATE_HEURE_FIN_TRAIT
        This function redirects to ::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_date_heure_fin_trait`

        """
        return self.get_date_heure_fin_trait()

    @tag(HardCoded, Deprecated)
    @deprecate(successor="DATE_HEURE_DEB_TRAIT")
    @validate_string(max_len=19, not_null=False)  # allow 19 chars instead of 10
    @log_results
    def get_hre_deb_trait(self) -> str | None:
        """HRE_DEB_TRAIT DATE
        Heure du début du trait, format HH:MI:SS

        This field is deprecated in favour of DATE_HEURE_DEB_TRAIT
        This function redirects to ::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_date_heure_deb_trait`

        """
        return self.get_date_heure_deb_trait()

    @tag(HardCoded)
    @deprecate(successor="DATE_HEURE_FIN_TRAIT")
    @validate_string(max_len=19, not_null=False)  # allow 19 chars instead of 10
    @log_results
    def get_hre_fin_trait(self) -> str | None:
        """HRE_FIN_TRAIT DATE
        Heure de la fin du trait, format HH:MI:SS

        This field is deprecated in favour of DATE_HEURE_FIN_TRAIT
        This function redirects to ::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_date_heure_fin_trait`

        """
        return self.get_date_heure_fin_trait()

    @validate_int(min_val=0, max_val=1, not_null=False)
    @log_results
    def get_cod_typ_heure(self) -> int | None:
        """COD_TYP_HEURE INTEGER / NUMBER(5,0)
        Type d'heure en vigueur lors de la réalisation du trait tel que défini dans la table TYPE_HEURE

        0 -> Normale Standard
        1 -> Avancée Daylight saving
        2 -> GMT GMT

        N.B. The 3 (GMT) value is not present in Access (but GMS is a zone that never gets DST?).

        Andes DB fields are always in internally stored as UTC and only converted to the timezone by a the client.

        To stay consistent with existing data, Quebec times will be used.

        The datetime from shared_models_set.start_date is taken as a reference.
        It is then cast in the America/Montreal zone and the DST offset is extracted from it.

        The code lookup is then made on the value "Avancée" or "Normale"

        """
        query = f"SELECT shared_models_set.start_date \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        dt = result[0][0]
        # if no start date, stop trying to find a cod_stype_heure, just return none
        if dt is None:
            return None
        dt_str, timezone_str, is_dst = TraitMollusque.format_time(dt)
        if is_dst:
            desc_val = "Avancée"
        else:
            desc_val = "Normale"
        to_return = self.reference_data.get_ref_key(
            table="TYPE_HEURE",
            pkey_col="COD_TYP_HEURE",
            col="DESC_TYP_HEURE_F",
            val=desc_val,
        )

        return to_return

    @validate_int(min_val=0, max_val=3, not_null=False)
    @log_results
    def get_cod_fuseau_horaire(self) -> int | None:
        """COD_FUSEAU_HORAIRE INTEGER / NUMBER(5,0)
        Fuseau horaire utilisé pour déterminer l'heure de réalisation du trait tel que décrit dans la table FUSEAU_HORAIRE

        0 -> GMT         GMT
        1 -> Québec      Quebec
        2 -> Maritimes   Atlantic
        3 -> Terre-Neuve Newfoundland

        To stay consistent with existing data, Quebec times will be used.

        The datetime from shared_models_set.start_date is taken as a reference.
        It is then cast in the America/Montreal zone and the DST offset is extracted from it.

        The code lookup is then made on the value "Quebec"

        """
        query = f"SELECT shared_models_set.start_date \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        dt = result[0][0]

        # if no start date, stop trying to find a cod_fuseau_horaire, just return none
        if dt is None:
            return None

        dt_str, timezone_str, is_dst = TraitMollusque.format_time(dt)
        if timezone_str == "America/Montreal":
            to_return = self.reference_data.get_ref_key(
                table="FUSEAU_HORAIRE",
                pkey_col="COD_FUSEAU_HORAIRE",
                col="DESC_FUSEAU_HORAIRE_F",
                val="Québec",
            )
            return to_return
        else:
            self.logger.error("Found an unexpected timezone: %s", timezone_str)
            raise NotImplementedError

    @tag(HardCoded, AndesCodeLookup)
    @validate_int(not_null=False)
    @log_results
    def get_cod_method_pos(self) -> int | None:
        """COD_METHOD_POS INTEGER / NUMBER(5,0)
        Identification de la méthode utilisée pour déterminer les coordonnées de l''emplacement d''échantillonnage tel que défini dans la table METHODE_POSITION

        0 -> Inconnue
        1 -> Estimation
        2 -> Radar
        3 -> Decca
        4 -> Loran
        5 -> Satellite (GPS)
        6 -> Satellite (DGPS)

        Andes uses the vessel GPS, which may or may not have DGPS data.
        This is specified via the GPS Quality indicator (6th field) of the $GPGGA NMEA message.

        Historical records indicated having used DGPS, which is what is returnd here.
        This function always returns 6.
        """
        # hard-code this
        to_return = self._hard_coded_result(6)
        return to_return

    @log_results
    def get_lat_deb_trait(self) -> float | None:
        """LAT_DEB_TRAIT DOUBLE / NUMBER
        Position de latitude du début du trait, unité ddmm.%%%% N

        This uses a unique encoding scheme for the coordinates.

        Andes: shared_models_set.start_latitude

        """
        query = f"SELECT shared_models_set.start_latitude \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        # convert to Oracle coord encoding
        to_return = OracleHelper._to_oracle_coord(to_return)
        return to_return

    @log_results
    def get_lat_fin_trait(self) -> float | None:
        """LAT_FIN_TRAIT DOUBLE / NUMBER
        Position de latitude de la fin du trait, unité ddmm.%%%% N

        This uses a unique encoding scheme for the coordinates.

        Andes: shared_models_set.end_latitude

        """
        query = f"SELECT shared_models_set.end_latitude \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        # convert to Oracle coord encoding
        to_return = OracleHelper._to_oracle_coord(to_return)
        return to_return

    @log_results
    def get_long_deb_trait(self) -> float | None:
        """LONG_DEB_TRAIT DOUBLE / NUMBER
        Position de longitude du début du trait, unité ddmm.%%%% W
        This uses a unique encoding scheme for the coordinates.

        Andes: shared_models_set.start_longitude

        """
        query = f"SELECT shared_models_set.start_longitude \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        
        # return right away if None
        if to_return is None:
            return to_return
        
        # convert to Oracle coord encoding
        to_return = OracleHelper._to_oracle_coord(to_return)
        # strip negative from longitudes
        to_return *= -1
        return to_return

    @log_results
    def get_long_fin_trait(self) -> float | None:
        """LONG_fin_TRAIT DOUBLE / NUMBER
        Position de longitude de la fin du trait, unité ddmm.%%%% W
        This uses a unique encoding scheme for the coordinates.

        Andes: shared_models_set.end_longitude

        """
        query = f"SELECT shared_models_set.end_longitude \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]

        # return right away if None
        if to_return is None:
            return to_return

        # convert to Oracle coord encoding
        to_return = OracleHelper._to_oracle_coord(to_return)
        # strip negative from longitudes
        to_return *= -1
        return to_return

    @tag(HardCoded)
    @log_results
    def get_latlong_p(self) -> float | None:
        """LATLONG_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage pour les variables de positionnement en latitude et longitude

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        Not sure what to use for GPS coords taken from the vessel NMEA feed.
        Historical records indicate 0.01 and 0.001, for now, this function will always return None.

        """
        # TODO: Find what to do about this.
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_distance_pos(self) -> float | None:
        """DISTANCE_POS DOUBLE / NUMBER
        Distance parcourue évaluée à partir des coordonnées des positions en latitude et longitude, unité mètre

        There can be more that than one way to evaluate the distance:
         - crow's distance on a locally flat projection (like quebec-lambert)
         - crow's distance on a curved surface
         - integration of the vessel position (path length) on a flat projection
         - integration of the vessel position (path length) on a curved surface

        This is a derived metric and not pure source-data (i.e., it can be re-computed from source data).
        It may be best to delagate the evaluation to the analyst, and to not populate this field with andes.

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_distance_pos_p(self) -> float | None:
        """DISTANCE_POS_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Distance_Pos"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_vit_touage(self) -> float | None:
        """VIT_TOUAGE DOUBLE / NUMBER
        Vitesse de touage, en nœuds,  lors de la réalisation du trait

        This is a derived metric and not pure source-data (i.e., it can be re-computed from source data).
        It may be best to delagate the evaluation to the analyst, and to not populate this field with andes.

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_vit_touage_p(self) -> float | None:
        """VIT_TOUAGE_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Vit_Touage"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty
        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_duree_trait(self) -> float | None:
        """DUREE_TRAIT DOUBLE / NUMBER
        Durée du trait, en seconde évaluée selon différence entre l'heures de fin et de début

        This is a derived metric and not pure source-data (i.e., it can be re-computed from source data).
        It may be best to delagate the evaluation to the analyst, and to not populate this field with andes.

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_duree_trait_p(self) -> float | None:
        """DUREE_TRAIT_P DOUBLE / NUMBER
        Nombre de chiiffre après la décimale pour la précision d'affichage associée à "Duree_Trait"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty
        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_temp_fond(self) -> float | None:
        """TEMP_FOND DOUBLE / NUMBER
        Température de l'eau sur le fond, unité ° C

        Andes: does not log temperature data.

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded)
    @log_results
    def get_temp_fond_p(self) -> float | None:
        """TEMP_FOND_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Temp_Eau_Fond"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        Andes:  does not log temperature data.

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @log_results
    def get_prof_deb(self) -> float | None:
        """PROF_DEB DOUBLE / NUMBER
        Profondeur au début du trait, unité mètre

        units: metre

        Andes: shared_models_set.start_depth_m

        """
        query = f"SELECT shared_models_set.start_depth_m \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @tag(HardCoded)
    @log_results
    def get_prof_deb_p(self) -> float | None:
        """PROF_DEB_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Temp_Eau_Fond"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @log_results
    def get_prof_fin(self) -> float | None:
        """PROF_FIN DOUBLE / NUMBER
        Profondeur au début du trait, unité mètre

        units: metre

        Andes: shared_models_set.start_depth_m

        """
        query = f"SELECT shared_models_set.start_depth_m \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = result[0][0]
        return to_return

    @tag(HardCoded)
    @log_results
    def get_prof_fin_p(self) -> float | None:
        """PROF_FIN_P DOUBLE / NUMBER
        Nombre de chiffre après la décimale pour la précision d'affichage associée à "Temp_Eau_Fond"

        N.B. the description seems wrong, it's not the number of digits after the decimal, but rather the uncertainty

        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @validate_string(max_len=500, not_null=False)
    @log_results
    def get_rem_trait_moll(self) -> str | None:
        """REM_TRAIT_MOLL VARCHAR(500) / VARCHAR2(500)
        Remarque sur la réalisation du trait

        Andes: shared_models_set.remarks

        """
        query = f"SELECT shared_models_set.remarks \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        to_return = str(result[0][0])

        # need to remove line breaks and carriage-returns
        # only because it currupts de .dat files,
        to_return = to_return.replace('\n', ' ')
        to_return = to_return.replace('\r', ' ')
        return to_return

    @validate_int(not_null=False)
    @log_results
    def get_no_chargement(self) -> float | int | None:
        """NO_CHARGEMENT DOUBLE / NUMBER
        Numéro de l'activité de chargement de données dans la base Oracle

        Extrait du projet ::func:`~andes_migrate.projet_mollusque.ProjetMollusque.get_no_chargement`

        """
        return self.proj.get_no_chargement()

    @validate_string(max_len=19, not_null=False)
    @log_results
    def get_date_heure_deb_trait(self) -> str | None:
        """DATE_HEURE_DEB_TRAIT
        Date et heure du début du trait, format AAAA-MM-JJ HH:MI:SS

        Andes: shared_models_set.end_date

        Best practices dictate the use of UTC to store datetimes.
        The convention is followed by andes, and thus all imported andes datetimes
        will be in the UTC, as indicate in COD_FUSEAU_HORAIRE and COD_TYPE_HEURE

        """

        query = f"SELECT shared_models_set.start_date \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        dt = result[0][0]
        if type(dt) == datetime.datetime:
            (dt_str, timezone_str, is_dst) = TraitMollusque.format_time(dt)
            return dt_str
        else:
            self.logger.warn("Expected a datetime object , received None")
            return None

    @validate_string(max_len=19, not_null=False)
    @log_results
    def get_date_heure_fin_trait(self) -> str | None:
        """DATE_HEURE_FIN_TRAIT
        Date et heure de la fin du trait, format AAAA-MM-JJ HH:MI:SS

        Andes: shared_models_set.end_date

        Best practices dictate the use of UTC to store datetimes.
        The convention is followed by andes, and thus all imported andes datetimes
        will be in the UTC, as indicate in COD_FUSEAU_HORAIRE and COD_TYPE_HEURE

        """

        query = f"SELECT shared_models_set.end_date \
                FROM shared_models_set \
                WHERE shared_models_set.id={self._get_current_row_pk()};"
        result = self.andes_db.execute_query(query)
        self._assert_one(result)
        dt = result[0][0]
        if type(dt) == datetime.datetime:
            (dt_str, timezone_str, is_dst) = TraitMollusque.format_time(dt)
            return dt_str
        else:
            self.logger.warn("Expected a datetime object , received None")
            return None

    @tag(HardCoded, Deprecated)
    @deprecate
    @log_results
    def get_salinite_fond(self) -> float | None:
        """SALINITE_FOND DOUBLE / NUMBER
        Salinité de l'eau au fond, unité psu

        Oracle Optimisation: column not in MSACCESS, is deprecated?

        Andes does not log salinity data.
        This function always returns None

        """
        # hard-code this
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded, Deprecated)
    @deprecate
    @log_results
    def get_salinite_fond_p(self) -> float | None:
        """SALINITE_FOND_P DOUBLE / NUMBER
        Précision d'affichage associée à '' Salinite_Fond''

        Oracle Optimisation: column not in MSACCESS, is deprecated?

        Andes does not log salinity data.
        This function always returns None
        """
        to_return = self._hard_coded_result(None)
        return to_return

    @tag(HardCoded, Deprecated)
    @log_results
    def get_cod_typ_ech_trait(self) -> float | None:
        """COD_TYP_ECH_TRAIT DOUBLE / NUMBER
        Identification du type d'activité d'échantillonnage réalisé à la station tel que décrit dans la table TYPE_ECHANT_TRAIT

        Oracle Optimisation: this datatype should be INTEGER
        Oracle Optimisation: seems to be relational, but not setup that way
        Oracle Optimisation: column and ref table not in MSACCESS, is deprecated?

        This function always returns None

        """

        return self._hard_coded_result(None)
