# Package index

## All functions

- [`access_db_connect()`](access_db_connect.md) : Establish a connection
  to an MS Access database
- [`add_hard_coded_value()`](add_hard_coded_value.md) : Add a hard-coded
  column with a specific value to the dataframe
- [`andes_db_connect()`](andes_db_connect.md) : Establish a connection
  to the andes database
- [`andes_str_to_oracle_date()`](andes_str_to_oracle_date.md) : Convert
  andes datetime string form DB to Oracle date format
- [`andes_str_to_oracle_datetime()`](andes_str_to_oracle_datetime.md) :
  Convert andes datetime string form DB to Oracle datetime format
- [`check_cols_contains_na()`](check_cols_contains_na.md) : Checks if
  all dataframe cols named in the col_names contain NA This is useful to
  validate if a dataframe can be written to a DB table (where some
  columns values cannot be null)
- [`check_columns_present()`](check_columns_present.md) : Make sure the
  columns listed in col_names are present in the dataframe
- [`check_numeric_columns()`](check_numeric_columns.md) : Make sure the
  columns listed in col_names have numeric (or NA) values
- [`check_other_columns()`](check_other_columns.md) : Make sure no other
  columns than the ones listed in col_names are present in the dataframe
- [`cleanup_text()`](cleanup_text.md) : Cleanup text form ANDES to play
  nice in Oracle
- [`cols_to_numeric()`](cols_to_numeric.md) : Convert all dataframe cols
  named in the col_names to a numeric value
- [`create_new_access_db()`](create_new_access_db.md) : Creates a new MS
  Access database file
- [`format_cod_descrip_capt()`](format_cod_descrip_capt.md) : Format
  COD_DESCRIP_CAPT
- [`format_cod_esp_gen()`](format_cod_esp_gen.md) : format the
  cod_esp_gen column
- [`format_cod_secteur_releve()`](format_cod_secteur_releve.md) : Format
  COD_SECTEUR_RELEVE
- [`format_cod_source_info()`](format_cod_source_info.md) : Format
  COD_SOURCE_INFO
- [`format_cod_strate()`](format_cod_strate.md) : Format COD_STRAT
- [`format_cod_tech_mesure_long()`](format_cod_tech_mesure_long.md) :
  Format COD_TECH_MESURE_LONG
- [`format_cod_typ_etat()`](format_cod_typ_etat.md) : Format
  COD_TYP_ETAT
- [`format_cod_typ_heure()`](format_cod_typ_heure.md) : Format
  COD_TYP_HEURE
- [`format_cod_typ_long()`](format_cod_typ_long.md) : Format
  COD_TYP_LONG
- [`format_cod_typ_mesure()`](format_cod_typ_mesure.md) : format
  COD_TYP_MESURE
- [`format_cod_typ_panier()`](format_cod_typ_panier.md) : Add the
  formated COD_TYP_PANIER column to the dataframe This is currently
  hard-coded to Panier doublé TODO: implement different types depending
  on chosen gear code, for exmaple:
- [`format_coordinates()`](format_coordinates.md) : Format coordinates
  for TRAIT_MOLLUSQUE
- [`format_date_deb_projet()`](format_date_deb_projet.md) : Format
  DATE_DE_PROJET
- [`format_date_fin_projet()`](format_date_fin_projet.md) : Format
  DATE_FIN_PROJET
- [`format_date_hre_trait()`](format_date_hre_trait.md) : Format dates
  for TRAIT_MOLLUSQUE
- [`format_date_trait()`](format_date_trait.md) : Format dates for
  TRAIT_MOLLUSQUE
- [`format_epibiont()`](format_epibiont.md) : Formated
  COD_COUVERTURE_EPIBIONT column to the dataframe
- [`format_no_mollusque()`](format_no_mollusque.md) : Format
  NO_MOLLUSQUE
- [`format_no_station()`](format_no_station.md) : Format NO_STATION
- [`format_seq_pecheur()`](format_seq_pecheur.md) : Format SEQ_PECHEUR
- [`format_zone()`](format_zone.md) : Format ZONE
- [`generate_sql_insert_statement()`](generate_sql_insert_statement.md)
  : Generate a SQL statement
- [`get_access_table_properties()`](get_access_table_properties.md) :
  get database table properties
- [`get_biometrie_petoncle()`](get_biometrie_petoncle.md) : Gets
  get_biometrie_petoncle (formatted results)
- [`get_biometrie_petoncle_db()`](get_biometrie_petoncle_db.md) : Gets
  capture_mollusc_db (raw database results)
- [`get_capture_mollusque()`](get_capture_mollusque.md) : Gets
  capture_mollusque (formatted results)
- [`get_capture_mollusque_db()`](get_capture_mollusque_db.md) : Gets
  capture_mollusc_db (raw database results)
- [`get_engin_mollusque()`](get_engin_mollusque.md) : Gets engin_mollusc
  (formatted results)
- [`get_engin_mollusque_db()`](get_engin_mollusque_db.md) : Gets
  engin_mollusc_db (raw database results)
- [`get_epibiont()`](get_epibiont.md) : This fetches the specimen-level
  coverage data and coverts it to an average set-level metric in
  accordance to the legacy Oracle database.
- [`get_freq_long_mollusque()`](get_freq_long_mollusque.md) : Gets
  freq_long_mollusque (formatted results)
- [`get_freq_long_mollusque_db()`](get_freq_long_mollusque_db.md) : Gets
  freq_long_mollusque_db (raw database results)
- [`get_legal_collection_names()`](get_legal_collection_names.md) : Get
  a list of legal collection names as a filter for
  get_biometrie_petoncle()
- [`get_projet_mollusque()`](get_projet_mollusque.md) : Gets fishing
  projet_mollusque (formatted results)
- [`get_projet_mollusque_db()`](get_projet_mollusque_db.md) : Gets
  fishing projet_mollusque (raw database results)
- [`get_ref_choices()`](get_ref_choices.md) : Builds a list of legal
  choices (descriptions) for get ref key
- [`get_ref_key()`](get_ref_key.md) : Get the reference key
  corresponding to a value (usually from the Oracle / MSAccess reference
  database)
- [`get_strate()`](get_strate.md) : Get the strate from reference
- [`get_trait_mollusque()`](get_trait_mollusque.md) : Gets
  trait_mollusque (formatted results)
- [`get_trait_mollusque_db()`](get_trait_mollusque_db.md) : Gets
  trait_mollusque_db (raw database results)
- [`init_cod_serie_hist()`](init_cod_serie_hist.md) : Add the
  cod_serie_hist to the whole dataframe This value is not present in
  ANDES so it will have to be specified here. Run this without
  desc_serie_hist_f to get a list of choices.
- [`is_andes_time_str_dst()`](is_andes_time_str_dst.md) : Verify is the
  ANDES dattime string is in daylight savings time
- [`left_join_preserve_order()`](left_join_preserve_order.md) : a merge
  that preserves row and column order
- [`lookup_cod_serie_hist()`](lookup_cod_serie_hist.md) : Lookup entry
  in Oracle
- [`lookup_cod_source_info()`](lookup_cod_source_info.md) : Lookup
  COD_SOURCE_INFO
- [`parse_andes_datetime()`](parse_andes_datetime.md) : Convert ANDES
  UTC time string and converts it to a POSIXct object
- [`sanitize_sql_value()`](sanitize_sql_value.md) : Sanitize value to
  SQL statement
- [`strip_alphabetic()`](strip_alphabetic.md) : This function removes
  alphabetic characters from a string
- [`to_oracle_coord()`](to_oracle_coord.md) : Convert coordinate to
  Oracle format
- [`validate_capture_mollusque()`](validate_capture_mollusque.md) :
  Perform database validation checks on the dataframe
- [`validate_engin_mollusque()`](validate_engin_mollusque.md) : Perform
  database validation checks on the dataframe
- [`validate_freq_long_mollusque()`](validate_freq_long_mollusque.md) :
  Perform database validation checks on the dataframe
- [`validate_projet_mollusque()`](validate_projet_mollusque.md) :
  Perform database validation checks on the dataframe
- [`validate_set_result()`](validate_set_result.md) : Validate set
  result consistency
- [`validate_trait_mollusque()`](validate_trait_mollusque.md) : Perform
  database validation checks on the dataframe
- [`write_capture_mollusque()`](write_capture_mollusque.md) : Write to
  Access file
- [`write_engin_mollusque()`](write_engin_mollusque.md) : Write to
  Access file
- [`write_freq_long_mollusque()`](write_freq_long_mollusque.md) : Write
  dataframe to Access file
- [`write_projet_mollusque()`](write_projet_mollusque.md) : Write to
  Access file
- [`write_trait_mollusque()`](write_trait_mollusque.md) : Write
  dataframe to Access file
