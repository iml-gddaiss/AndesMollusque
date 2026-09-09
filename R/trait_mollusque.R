#' Gets trait_mollusque_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the TRAIT_MOLLUSQUE table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_trait_mollusque`
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @return A dataframe containing fishing set data.
#' @seealso [get_trait_mollusque()] for the formatted results
#' @export
get_trait_mollusque_db <- function(andes_db_connection) {
  query <- readr::read_file(system.file(
    "sql_queries",
    "trait_mollusque.sql",
    package = "ANDESMollusque"
  ))
  result <- DBI::dbSendQuery(andes_db_connection, query)
  proj <- DBI::dbFetch(result, n = Inf)
  DBI::dbClearResult(result)
  return(proj)
}


#' Gets trait_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data
#' to construct the TRAIT_MOLLUSQUE table and formats the results.
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @param proj A formatted projet_mollusque dataframe
#' @return A dataframe containing formatted fishing set data.
#' @export
get_trait_mollusque <- function(andes_db_connection, proj = NULL) {
  # Validate input
  if (is.null(proj)) {
    logger::log_error(
      "Must provide a formatted projet_mollusque dataframe."
    )
    stop("Must provide a formatted projet_mollusque dataframe.")
  }

  # Get raw data
  trait <- get_trait_mollusque_db(andes_db_connection)

  # Take data from projet_mollusque
  cols_from_parent <- c(
    "COD_SOURCE_INFO",
    "NO_RELEVE",
    "COD_NBPC",
    "NO_CHARGEMENT"
  )
  data_from_parent <- proj[, names(proj) %in% cols_from_parent]
  trait <- left_join_preserve_order(
    trait,
    data_from_parent,
    on = "andes_mission_id"
  )

  # temporarily get desc_serie_hist_f from proj to trait, it will provide the context to correctly get the zone/strate
  desc_serie_hist_f <- get_ref_key(
    table = "Indice_Suivi_Etat_Stock",
    pkey_col = "DESC_SERIE_HIST_F",
    col = "COD_SERIE_HIST",
    val = proj$COD_SERIE_HIST
  )

  # temporarily get cod_sect_releve, this is obtained from desc_secteur_releve_f
  # all sets shold have the same desc_serie_hist_f, verify this
  if (length(unique(trait$desc_secteur_releve_f)) != 1) {
    logger::log_error("The sets do not have the same desc_secteur_releve_f")
    stop("The sets do not have the same desc_secteur_releve_f")
  }
  # they are all the same, as it should be be, so just take the first one)
  desc_secteur_releve_f <- trait$desc_secteur_releve_f[1]

  trait <- format_cod_secteur_releve(trait, desc_secteur_releve_f)

  # we can now get rid of that column
  trait$desc_secteur_releve_f <- NULL

  # add COD_STRATE
  trait <- format_cod_strate(trait, desc_serie_hist_f)

  # add COD_ZONE_GEST_MOLL
  trait <- format_zone(trait, desc_serie_hist_f)

  # it is now fine to rename (if needed) the stations
  trait <- format_no_station(trait)

  # The mission-level desc_stratification was part of the set
  # they are all the same, as it should be be, so just take the first one)
  if (length(unique(trait$desc_stratification)) != 1) {
    logger::log_error("The sets do not have the same desc_secteur_releve_f")
    stop("The sets do not have the same desc_secteur_releve_f")
  }

  desc_stratification <- trait$desc_stratification[1]
  # we can now get rid of the desc_stratification column
  trait$desc_stratification <- NULL

  trait <- format_cod_typ_trait(trait, desc_stratification)

  # can get rid of operation column
  trait$operation <- NULL

  # can get rid of andes_mission_id column
  trait$andes_mission_id <- NULL
  # validate set_result : conistency between set_is_valid and COD_RESULT_OPER.

  trait <- validate_set_result(trait)

  # make a copy for for the times before we format the dates
  trait$HRE_DEB_TRAIT <- trait$DATE_DEB_TRAIT
  trait$HRE_FIN_TRAIT <- trait$DATE_FIN_TRAIT

  trait <- format_date_trait(trait)

  # call format_cod_typ_heure before changing to oracle time
  trait <- format_cod_typ_heure(trait)

  # ok, now change andes time string to oracle time string
  trait <- format_date_hre_trait(trait)

  # all times are cast to America/Toronto, which is called "Québec"
  cod_fuseau_horaire <- get_ref_key(
    table = "FUSEAU_HORAIRE",
    pkey_col = "COD_FUSEAU_HORAIRE",
    col = "DESC_FUSEAU_HORAIRE_F",
    val = "Québec"
  )

  trait <- add_hard_coded_value(
    trait,
    col_name = "COD_FUSEAU_HORAIRE",
    value = cod_fuseau_horaire
  )

  # 0 -> Inconnue
  # 1 -> Estimation
  # 2 -> Radar
  # 3 -> Decca
  # 4 -> Loran
  # 5 -> Satellite (GPS)
  # 6 -> Satellite (DGPS)
  trait <- add_hard_coded_value(trait, col_name = "COD_METHOD_POS", value = 6)

  trait <- format_coordinates(trait)
  trait <- add_hard_coded_value(trait, col_name = "LATLONG_P", value = NA)

  trait <- add_hard_coded_value(trait, col_name = "DISTANCE_POS", value = NA)
  trait <- add_hard_coded_value(
    trait,
    col_name = "DISTANCE_POS_P",
    value = NA
  )

  trait <- add_hard_coded_value(trait, col_name = "VIT_TOUAGE", value = NA)
  trait <- add_hard_coded_value(trait, col_name = "VIT_TOUAGE_P", value = NA)

  trait <- add_hard_coded_value(trait, col_name = "DUREE_TRAIT", value = NA)
  trait <- add_hard_coded_value(trait, col_name = "DUREE_TRAIT_P", value = NA)

  trait <- add_hard_coded_value(trait, col_name = "TEMP_FOND", value = NA)
  trait <- add_hard_coded_value(trait, col_name = "TEMP_FOND_P", value = NA)

  trait <- add_hard_coded_value(trait, col_name = "PROF_DEB_P", value = NA)
  trait <- add_hard_coded_value(trait, col_name = "PROF_FIN_P", value = NA)

  # trait <- add_hard_coded_value(trait, col_name = "SALINITE_FOND", value = NA)
  # trait <- add_hard_coded_value(trait, col_name = "SALINITE_FOND_P", value = NA)
  # trait <- add_hard_coded_value(trait, col_name = "COD_TYP_ECH_TRAIT", value = NA)

  # convert these strings to numeric
  trait <- cols_to_numeric(
    trait,
    col_names = c(
      "NO_STATION",
      "COD_RESULT_OPER",
      "LATLONG_P",
      "DISTANCE_POS",
      "DISTANCE_POS_P",
      "VIT_TOUAGE",
      "VIT_TOUAGE_P",
      "DUREE_TRAIT",
      "DUREE_TRAIT_P",
      "TEMP_FOND",
      "TEMP_FOND_P",
      "PROF_DEB",
      "PROF_DEB_P",
      "PROF_FIN",
      "PROF_FIN_P"
    )
  )

  return(trait)
}

#' Perform database validation checks on the dataframe
#' @details This compares the dataframe columns and values to the requirements of the database
#' @param df The dataframe to validate
#' @return Boolean representing if all the validation tests have passed
#' @export
validate_trait_mollusque <- function(df) {
  is_valid <- TRUE
  # check all required cols are present
  result <- check_columns_present(
    df,
    col_names = c(
      "COD_SOURCE_INFO",
      "NO_RELEVE",
      "COD_NBPC",
      "IDENT_NO_TRAIT",
      "COD_ZONE_GEST_MOLL",
      "COD_SECTEUR_RELEVE",
      "COD_STRATE",
      "NO_STATION",
      "COD_TYP_TRAIT",
      "COD_RESULT_OPER",
      "DATE_DEB_TRAIT",
      "DATE_FIN_TRAIT",
      "HRE_DEB_TRAIT",
      "HRE_FIN_TRAIT",
      "COD_TYP_HEURE",
      "COD_FUSEAU_HORAIRE",
      "COD_METHOD_POS",
      "LAT_DEB_TRAIT",
      "LAT_FIN_TRAIT",
      "LONG_DEB_TRAIT",
      "LONG_FIN_TRAIT",
      "LATLONG_P",
      "DISTANCE_POS",
      "DISTANCE_POS_P",
      "VIT_TOUAGE",
      "VIT_TOUAGE_P",
      "DUREE_TRAIT",
      "DUREE_TRAIT_P",
      "TEMP_FOND",
      "TEMP_FOND_P",
      "PROF_DEB",
      "PROF_DEB_P",
      "PROF_FIN",
      "PROF_FIN_P",
      "REM_TRAIT_MOLL",
      "NO_CHARGEMENT"
    )
  )
  is_valid <- is_valid & result

  # check all not-null columns do not have nulls
  result <- check_cols_contains_na(
    df,
    col_names = c(
      "COD_SOURCE_INFO",
      "NO_RELEVE",
      "COD_NBPC",
      "IDENT_NO_TRAIT",
      "NO_STATION",
      "COD_TYP_TRAIT",
      "COD_RESULT_OPER"
    )
  )
  is_valid <- is_valid & result

  result <- check_other_columns(
    df,
    col_names = c(
      "COD_SOURCE_INFO",
      "NO_RELEVE",
      "COD_NBPC",
      "IDENT_NO_TRAIT",
      "COD_ZONE_GEST_MOLL",
      "COD_SECTEUR_RELEVE",
      "COD_STRATE",
      "NO_STATION",
      "COD_TYP_TRAIT",
      "COD_RESULT_OPER",
      "DATE_DEB_TRAIT",
      "DATE_FIN_TRAIT",
      "HRE_DEB_TRAIT",
      "HRE_FIN_TRAIT",
      "COD_TYP_HEURE",
      "COD_FUSEAU_HORAIRE",
      "COD_METHOD_POS",
      "LAT_DEB_TRAIT",
      "LAT_FIN_TRAIT",
      "LONG_DEB_TRAIT",
      "LONG_FIN_TRAIT",
      "LATLONG_P",
      "DISTANCE_POS",
      "DISTANCE_POS_P",
      "VIT_TOUAGE",
      "VIT_TOUAGE_P",
      "DUREE_TRAIT",
      "DUREE_TRAIT_P",
      "TEMP_FOND",
      "TEMP_FOND_P",
      "PROF_DEB",
      "PROF_DEB_P",
      "PROF_FIN",
      "PROF_FIN_P",
      "REM_TRAIT_MOLL",
      "NO_CHARGEMENT"
    )
  )
  is_valid <- is_valid & result

  result <- check_numeric_columns(
    df,
    col_names = c(
      "COD_SOURCE_INFO",
      "NO_RELEVE",
      "IDENT_NO_TRAIT",
      "COD_ZONE_GEST_MOLL",
      "COD_SECTEUR_RELEVE",
      "COD_STRATE",
      "NO_STATION",
      "COD_TYP_TRAIT",
      "COD_RESULT_OPER",
      "COD_TYP_HEURE",
      "COD_FUSEAU_HORAIRE",
      "COD_METHOD_POS",
      "LAT_DEB_TRAIT",
      "LAT_FIN_TRAIT",
      "LONG_DEB_TRAIT",
      "LONG_FIN_TRAIT",
      "LATLONG_P",
      "DISTANCE_POS",
      "DISTANCE_POS_P",
      "VIT_TOUAGE",
      "VIT_TOUAGE_P",
      "DUREE_TRAIT",
      "DUREE_TRAIT_P",
      "TEMP_FOND",
      "TEMP_FOND_P",
      "PROF_DEB",
      "PROF_DEB_P",
      "PROF_FIN",
      "PROF_FIN_P",
      "NO_CHARGEMENT"
    )
  )
  is_valid <- is_valid & result

  return(is_valid)
}

#' Write dataframe to Access file
#'
#' @param trait Dataframe
#' @param access_db_write_connection the connection to Access file
#' @export
write_trait_mollusque <- function(trait, access_db_write_connection = NULL) {
  # write the dataframe to the database
  if (is.null(access_db_write_connection)) {
    logger::log_error("Failed to provide a new MS Access connection.")
    stop("Failed to provide a new MS Access connection")
  }

  # insert make one row at a time
  for (i in seq_len(nrow(trait))) {
    statement <- generate_sql_insert_statement(
      trait[i, ],
      "TRAIT_MOLLUSQUE"
    )
    logger::log_debug(
      "Writing the following statement to the database: {statement}"
    )
    result <- DBI::dbExecute(access_db_write_connection, statement)
    if (result != 1) {
      logger::log_error(
        "Failed to write a row to the TRAIT_MOLLUSQUE Table, row: {i}"
      )
      stop("Failed to write a row to the TRAIT_MOLLUSQUE Table")
    } else {
      logger::log_debug(
        "Successfully added a row to the TRAIT_MOLLUSQUE Table"
      )
    }
  }
  logger::log_info("Successfully wrote the trait_mollusque to the database.")
}
