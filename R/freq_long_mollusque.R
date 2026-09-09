#' Gets freq_long_mollusque_db (raw database results)
#'
#' This function executes a SQL query to retrieve the needed andes data to construct the FREQ_LONG_MOLLUSQUE table.
#' The current ANDES active mission will determine for which data are returned.
#'
#' This function is intended for internal use and returns raw results from the database.
#' It is not meant for direct use in analysis or reporting. Users should use `get_freq_long_mollusque`
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @return A dataframe containing fishing set data.
#' @seealso [get_freq_long_mollusque()] for the formatted results
#' @export
get_freq_long_mollusque_db <- function(andes_db_connection) {
  # mnake a small pre-query to get the observation type for length.
  # this is purposefully not inside the main query in case we want to change it later ...

  pre_query <- "SELECT shared_models_observationtype.id FROM shared_models_observationtype WHERE shared_models_observationtype.special_type=1"

  result <- DBI::dbSendQuery(andes_db_connection, pre_query)
  length_type <- DBI::dbFetch(result, n = Inf)
  DBI::dbClearResult(result)

  query <- readr::read_file(system.file(
    "sql_queries",
    "freq_long_mollusque.sql",
    package = "ANDESMollusque"
  ))

  # Select active mission
  query <- paste(query, "WHERE shared_models_mission.is_active=1 ")

  # just select the official length (obtained with pre-query above)
  query <- paste(
    query,
    "AND shared_models_observationtype.id=",
    length_type,
    sep = ""
  )
  query <- paste(query, "ORDER BY shared_models_specimen.id ASC")

  result <- DBI::dbSendQuery(andes_db_connection, query)
  freq <- DBI::dbFetch(result, n = Inf)
  DBI::dbClearResult(result)
  return(freq)
}

#' Gets freq_long_mollusque (formatted results)
#'
#' This function executes a SQL query to retrieve the needed andes data
#' to construct the TRAIT_MOLLUSQUE table and formats the results.
#'
#' @param andes_db_connection a connection object to the ANDES database.
#' @param capt A formatted capture_mollusque dataframe
#' @return A dataframe containing formatted fishing set data.
#' @export
get_freq_long_mollusque <- function(andes_db_connection, capt = NULL) {
  # Validate input
  if (is.null(capt)) {
    logger::log_error(
      "Must provide a formatted captutre_mollusque dataframe."
    )
    stop("Must provide a formatted captutre_mollusque dataframe.")
  }

  # Get raw data
  freq <- get_freq_long_mollusque_db(andes_db_connection)

  cols_from_capt <- c(
    "IDENT_NO_TRAIT",
    "COD_SOURCE_INFO",
    "NO_RELEVE",
    "COD_NBPC",
    "NO_CHARGEMENT",
    "COD_TYP_PANIER",
    "COD_ENG_GEN",
    "NO_ENGIN",
    "COD_ESP_GEN"
  )

  data_from_capt <- capt[, names(capt) %in% cols_from_capt]

  freq <- left_join_preserve_order(freq, data_from_capt, on = "IDENT_NO_TRAIT")

  freq <- add_hard_coded_value(
    freq,
    col_name = "VALEUR_LONG_MOLL_P",
    value = NA
  )

  freq <- format_cod_typ_long(freq)

  freq <- format_cod_tech_mesure_long(freq)

  freq <- format_cod_typ_etat(freq)

  freq <- format_no_mollusque(freq)

  # can get rid of temporary columns
  freq$id <- NULL
  freq$sample_number <- NULL
  freq$strap_code <- NULL
  freq$description_fra <- NULL
  freq$observation_type_id <- NULL

  # convert these strings to numeric
  freq <- cols_to_numeric(
    freq,
    col_names = c("VALEUR_LONG_MOLL", "VALEUR_LONG_MOLL_P")
  )
}

#' Perform database validation checks on the dataframe
#' @details This compares the dataframe columns and values to the requirements of the database
#' @param df The dataframe to validate
#' @return Boolean representing if all the validation tests have passed
#' @export

validate_freq_long_mollusque <- function(df) {
  is_valid <- TRUE
  # check all required cols are present
  result <- check_columns_present(
    df,
    col_names = c(
      "COD_ESP_GEN",
      "COD_ENG_GEN",
      "COD_SOURCE_INFO",
      "NO_RELEVE",
      "IDENT_NO_TRAIT",
      "COD_TYP_PANIER",
      "COD_NBPC",
      "NO_ENGIN",
      "VALEUR_LONG_MOLL",
      "NO_MOLLUSQUE",
      "COD_TYP_LONG",
      "VALEUR_LONG_MOLL_P",
      "COD_TYP_ETAT",
      "NO_CHARGEMENT",
      "COD_TECH_MESURE_LONG"
    )
  )
  is_valid <- is_valid & result

  # check all not-null columns do not have nulls
  result <- check_cols_contains_na(
    df,
    col_names = c(
      "COD_ESP_GEN",
      "IDENT_NO_TRAIT",
      "NO_RELEVE",
      "COD_SOURCE_INFO",
      "COD_NBPC",
      "COD_ENG_GEN",
      "COD_TYP_PANIER",
      "NO_ENGIN",
      "COD_TYP_LONG",
      "COD_TYP_ETAT",
      "COD_TECH_MESURE_LONG"
    )
  )
  is_valid <- is_valid & result

  result <- check_other_columns(
    df,
    col_names = c(
      "COD_ESP_GEN",
      "IDENT_NO_TRAIT",
      "NO_RELEVE",
      "COD_SOURCE_INFO",
      "COD_NBPC",
      "COD_ENG_GEN",
      "COD_TYP_PANIER",
      "NO_ENGIN",
      "NO_MOLLUSQUE",
      "COD_TYP_LONG",
      "VALEUR_LONG_MOLL",
      "VALEUR_LONG_MOLL_P",
      "COD_TYP_ETAT",
      "COD_TECH_MESURE_LONG",
      "NO_CHARGEMENT"
    )
  )
  is_valid <- is_valid & result

  result <- check_numeric_columns(
    df,
    col_names = c(
      "COD_ESP_GEN",
      "IDENT_NO_TRAIT",
      "NO_RELEVE",
      "COD_SOURCE_INFO",
      "COD_ENG_GEN",
      "COD_TYP_PANIER",
      "NO_ENGIN",
      "NO_MOLLUSQUE",
      "COD_TYP_LONG",
      "VALEUR_LONG_MOLL",
      "VALEUR_LONG_MOLL_P",
      "COD_TECH_MESURE_LONG",
      "NO_CHARGEMENT"
    )
  )
  is_valid <- is_valid & result

  return(is_valid)
}

#' Write dataframe to Access file
#'
#' @param df Dataframe
#' @param access_db_write_connection connection to Access file
#' @export
write_freq_long_mollusque <- function(df, access_db_write_connection = NULL) {
  # write the dataframe to the database
  if (is.null(access_db_write_connection)) {
    logger::log_error("Failed to provide a new MS Access connection.")
    stop("Failed to provide a new MS Access connection")
  }

  # insert make one row at a time
  for (i in seq_len(nrow(df))) {
    statement <- generate_sql_insert_statement(
      df[i, ],
      "FREQ_LONG_MOLLUSQUE"
    )
    logger::log_debug(
      "Writing the following statement to the database: {statement}"
    )
    result <- DBI::dbExecute(access_db_write_connection, statement)
    if (result != 1) {
      logger::log_error(
        "Failed to write a row to the FREQ_LONG_MOLLUSQUE Table, row: {i}"
      )
      stop("Failed to write a row to the FREQ_LONG_MOLLUSQUE Table")
    } else {
      logger::log_debug(
        "Successfully added a row to the FREQ_LONG_MOLLUSQUE Table"
      )
    }
  }
  logger::log_info(
    "Successfully wrote the freq_long_mollusque dataframe to the database."
  )
}
