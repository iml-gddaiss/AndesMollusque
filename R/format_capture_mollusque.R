#' Formated COD_COUVERTURE_EPIBIONT column to the dataframe
#'
#' This categorizes AND includes the of the following definition:
#' ave_coverage to cod_couverture :
#' 0 -> Aucune balane
#' 1 -> 1/3 et moins surface colonisée
#' 2 -> 1/3 à 2/3 surface colonisée
#' 3 -> 2/3 et plus surface colonisée
#'
#' #' ave_with_barnacles to cod_abondance
#' 0 -> Aucun des pétoncles ne porte de balane
#' 1 -> 1% à 20%% des pétoncles portent des balanes
#' 2 -> 21% à 40%% des pétoncles portent des balanes
#' 3 -> 41% à 60%% des pétonlces portent des balanes
#' 4 -> 61% à 80%% despétoncles portent des balanes
#' 5 -> 81% à 100%% des pétonlces portent des balanes
#' @param capt Dataframe
#' @param andes_db_connection A connection object to the ANDES database.
#' @param code_filter a list of species code to filter on, or NULL for no filtering
#' @return The input dataframe with columns for categorical codes
#' @export
format_epibiont <- function(capt, andes_db_connection, code_filter) {
  epibiont_data <- get_epibiont(andes_db_connection, code_filter)

  assert_col(epibiont_data, "ave_with_barnacles")
  assert_col(epibiont_data, "ave_coverage")

  # cod_couverture categories:
  # 0 -> Aucune balane
  # 1 -> 1/3 et moins surface colonisée
  # 2 -> 1/3 à 2/3 surface colonisée
  # 3 -> 2/3 et plus surface colonisée
  breaks <- c(0, 1 / 3., 2 / 3., 1)
  categories <- c(1, 2, 3)
  # We will handle case 0 later, no barnacle cases should have ave_coverage=NA

  # this should take care of all legal values of area
  cod_couverture <- cut(
    epibiont_data$ave_coverage,
    breaks = breaks,
    labels = categories,
  )
  # we don't actually want a factor, but a list with values from categories
  cod_couverture <- categories[as.integer(cod_couverture)]

  # now handle edge case when there is no legal area (because there are no barnacles)
  is_na_because_no_barnacles <- is.na(cod_couverture) &
    (epibiont_data$ave_with_barnacles == 0)
  cod_couverture[is_na_because_no_barnacles] <- 0

  epibiont_data$COD_COUVERTURE_EPIBIONT <- cod_couverture

  # cod_abondance categories:
  # 0 -> Aucun des pétoncles ne porte de balane
  # 1 -> 1% à 20%% des pétoncles portent des balanes
  # 2 -> 21% à 40%% des pétoncles portent des balanes
  # 3 -> 41% à 60%% des pétonlces portent des balanes
  # 4 -> 61% à 80%% despétoncles portent des balanes
  # 5 -> 81% à 100%% des pétonlces portent des balanes
  breaks <- c(0, 0.01, 0.21, 0.41, 0.61, 0.81, 1.0)
  categories <- c(0, 1, 2, 3, 4, 5)

  # this should take care of all legal values of area
  cod_abondance <- cut(
    epibiont_data$ave_with_barnacles,
    breaks = breaks,
    labels = categories,
    include.lowest = TRUE
  )
  # we don't actually want a factor, but a list with values from categories
  cod_abondance <- categories[as.integer(cod_abondance)]
  epibiont_data$COD_ABONDANCE_EPIBIONT <- cod_abondance

  # rename columns for the merge
  names(epibiont_data)[
    names(epibiont_data) == "sample_number"
  ] <- "IDENT_NO_TRAIT"
  names(epibiont_data)[names(epibiont_data) == "code"] <- "strap_code"

  # merge
  capt <- left_join_preserve_order(
    capt,
    epibiont_data,
    by = c("IDENT_NO_TRAIT", "strap_code")
  )
  # can get rid the extra cols
  capt$ave_with_barnacles <- NULL
  capt$ave_coverage <- NULL
  capt$description_fra <- NULL

  return(capt)
}


#' This fetches the specimen-level coverage data and
#' coverts it to an average set-level metric in accordance to the legacy Oracle database.
#' @param andes_db_connection A connection object to the ANDES database.
#' @param code_filter a list of species code to filter on, or NULL for no filtering
#' @export
get_epibiont <- function(andes_db_connection, code_filter) {
  query <- readr::read_file(system.file(
    "sql_queries",
    "epibiont_cte.sql",
    package = "ANDESMollusque"
  ))
  # finish the query from the primed CTE statement
  # the easy way is to grab the final epibiont_cte table
  # query <- paste(query,"SELECT * FROM epibiont_cte")

  # the hard way is to take the balane_cte table and re-compute the post-porcessing.
  # let's to the hard-way, this effectively move SQL post-precessing into R post-processing
  # Moving the post-processing out of SQL and into R makes it easier to maintain / debug

  query <- paste(query, "SELECT * FROM balane_cte")
  result <- DBI::dbSendQuery(andes_db_connection, query)
  df <- DBI::dbFetch(result, n = Inf)
  DBI::dbClearResult(result)

  # apply the code filter, ideally it would have been applied in the SQL, but here we are
  if (!is.null(code_filter)) {
    df <- df[df$code %in% code_filter, ]
  }

  # tag each specimen as having barnacles or not ( observation_value is anything but category 0)
  has_barnacles <- as.numeric(df$observation_value > 0)
  abondance_epibiont <- stats::aggregate(
    x = list(ave_with_barnacles = has_barnacles),
    by = list(code = df$code, sample_number = df$sample_number),
    FUN = mean
  )
  # View(abondance_epibiont)
  # done with abondance_epibiont

  # assign each coverage category with a numerical value (based o the midpoint)
  code_map <- data.frame(
    observation_value = c("1", "2", "3"),
    coverage = c((0 + 1) / 6., (1 + 2) / 6., (2 + 3) / 6.)
  )
  # res <- merge(desc_typ_trait, code_map, by = "desc", all.x = TRUE, sort = FALSE)
  coverage <- left_join_preserve_order(
    df,
    code_map,
    by = "observation_value"
  )$coverage

  # category <- c ("1", "2", "3")
  # value <- c ((0+1)/6. ,(1+2)/6. ,(2+3)/6. )

  couverture_epibiont <- stats::aggregate(
    x = list(ave_coverage = coverage),
    by = list(code = df$code, sample_number = df$sample_number),
    FUN = mean,
    na.rm = TRUE
  )
  # View(couverture_epibiont)
  # done with couverture_epibiont

  # now we combine them
  joined <- merge(
    x = abondance_epibiont,
    y = couverture_epibiont,
    all = TRUE,
    sort = FALSE
  )

  return(joined)
}

#' format the cod_esp_gen column
#'
#' @param capt Dataframe de capture
#' @export
format_cod_esp_gen <- function(capt) {
  query <- readr::read_file(system.file(
    "sql_queries",
    "esp_gen_code_map.sql",
    package = "ANDESMollusque"
  ))

  # manually delete the comments from the SQL query... because ACCESS...
  query <- gsub("--.*?\n", "", query)
  access_db_connection <- access_db_connect()
  result <- DBI::dbSendQuery(access_db_connection, query)
  cod_esp_gen_map <- DBI::dbFetch(result, n = Inf)
  DBI::dbClearResult(result)
  DBI::dbDisconnect(access_db_connection)

  # rename column to match for the merge
  names(cod_esp_gen_map)[
    names(cod_esp_gen_map) == "COD_ESPECE"
  ] <- "strap_code"
  capt <- left_join_preserve_order(capt, cod_esp_gen_map, by = "strap_code")

  # we can probably drop the strap_code column now...
  # capt$strap_code <- NULL
  return(capt)
}

#' format COD_TYP_MESURE
#'
#' @param capt Dataframe
#' For now, we will just assume all data is quantitative
#' since we are limiting ourselves to commercial (scallops and whelk).
#' One day, this function should be generalized to also lookup the data and determine it.
#' @export
format_cod_typ_mesure <- function(capt) {
  quantitative_code <- get_ref_key(
    table = "TYPE_MESURE_MOLL",
    pkey_col = "COD_TYP_MESURE",
    col = "DESC_TYP_MESURE_F",
    val = "Données quantitatives",
  )
  capt <- add_hard_coded_value(
    capt,
    col_name = "COD_TYP_MESURE",
    value = quantitative_code
  )
  return(capt)
}

#' Format COD_DESCRIP_CAPT
#'
#' For now, we will just keep this blank
#' It is a function here so that one day we can use the ANDES relative abundance category
#' But it is not used for commercial stocks, so we skip it
#'
#' @param capt Dataframe
#' @export
format_cod_descrip_capt <- function(capt) {
  capt <- add_hard_coded_value(
    capt,
    col_name = "COD_DESCRIP_CAPT",
    value = NA
  )
  return(capt)
}
