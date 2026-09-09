#' Format COD_TYP_LONG
#'
#' @param freq Dataframe to reformat
#' @export
format_cod_typ_long <- function(freq) {
  code <- get_ref_key(
    table = "Type_Longueur",
    pkey_col = "COD_TYP_LONG",
    col = "NOM_TYP_LONG_F",
    val = "Hauteur coquille",
  )
  freq <- add_hard_coded_value(freq, col_name = "COD_TYP_LONG", value = code)
  return(freq)
}

#' Format COD_TECH_MESURE_LONG
#'
#' @param freq Dataframe to reformat
#' @export
format_cod_tech_mesure_long <- function(freq) {
  code_class_proj <- get_ref_key(
    table = "Classe_Projet",
    pkey_col = "COD_CLASSE_PROJET",
    col = "DESC_CLASSE_PROJET_F",
    val = "Relevés mollusque MPO"
  )

  code <- get_ref_key(
    table = "TECHNIQUE_MESURE_LONG_PROJET",
    pkey_col = "COD_TECH_MESURE_LONG",
    col = "DESC_TECH_MESURE_LONG_F",
    val = "Vernier électronique",
    optional_query = paste("AND COD_CLASSE_PROJET=", code_class_proj)
  )
  freq <- add_hard_coded_value(
    freq,
    col_name = "COD_TECH_MESURE_LONG",
    value = code
  )
  return(freq)
}

#' Format COD_TYP_ETAT
#'
#' @param freq Dataframe to reformat
#' @export
format_cod_typ_etat <- function(freq) {
  # In andes there are only two types (encoded by basket-class), vivant or claquettes
  code_vivant <- get_ref_key(
    table = "TYPE_ETAT_MOLL",
    pkey_col = "COD_TYP_ETAT",
    col = "DESC_TYP_ETAT_F",
    val = "Vivant intact",
  )
  code_claquette <- get_ref_key(
    table = "TYPE_ETAT_MOLL",
    pkey_col = "COD_TYP_ETAT",
    col = "DESC_TYP_ETAT_F",
    val = "Claquette ouverte, int. nacré, ressort dans charnière",
  )

  type_etat_map <- data.frame(
    description_fra = c(
      "Vivant intact",
      "Claquette ouverte, int. nacré, ressort dans charnière"
    ),
    COD_TYP_ETAT = c(code_vivant, code_claquette)
  )

  freq <- left_join_preserve_order(freq, type_etat_map, on = "description_fra")

  return(freq)
}


#' Format NO_MOLLUSQUE
#'
#' @param freq Dataframe to reformat
#' @export
format_no_mollusque <- function(freq) {
  freq$NO_MOLLUSQUE <- seq_len(nrow(freq))
  return(freq)
}
