devtools::load_all()
library(ANDESMollusque)

url_bd <- "iml-science-4.ent.dfo-mpo.ca"
port_bd <- 26981 #IML-2026-019 Minganie petoncle

nom_bd <- "andesdb"
nom_usager <- Sys.getenv("NOM_USAGER_BD")
mot_de_passe <- Sys.getenv("MOT_DE_PASSE_BD")

# établir connexion BD (il faut être sur le réseau MPO)
andes_db_connection <- andes_db_connect(
  url_bd = url_bd,
  port_bd = port_bd,
  nom_usager = nom_usager,
  mot_de_passe = mot_de_passe,
  nom_bd = nom_bd
)

# cod_petoncle_island <- 4167
# cod_petoncle_geant <- 4179
# cod_buccin_commun <- 3517
code_filter <- c(4167, 4179)

# useful basket classes
# 0 - NA
# 1 - Vivant intact
# 2 - Claquette ouverte, int. nacré, ressort dans charnière
# 9 - Biodiversité
basket_class_filter <- c(1, 2)


trait <- get_trait_mollusque_db(andes_db_connection)

capture <- get_capture_mollusque_db(
  andes_db_connection,
  code_filter = code_filter,
  basket_class_filter = basket_class_filter
)

freq <- get_freq_long_mollusque_db(andes_db_connection)
freq <- subset(
  freq,
  select = c(
    "VALEUR_LONG_MOLL",
    "IDENT_NO_TRAIT",
    "strap_code",
    "description_fra"
  )
)

res <- left_join_preserve_order(
  freq,
  capture,
  by = c("IDENT_NO_TRAIT")
)
res <- left_join_preserve_order(res, trait, by = c("IDENT_NO_TRAIT"))


# remove REM_TRAIT_MOLL that introduces line breaks which Excel cannot handle.
res <- subset(res, select = c(-REM_TRAIT_MOLL))

# write.csv(res, file = "freq_long-IML-2026-019_16E.csv", row.names = FALSE)
write.csv(res, file = "freq_long-IML-2026-019_16F.csv", row.names = FALSE)
