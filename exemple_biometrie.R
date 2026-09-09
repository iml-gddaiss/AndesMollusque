library(devtools)
devtools::install_github("MPO-Quebec-Science/AndesMollusque")
library(ANDESMollusque)


url_bd <- "iml-science-4.ent.dfo-mpo.ca"
port_bd <- 25988 #IML-2025-012 Minganie petoncle

nom_bd <- "andesdb"
nom_usager <- Sys.getenv("NOM_USAGER_BD") # ecrire directe ici ou bien utiliser variable environmentale dans .Renviron
mot_de_passe <- Sys.getenv("MOT_DE_PASSE_BD") # ecrire directe ici ou bien utiliser variable environmentale dans .Renviron

# établir connexion BD (il faut être sur le réseau MPO)
andes_db_connection <- andes_db_connect(
  url_bd = url_bd,
  port_bd = port_bd,
  nom_usager = nom_usager,
  mot_de_passe = mot_de_passe,
  nom_bd = nom_bd
)

# BIOMETRIE PETONCLE
# select collection_name as one of the following
# "Conserver pour biométrie 16E"
# "Conserver pour biométrie 16F"
# "Conserver pour biométrie centre"
# "Conserver pour biométrie ouest"
collection_name <- "Conserver pour biométrie 16E"
bio <- get_biometrie_petoncle(
  andes_db_connection,
  collection_name = collection_name
)

#############
# choses a faire par l'equipe pour reformatter le dataframe:
#
# ajout zone, s_zone selon le collection_name
# convertir code sex (1,2,9)  pour M/F/I
# convertir code strap (4167, 4179)  pour I/G
# ajout colonnes  NA panier, age no_ann, prem_ann dern_ann
# ajout colonne annee
# éffacer colonnes superflus

# map collection to zone and s_zone
zone_map <- data.frame(list(
  collection = c(
    "Conserver pour biométrie 16E",
    "Conserver pour biométrie 16F",
    "Conserver pour biométrie centre",
    "Conserver pour biométrie ouest"
  ),
  zone = c(
    "16E",
    "16F",
    "20A",
    "20A"
  ),
  s_zone = c(
    "Ext", # all andes zones are Ext, Int is discontinued
    NA,
    "Centre",
    "Ouest"
  )
))

# map sex code to value
sex_map <- data.frame(list(
  sexe = c(
    0, # indéterminé
    1, # Mâle
    2, # Femelle
    9 # non-sexé
  ),
  sexe_val = c(
    "I",
    "M",
    "F",
    "I"
  )
))

# map strap code to value
strap_map <- data.frame(list(
  strap = c(
    4167, # petoncle island
    4179 # petoncle geant
  ),
  espece = c(
    "I",
    "G"
  )
))

# add zone and s_zone using zone map
bio <- left_join_preserve_order(bio, zone_map, by = c("collection"))

# add sexe_val sex map
bio <- left_join_preserve_order(bio, sex_map, by = c("sexe"))

# add espece using strap map
bio <- left_join_preserve_order(bio, strap_map, by = c("strap"))

# add NA values to needed columns
bio$panier <- NA
bio$age <- NA
bio$no_ann <- NA
bio$prem_ann <- NA
bio$dern_ann <- NA
