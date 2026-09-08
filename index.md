# AndesMollusque

[![R-CMD-check.yaml](https://github.com/MPO-Quebec-Science/AndesMollusque/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/MPO-Quebec-Science/AndesMollusque/actions/workflows/R-CMD-check.yaml)
[![Docs](https://github.com/MPO-Quebec-Science/AndesMollusque/actions/workflows/documentation.yml/badge.svg)](https://github.com/MPO-Quebec-Science/AndesMollusque/actions/workflows/documentation.yml)

Documentation disponible icit:
<https://mpo-quebec-science.github.io/AndesMollusque/>

Ce dépot contient des wrappers en `R` de commandes `SQL` pour extraire
des données équipe IML de la région du Québec fait avec ANDES.

# Utilisation

1.  Installer
2.  Établir un connexion a la BD ANDES
3.  Obtenir les tables en dataframs
4.  Faire une BD Access

## Installation du package

``` r

devtools::install_github("MPO-Quebec-Science/AndesMollusque")
library(ANDESMollusque)
```

### Installation des pilots SQL sur un poste Windows du MPO

Si ce n’est pas fait, il faut installer des pilotes de BD. Cèst possible
de le faire à partir du `Centre Logiciel`. - Pour connexion BD ANDES,
installer `My SQL Connecteur ODBC 8.0.22` - Pour BD MS ACCESS, rien a
faire de plus. Office 365 deja présent. - (facultatif) Pour connexion
Oracle, installer `Oracle 12 (Instant Client) x64` qui va mettre les
libraries sous `C:\Oracle\12.2.0_Instant_x64`.

## Connexion BD ANDES

``` r

# Infos connexion BD, voir section Authentification Connexion BD
url_bd <- "iml-science-4.ent.dfo-mpo.ca"
port_bd <- 25993
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
```

## Obtenir les tables en dataframes

### Tables Access

Il faut avant tout définir certains parametres, dont certain qui ne sont
PAS dans le BD ANDES. Entre autres: - `desc_serie_hist_f` -
`code_filter` (list de code strap) - `basket_class_filter` (list de code
de types de paniers andes)

Les tables Access suivent une hiearchy `proj` -\> `trait` -\> `engin`
-\> `capture` -\> `FreqLongMollusque`

``` r

################################################
# EXTERNAL INPUT
# This needs to be manually defined
# "Indice d'abondance zone 16E - pétoncle"
# "Indice d'abondance zone 16F - pétoncle"
# "Indice d'abondance zone 20 - pétoncle"
# "Indice d'abondance buccin"
desc_serie_hist_f <- "Indice d'abondance zone 16E - pétoncle"

proj <- get_projet_mollusque(andes_db_connection)
# définir dans le projet, la desc_serie_hist_f
proj <- init_cod_serie_hist(proj, desc_serie_hist_f)

validate_projet_mollusque(proj)

# choose which species we want to have using code_filter
# usefull strap codes:
cod_petoncle_island <- 4167
cod_petoncle_geant <- 4179
code_filter <- c(cod_petoncle_island, cod_petoncle_geant)

# choose which basket class have using basket_class_filter
# useful basket classes
# 0 - NA
# 1 - Vivant intact
# 2 - Claquette ouverte, int. nacré, ressort dans charnière
# 9 - Biodiversité

basket_class_filter <- c(1)

trait <- get_trait_mollusque(andes_db_connection, proj = proj)
validate_trait_mollusque(trait)

engin <- get_engin_mollusque(andes_db_connection, proj = proj)
validate_engin_mollusque(engin)

# Captures should only use the basket_class filter to select 1 - Vivant intact
capt <- get_capture_mollusque(andes_db_connection, engin, code_filter = code_filter, basket_class_filter = basket_class_filter)
validate_capture_mollusque(capt)

freq <- get_freq_long_mollusque(andes_db_connection, capt)
validate_freq_long_mollusque(freq)
```

### Biométrie Pétoncle

Pour les données de biométrie pétconle, il faut définir le nom de la
collection lié au champ `conserver pour biométrie?`. Nous allons alors
pourvoir obtenir les spécimens de la mission ou la réponse a été
`1=OUI`. Normalement, les données de biométrie sont présent pour ces
spécimens.

Utilisé ensuite la fonction
[`get_biometrie_petoncle()`](reference/get_biometrie_petoncle.md)

``` r

# BIOMETRIE PETONCLE
# select collection_name as one of the following
# "Conserver pour biométrie 16E"
# "Conserver pour biométrie 16F"
# "Conserver pour biométrie centre"
# "Conserver pour biométrie ouest"
bio <- get_biometrie_petoncle(andes_db_connection, collection_name = "Conserver pour biométrie 16E")
```

## Écrire une BD Access

Les tables en dataframes peuvent être sauvegardé en format BD Access.

``` r

# Faire une BD acces vide
file_path <- create_new_access_db()
# etablir la connexion a cette BD
access_db_write_connection <- access_db_connect(paste("./", file_path, sep = ""))
# écrire la table projet
write_projet_mollusque(proj, access_db_write_connection)
# écrire la table trait
write_trait_mollusque(trait, access_db_write_connection)
# écrire la table engin
write_engin_mollusque(engin, access_db_write_connection)
# écrire la table capture_mollusque
write_capture_mollusque(capt, access_db_write_connection)
# écrire la table freq_long_mollusque
write_freq_long_mollusque(freq, access_db_write_connection)
# fermer la connexion a la BD Access
DBI::dbDisconnect(access_db_write_connection)
```

## Authentification Connexion BD

Il faut faire une copie du fichier gabarit `exemple.Renviron` et le
nomer `.Renviron`. Par la suite il faut remplir le nom d’usagé et le mot
de passe pour pouvoir faire une connexion a la BD. Il est possible de
falloir redémarré `R` apres avoir modifier `.Renviron` car la lecture
est uniquement fait au démarage de `R`. Le fichier `.Renviron` peut être
placé au dossier home de l’usager `C:\Users\TON_NOM` (sur windows) ou
`/home/TON_NOM` (sur Linux).

# Developpement

checkout the repo and use

``` r

devtools::load_all()
devtools::document()
```

to load the library in memory without installing it.

To test the github package, detach an re-install

``` r

detach("package:ANDESMollusque", unload=TRUE)
devtools::install_github("MPO-Quebec-Science/ANDESMollusque")
library(ANDESMollusque)

# run unittests
devtools::test()

devtools::document()

Sys.setenv("_R_CHECK_ASCII_CODE_"=FALSE)
Sys.setenv("_R_CHECK_ASCII_DATA_"=FALSE)
rcmdcheck::rcmdcheck(args = "--no-manual", error_on = "error")
```
