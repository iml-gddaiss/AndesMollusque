    """ 
    andes_migrate module

    This module is meant to help map the data captured from Andes,
    towards the "Peche Sentinelle" aka "PSE' database (colloquially know as the Oracle database).

    The goal is to cut dependency in legacy (deprecated) data input tooling and move
    towards the adoption of Andes for MLI coastal surveys.

    The scallop survey data will have to continue yearly contributions to the existing historical records stored in the Peche Sentinelle database.

    Other coastal surveys (surf-clam, sea-cucumber, whelk) can start contributing to their own longitudinal datasets.

    The basic data structure is heavily influenced by the historical structure of Peche Sentinelle, since it already exists and needs to be updated with yearly data.

    The main Peche Sentinelle tables that meed to be populated are: 
        - `PROJET_MOLLUSQUE`
        - `TRAIT_MOLLUSQUE`
        - `ENGIN_MOLLUSQUE`
        - `CAPTURE_MOLUSQUE`
        - `FREQ_LONG_MOLLUSQUE`
    With optionally the 
        - `BIOMETRE_MOLLUSQUE`

    These tables are reproduced using psuedo ORM-style objects: 
        `ProjetMollusque`
        `TraitMollusque`
        `EnginMollusque`
        `CaptureMollusque`
        `FreqLongMollusque`
    where the collumn data can be accessed as class attributes.

    The relational reference tables are not duplicated here. The relations are made by assigning the correct keys which are mosly stored as integers or strings.
    To ensure relational integrity, the data are looked up by connecting to the Peche Sentinelle database and finding the keys, instead of having hard-coded values.
    In some situation, an effort to reproduce the reference lookup talbes are made in Andes, and the key is directly read form the Andes database.

    Hopefully, the reference table won't change over time (except by adding entries).

    An effort was made to have this module fonction with an Andes input as an SQlite file or a MySQL connection.
    An effort was made to have this module function with an reference from an MSAccess file or an Oracle connection.
    However, some discrepancies were spotted between MSAccess and the Oracle datbase, care must be taken.
    
    In all cases, an MS access will be created representing the data that will need to be migrated towards the Oracle server.
    """