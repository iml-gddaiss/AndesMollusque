# Pétoncle

## distance de trait visée par la mission

[<img src="shared_models.cruise.targeted_vessel_speed.png">](static/shared_models.cruise.targeted_vessel_speed.png)

Dans l'ancien outil de saisie, il faut saisir des métadonnées de mission tel que i) la vitesse visée ($v$), ii) la durée visée ($t$) et ii) la distance visée ($d$) comme trois variables indépendantes. Étant donnée la relation $v=d t$, il ne peut q'avoir deux variables indépendents.

La vitesse, ayant un impacte sur la performance de drague, mérite d'etre traité comme variable indépendente. Il reste donc a faire le choix arbitraire entre un temps visée ou une distance visée, selon la discrétion du chef de mission.

Andes impose comme métadonnées de mission, que la saisie de la vitesse visée, ainsi que la durée visée soit traité comme variables indépendentes, et donc distance est dérivées de ces derniers, voir figure.

Dans les situations ou la distance devait être traité somme variable indépendent au lieu de la vitesse,  il était jugé plus simple de seulement pré-calculé et saisir la durée requise pour donner la distance voulu. Cette approche étant la plus simple ne demande aucune modifications d'Andes.

N.B. Ces distances sont utilisé comme métadonnées de mission. Il est possible que la distances visées de traits différent entre les stations. Ceux-ci devront être saisie autrement.

## Déscription de mission

[<img src="shared_models_cruise.description.png">](static/shared_models_cruise.description.png)

Doit corréspondre à une entrée de la table `PROJET_MOLLUSQUE` ayant une valeur éxistante pour la colonne `DESC_SOURCE_INFO_F`

Pour les mission pétoncle, un de ces choix:
 - `Évaluation de stocks IML - Pétoncle I de M`
 - `Évaluation de stocks IML - Pétoncle Minganie`


## secteur du relevé

[<img src="shared_models_cruise.area_of_operation.png">](static/shared_models_cruise.area_of_operation.png)

Doit corréspondre à une entrée de la table `TRAIT_MOLLUSQUE` ayant une valeur éxistante pour la colonne `DESC_SECTEUR_RELEVE_F`

Pour les mission pétoncle, un de ces choix:
 - `Îles de la Madeleine`
 - `Côte-Nord`

## Résumeé de contraintes où les valeurs doivent correspondre
Évaluation de stocks IML - Pétoncle I de M
|Andes   |PSentinelle   |valeur   |
|---|---|---|
|`shared_models_cruise.description`  |`PROJET_MOLLUSQUE.DESC_SOURCE_INFO_F`   |`Évaluation de stocks IML - Pétoncle I de M` ou `Évaluation de stocks IML - Pétoncle Minganie`  |
|`shared_models_cruise.area_of_operation`  | `TRAIT_MOLLUSQUE.DESC_SECTEUR_RELEVE_F`  |`Îles de la Madeleine` ou `Côte-Nord`   |
|   |   |   |