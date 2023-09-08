# Équipe Pétoncle

## Distance de trait visée par la mission

[<img src="shared_models.cruise.targeted_vessel_speed.png">](static/shared_models.cruise.targeted_vessel_speed.png)

Dans l'ancien outil de saisie, il faut saisir des métadonnées de mission tel que i) la vitesse visée ($v$), ii) la durée visée ($t$) et ii) la distance visée ($d$) comme trois variables indépendantes. Étant donnée la relation $v=d t$, il ne peut q'avoir deux variables indépendents (il est donc possible sur l'ancien outil de saisir des valeurs invalides).

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


## Région échantilonnée -> Secteur du relevé

[<img src="shared_models_cruise.area_of_operation.png">](static/shared_models_cruise.area_of_operation.png)

Doit corréspondre à une entrée de la table `TRAIT_MOLLUSQUE` ayant une valeur éxistante pour la colonne `DESC_SECTEUR_RELEVE_F`

Pour les mission pétoncle, un de ces choix:
 - `Îles-de-la-Madeleine`
 - `Côte-Nord`

### ATTENTION!
La table de reference de MS Access `SECTEUR_RELEVE_MOLL` et  la table de reference Peche_Sentinelle `SECTEUR_RELEVE_MOLL` n'ont pas les memes valeurs de descriptions
(`Îles de la Madeleine` versus `Îles-de-la-Madeleine`), en cas de doubte Peche_Sentinelle devrait etre considéreé comme étant la bonne.

| COD_SECTEUR_RELEVE | DESC_SECTEUR_RELEVE_F | SECTEUR_RELEVE |
|--------------------|-----------------------|----------------|
| 1                  | Côte-Nord             | C              |
| 2                  | Estuaire              | E              |
| 3                  | Gaspésie              | G              |
| 4                  | Îles-de-la-Madeleine  | I              |
| 5                  | Québec                | Q              |
| 6                  | Basse Côte-Nord       | B              |
| 7                  | Haute Côte-Nord       | H              |
| 8                  | Moyenne Côte-Nord     | M              |
| 9                  | Anticosti             | A              |

Un approche plus robuste est de faire le lien en utilisant la valeur `SECTEUR_RELEVE`, ce qui est fait par le code.
Par contre, elle est toute de même extrait de la premier lettre (apres un nettoyage d'accents et mont en majuscules) du champ Andes `Région échantilonnée`.
Donc dans les faits, seul le premier charatere compte est utilisé.


## Région échantilonnée -> Secteur du relevé


## Résumeé de contraintes où les valeurs sur Andes doivent correspondre avec Oracle
Évaluation de stocks IML - Pétoncle I de M
|Andes   | PSentinelle   | exemple   | notes |
|--------|---------------|----------|-------|
|`shared_models_cruise.description`  |`PROJET_MOLLUSQUE.DESC_SOURCE_INFO_F`   |`Évaluation de stocks IML - Pétoncle I de M` | texte verbatim|
|`shared_models_cruise.area_of_operation`  | `TRAIT_MOLLUSQUE.SECTEUR` | `Côte-Nord` | Permiere lettre seulement (devient `C`)
| `shared_models_new_station.name`  | `TRAIT_MOLLUSQUE.NO_STATION` | `N531` | Parti numérique extrait (devient `531`) |