# Combiner deux BD MS Access
FAIRE UNE SAUVEGARDE DES DEUX BDS AVANT DE COMMENCER!

Il vaut mieux commencer avec une copie locale de la BD maitre, i.e., faire une copie `historique.mdb` -> `historique_essaie.mdb` et seulement modifier `historique_essaie.mdb`.

On peut simplement ouvrire la BD *source* (e.g., `iml-2024-008.mdb`) et la BD *destination* (e.g., `historique-essaie.mdb`) et faire un copie/coller avec la contenu entier des tables (de *source* vers *destination*). 

astuce:
`ctrl+a` (tout selectioner)
`ctrl+c` (copie)
`ctrl+v` (coller)

## Pétoncle
Copier des tables suivantes (dans cette ordre) :
1. `PROJET_MOLLUSQUE` (normalement une seul ligne)
2. `TRAIT_MOLLUSQUE` (tout)
3. `ENGIN_MOLLUSQUE` (tout)
4. `CAPTURE_MOLLUSQUE` (tout)
5. `FREQ_LONG_MOLLUSQUE` (tout)

## Buccin
Même chose que pétoncle, avec deux tables additionelles (pour le poids des oeufs)

6. `BIOMETRIE_MOLLUSQUE`
7. `POIDS_BIOMETRIE`


