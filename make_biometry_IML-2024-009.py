import csv
import shutil
import pyodbc
import logging
from andes_migrate.biometrie_petoncle import BiometriePetoncle 

from andes_migrate.oracle_helper import OracleHelper
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.andes_helper import AndesHelper


logging.basicConfig(level=logging.ERROR)


andes_db = AndesHelper()
access_file = 'andes_migrate/ref_data/access_template.mdb'
ref = OracleHelper(access_file=access_file)


# INPUT VALUES
no_notification = "IML-2024-009"
zone = "20"
espece = "pétoncle"
SEQ_peche = 151

output_fname = f'./{no_notification}.mdb'
shutil.copyfile('andes_migrate/ref_data/access_template.mdb', output_fname)
con = pyodbc.connect(
    f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={output_fname};"
)
output_cur = con.cursor()



proj = ProjetMollusque(andes_db, output_cur, ref=ref, zone=zone, no_notif=no_notification, espece=espece)



for p in proj:
    print(f"Projet: ", p)
    with open('biometrie.csv','w') as fp:
        writer = csv.DictWriter(fp, lineterminator="\n", fieldnames=["id_specimen",
                                                                    "secteur",
                                                                    "trait",
                                                                    "no",
                                                                    "taille", 
                                                                    "poids_vif",
                                                                    "poids_muscle", 
                                                                    "poids_gonade",
                                                                    "poids_visceres",
                                                                    "poids_gonade",
                                                                    "sexe",
                                                                    "comment"])
        writer.writeheader()
        # this is the observation name
        collection_name='Conserver un specimen'
        biometrie = BiometriePetoncle(andes_db, proj, collection_name, output_cur)
        for b in biometrie:
            print(b)
            writer.writerow(b)

