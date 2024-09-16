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
#  re-do with 16E and 16F
no_notification = "IML-2024-008E"
zone = "16E"
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

        # writer.writeheader()
        # collection_name='Conserver le spécimen (Biométrie Ouest)'
        # biometrie = BiometriePetoncle(andes_db, proj, collection_name, output_cur)
        # for b in biometrie:
        #     print(b)
        #     writer.writerow(b)

        # collection_name='Conserver le spécimen (Biométrie Centre)'
        # biometrie = BiometriePetoncle(andes_db, proj, collection_name, output_cur)
        # for b in biometrie:
        #     print(b)
        #     writer.writerow(b)

    exit()
#     trait = TraitMollusque(andes_db, proj, output_cur)
#     for t in trait:
#         no_moll = 1
#         print(f"Trait: ", t)
#         engin = EnginMollusque(trait, output_cur)
#         for e in engin:
#             # print(f"Engin: ", e)
#             capture = CaptureMollusque(engin, output_cur)
#             for c in capture:
#                 # print(f"Capture: ", c)

#                 freq = FreqLongMollusque(capture, output_cur, no_moll_init=no_moll)
#                 for f in freq:
#                     # print(f"FreqLong: ", f)
#                     # if (c['COD_ESP_GEN'] == 48 or c['COD_ESP_GEN'] == 50) : 
#                     no_moll += 1


# monolithic commit if no errors are found
# output_cur.commit()





# for i in range(12):
#     trait._increment_row()
# print("trait: ", trait.get_ident_no_trait())

# statement = trait.get_insert_statement()
# cur.execute(statement)
# cur.commit()


# statement = engin.get_insert_statement()
# cur.execute(statement)
# cur.commit()

# capture.populate_data()
# statement = capture.get_insert_statement()
# cur.execute(statement)
# cur.commit()

# freq_long = FreqLongMollusque(capture)
# print("iterating...")
# for i in FreqLongMollusque(capture, output_cur):
#     print(i)
#     # statement = i.get_insert_statement()
#     # cur.execute(statement)
#     # cur.commit()
