import shutil
import pyodbc
import logging 

from andes_migrate.capture_mollusque import CaptureMollusque
from andes_migrate.oracle_helper import OracleHelper
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.engin_mollusque import EnginMollusque
from andes_migrate.freq_long_mollusque import FreqLongMollusque
from andes_migrate.biometrie_mollusque import BiometrieMollusque
from andes_migrate.poids_biometrie import PoidsBiometrie

from andes_migrate.andes_helper import AndesHelper


logging.basicConfig(level=logging.ERROR)


andes_db = AndesHelper()
access_file = 'andes_migrate/ref_data/access_template.mdb'
ref = OracleHelper(access_file=access_file)

# HACK! only choose aphia IDs for commercial target species
placopecten_magellanicus = 156972
chlamys_islandica = 140692
buccinum_undatum = 138878

na_size_class = 0
claquette_ouverte=2
vivant_intact_size_class = 1
vivant_brisé_size_class = 2
oeufs_buccin_size_class = 3
predateur_size_class = 4
biodiversite_size_class = 9

# INPUT VALUES
no_notification = "IML-2024-022"
zone = None
espece = "buccin"
SEQ_peche = 151
aphia_id_filter = [buccinum_undatum]
size_class_filter = [vivant_intact_size_class, vivant_brisé_size_class]

ANDES_HOST = 'iml-science-1.ent.dfo-mpo.ca'
ANDES_PORT = 24988
ANDES_DB_NAME = 'andesdb'
ANDES_DB_USERNAME = 'lecture_seulement'
ANDES_DB_USERPASS = 'NOPE NO PASSWORD HERE'


output_fname = f'./{no_notification}.mdb'
shutil.copyfile('andes_migrate/ref_data/access_template.mdb', output_fname)
con = pyodbc.connect(
    f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={output_fname};"
)
output_cur = con.cursor()


proj = ProjetMollusque(andes_db, output_cur, ref=ref, zone=zone, no_notif=no_notification, espece=espece)

for p in proj:
    print(f"Projet: ", p)
    proj.validate()
    trait = TraitMollusque(andes_db, proj, output_cur)
    trait_n =0
    for t in trait:
        trait_n = trait_n +1

        no_moll_freq_long = 1
        no_moll_biometrie = 1

        if trait_n>0:
            print(f"Trait: ", t)

            engin = EnginMollusque(trait, output_cur)
            for e in engin:
                # print(f"Engin: ", e)
                # capture for freq-long
                capture = CaptureMollusque(engin, output_cur,
                                           aphia_id_filter=aphia_id_filter,
                                           size_class_filter=[vivant_intact_size_class, vivant_brisé_size_class])
                for c in capture:
                    # print(f"Capture: ", c)

                    freq = FreqLongMollusque(capture, output_cur, no_moll_init=no_moll_freq_long)
                    for f in freq:
                        # print(f"FreqLong: ", f)
                        # if (c['COD_ESP_GEN'] == 48 or c['COD_ESP_GEN'] == 50) : 
                        no_moll_freq_long += 1

                # capture for biometrie (whelk eggs)
                capture = CaptureMollusque(engin, output_cur,
                                           aphia_id_filter=aphia_id_filter,
                                           size_class_filter=[oeufs_buccin_size_class])
                for c in capture:
                    biometrie = BiometrieMollusque(capture, output_cur, no_moll_init=no_moll_biometrie)
                    for b in biometrie:
                        print(f"biometrie: ", b)
                        no_moll_biometrie += 1

                        poids = PoidsBiometrie(biometrie, output_cur)
                        for p in poids:
                            pass


# monolithic commit if no errors are found
output_cur.commit()


