import shutil
import pyodbc
import logging 

from andes_migrate.capture_mollusque import CaptureMollusque
from andes_migrate.oracle_helper import OracleHelper
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.engin_mollusque import EnginMollusque
from andes_migrate.freq_long_mollusque import FreqLongMollusque
from andes_migrate.andes_helper import AndesHelper


logging.basicConfig(level=logging.ERROR)


andes_db = AndesHelper()
access_file = 'andes_migrate/ref_data/access_template.mdb'
ref = OracleHelper(access_file=access_file)

placopecten_magellanicus = 156972
chlamys_islandica = 140692
buccinum_undatum = 138878

na_size_class = 0
claquette_ouverte=2
vivant_intact_size_class = 1
vivant_brisé_size_class = 2
oeufs_Buccin_size_class = 3
predateur_size_class = 4
biodiversite_size_class = 9


# INPUT VALUES
no_notification = "IML-2024-009"
zone = "20"
espece = "pétoncle"
SEQ_peche = 151
aphia_id_filter = [placopecten_magellanicus, chlamys_islandica]
size_class_filter = [vivant_intact_size_class, claquette_ouverte]


output_fname = f'./{no_notification}.mdb'
shutil.copyfile('andes_migrate/ref_data/access_template.mdb', output_fname)
con = pyodbc.connect(
    f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={output_fname};"
)
output_cur = con.cursor()


# proj = ProjetMollusque(andes_db, output_cur, ref=ref)
# proj.init_input(zone="20", no_releve=34, no_notif=no_notification, espece="pétoncle")
proj = ProjetMollusque(andes_db, output_cur, ref=ref, zone=zone, no_notif=no_notification, espece=espece)


for p in proj:
    print(f"Projet: ", p)
    trait = TraitMollusque(andes_db, proj, output_cur)
    trait_n =0
    for t in trait:
        trait_n = trait_n +1

        no_moll = 1
        print(f"Trait: ", t)
        if trait_n>0:

            engin = EnginMollusque(trait, output_cur)
            for e in engin:
                # print(f"Engin: ", e)
                capture = CaptureMollusque(engin, output_cur)
                for c in capture:
                    print(f"Capture: ", c)

                    freq = FreqLongMollusque(capture, output_cur, no_moll_init=no_moll)
                    for f in freq:
                        # print(f"FreqLong: ", f)
                        # if (c['COD_ESP_GEN'] == 48 or c['COD_ESP_GEN'] == 50) : 
                        no_moll += 1


# monolithic commit if no errors are found
output_cur.commit()

