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
access_file = 'andes_migrate/template/access_template.mdb'
ref = OracleHelper(access_file=access_file)


# INPUT VALUES
no_notification = "IML-2023-011"
no_releve=34
zone="20"
espece="pétoncle"

output_fname = f'./{no_notification}.mdb'
shutil.copyfile('andes_migrate/template/access_template.mdb', output_fname)
con = pyodbc.connect(
    f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={output_fname};"
)
output_cur = con.cursor()


# proj = ProjetMollusque(andes_db, output_cur, ref=ref)
# proj.init_input(zone="20", no_releve=34, no_notif=no_notification, espece="pétoncle")
proj = ProjetMollusque(andes_db, output_cur, ref=ref, zone=zone, no_releve=no_releve, no_notif=no_notification, espece=espece)

no_moll = 0
for p in proj:
    print(f"Projet: ", p)
    trait = TraitMollusque(andes_db, proj, output_cur)
    for t in trait:
        print(f"Trait: ", t)
        engin = EnginMollusque(trait, output_cur)
        for e in engin:
            print(f"Engin: ", e)
            capture = CaptureMollusque(engin, output_cur)
            for c in capture:
                print(f"Capture: ", c)
                freq = FreqLongMollusque(capture, output_cur, no_moll_init=no_moll)
                for f in freq:
                    print(f"FreqLong: ", f)
                    no_moll += 1



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
