import shutil
import pyodbc

from andes_migrate.capture_mollusque import CaptureMollusque
from andes_migrate.oracle_helper import OracleHelper
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.engin_mollusque import EnginMollusque
from andes_migrate.freq_long_mollusque import FreqLongMollusque
from andes_migrate.andes_helper import AndesHelper




andes_db = AndesHelper()
access_file = 'andes_migrate/template/access_template.mdb'
ref = OracleHelper(access_file=access_file)

proj = ProjetMollusque(andes_db, ref=ref)
proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")
proj.populate_data()

output_fname = f'./{proj.no_notification}.mdb'
shutil.copyfile('andes_migrate/template/access_template.mdb', output_fname)
con = pyodbc.connect(
    f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={output_fname};"
)

cur = con.cursor()
statement = proj.get_insert_statement()
cur.execute(statement)
# cur.commit()

trait = TraitMollusque(andes_db, proj)
# for i in range(12):
#     trait._increment_row()
print("trait: ", trait.get_ident_no_trait())
trait.populate_data()

cur = con.cursor()
statement = trait.get_insert_statement()
print(statement)
cur.execute(statement)
# cur.commit()
exit()

engin = EnginMollusque(trait)
engin.populate_data()

capture = CaptureMollusque(engin)
# print("capt: ", capture._get_current_row_pk())
capture.populate_data()

freq_long = FreqLongMollusque(capture)
freq_long.populate_data()
