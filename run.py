from andes_migrate.capture_mollusque import CaptureMollusque
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.engin_mollusque import EnginMollusque
from andes_migrate.freq_long_mollusque import FreqLongMollusque
from andes_migrate.andes_helper import AndesHelper


andes_db = AndesHelper()

proj = ProjetMollusque(andes_db)
proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")
# proj.populate_data()

trait = TraitMollusque(andes_db, proj)
for i in range(12):
    trait._increment_row()
print("trait: ", trait.get_ident_no_trait())
# trait.populate_data()

engin = EnginMollusque(trait)
# engin.populate_data()

capture = CaptureMollusque(engin)
print("capt: ", capture._get_current_row_pk())


# capture.populate_data()

freq_long = FreqLongMollusque(capture)
freq_long.populate_data()
