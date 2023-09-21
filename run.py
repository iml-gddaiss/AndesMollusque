from andes_migrate.capture_mollusque import CaptureMollusque
from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.engin_mollusque import EnginMollusque
from andes_migrate.andes_helper import AndesHelper


andes_db = AndesHelper()

proj = ProjetMollusque(andes_db)
proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")
# proj.populate_data()

trait = TraitMollusque(andes_db, proj)
# trait.populate_data()

engin = EnginMollusque(trait)
# engin.populate_data()

capture = CaptureMollusque(engin)
capture.populate_data()