from andes_migrate.projet_mollusque import ProjetMollusque
from andes_migrate.trait_mollusque import TraitMollusque
from andes_migrate.engin_mollusque import EnginMollusque
from andes_migrate.andes_helper import AndesHelper


andes_db = AndesHelper()

proj = ProjetMollusque(andes_db)
proj.init_mission_pk("IML-2023-011")
proj.init_input(zone="20", no_releve=34, no_notif="IML-2023-011", espece="pétoncle")
trait = TraitMollusque(andes_db, proj)

engin = EnginMollusque(trait)

engin.populate_data()
