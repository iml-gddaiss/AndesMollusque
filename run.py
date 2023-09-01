
import sqlite3
from project_mollusque import ProjetMollusque

# 15 -> Indice d'abondance zone 16E - pétoncle
# 16 -> Indice d'abondance zone 16F - pétoncle
# 18 -> Indice d'abondance zone 20 - pétoncle


con = sqlite3.connect("db.sqlite3")

proj = ProjetMollusque(con)
proj.init_mission_pk("IML-2023-011")
proj.init_input(zone='20',
                no_releve=34,
                no_notif='IML-2023-011'
                )
proj.get_cod_source_info()
proj.get_cod_nbpc()
proj.get_annee()
proj.get_cod_serie_hist()
proj.get_cod_type_stratif()
proj.get_date_deb_project()
proj.get_date_fin_project()
proj.get_no_notif_iml()
proj.get_chef_mission()
proj.get_seq_pecheur()
# proj.validate()
