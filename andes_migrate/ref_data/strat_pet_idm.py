from os.path import abspath, dirname
import csv
# TODO
# read from a CSV the station list

# open from shared drive
dir_path = "S:/Petoncle/Recherche/Mission/Îles"

# only open locally if local files are updated
# dir_path = abspath(dirname(__file__))

stations_cp = []
with open(f"{dir_path}/stations_CP.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=";", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_cp.append(int(r[0])) 


stations_dm = []
with open(f"{dir_path}/stations_DM_sans_400.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=";", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_dm.append(int(r[0])) 


stations_pdo = []
with open(f"{dir_path}/stations_PDO.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=";", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_pdo.append(int(r[0])) 


strat_dict = {
    "Étang-du-Nord": stations_pdo,
    "Dix-Milles": stations_dm,
    "Chaîne-de-la-Passe": stations_cp,
}
