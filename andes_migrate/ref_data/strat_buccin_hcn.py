from os.path import abspath, dirname
import csv

# read from a CSV the station list

# only open locally if local files are updated
dir_path = abspath(dirname(__file__))

stations_for = []
with open(f"{dir_path}/stations_FOR.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=",", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_for.append(int(r[0])) 


stations_bc = []
with open(f"{dir_path}/stations_BC.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=",", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_bc.append(int(r[0])) 


stations_pao = []
with open(f"{dir_path}/stations_PAO.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=",", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_pao.append(int(r[0])) 


strat_dict = {
    "Forestville": stations_for,
    "Pointe-aux-Outardes": stations_pao,
    "Baie-Comeau": stations_bc,
}
