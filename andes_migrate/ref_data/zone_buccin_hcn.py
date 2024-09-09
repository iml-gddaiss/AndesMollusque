from os.path import abspath, dirname
import csv

# read from a CSV the station list

# only open locally if local files are updated
dir_path = abspath(dirname(__file__))

stations_01 = []
with open(f"{dir_path}/stations_buccin01.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=",", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_01.append(int(r[0])) 


stations_02 = []
with open(f"{dir_path}/stations_buccin02.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=",", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_02.append(int(r[0])) 


zone_dict = {
    "1": stations_01,
    "2": stations_02,
}
