from os.path import abspath, dirname
import csv

# only open locally if local files are updated
dir_path = abspath(dirname(__file__))

stations_centre = []
##  gisement chaine de la passe (CP)
with open(f"{dir_path}/stations_CP.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=";", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_centre.append(r[0]) 
##  gisement dix-milles (DM)
with open(f"{dir_path}/stations_DM_sans_400.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=";", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_centre.append(r[0]) 


stations_ouest = []
##  gisement pointe-a-l'ouest (PDO)

with open(f"{dir_path}/stations_PDO.csv", 'r') as fp:
    csv_data = csv.reader(fp, delimiter=";", skipinitialspace=True)
    next(csv_data, None)  # skip the headers
    for r in csv_data:
        stations_ouest.append(r[0]) 

secteur_dict = {
    "Centre": stations_centre,
    "Ouest": stations_ouest,
}
