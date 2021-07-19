import pandas as pd
import jsonlines
import os
import pathlib
from copy import deepcopy

file_path = pathlib.Path(__file__).parent.parent.parent
directory_path = file_path.joinpath("data/access_data/tables_json/")
filenames = os.listdir(directory_path)
tables = {}

for name in filenames:
    with jsonlines.open(f"{str(directory_path)}/{name}", "r") as file:
        temp_arr = []
        for obj in file:
            temp_arr.append(obj)
        tables.update({name[:-6]:temp_arr})

per =  tables["Personen"]
hat = tables["hatAmtInne"]
amt = tables["Ämter"]
konf = tables["Konfession"]
gesch = tables["Geschlecht"]
st = tables["Stäbe"]
unt = tables["Untergebene"]

iper = {p["Nr"]:p for p in per}
istab = {s["Nr"]:s for s in st}
iamt = {a["Nr"]:a for a in amt}

per_amt = {p:[] for p in iper.keys()}
for entry in hat:
    data = deepcopy(entry)
    data.pop("Person", None)
    data.pop("Nr", None)
    if "Person" in entry.keys():
        per_amt[entry["Person"]].append(data)

per_unt = {p:[] for p in iper.keys()}
for entry in unt:
        if len(entry) > 2:
            data = deepcopy(entry)
            data.pop("Person", None)
            data.pop("Nr", None)
            if "Person" in entry.keys():
                per_unt[entry["Person"]].append(data)

temp_dic = {}
for k, v in per_unt.items():
    if len(v) > 0:
        temp_dic.update({k:v})

per_unt = temp_dic


dir_path = file_path.joinpath("data/access_data/konkordanzen/")
konk = os.listdir(dir_path)

def get_frame(path):
    df = pd.read_excel(dir_path.joinpath(path), engine="openpyxl")
    return df

konk_dic = {name[:-5]: get_frame(name) for name in konk}

k_in = konk_dic["Konkordanz-Inhaltsfelder-APIS-FWF-EXCEL_MK-MR-CS-2020-01-26"]
k_fun = konk_dic["Konkordanz_Access-APIS_Funktionen_01.07.2021"]
k_fun.columns = k_fun.iloc[0]
k_hof = konk_dic["Konkordanz_Access-APIS_Hofstäbe_12.07.2021"]
k_t_f = konk_dic["Konkordanz_Access-APIS_Titel zu Funktionen_01.07.2021"]