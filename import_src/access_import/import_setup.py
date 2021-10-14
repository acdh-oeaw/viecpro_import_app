import pandas as pd
import os
from copy import deepcopy
from viecpro_import_app.config import cfg
from viecpro_import_app.hardcode_cfg import run_hardcode

cfg.set_settings("django_settings.apis_backup")

from viecpro_import_app.import_src.create_database_objects import *
from viecpro_import_app.import_src.access_import.import_access_jsonlines import konf

create_all()


cfg = run_hardcode()

dir_path = "../data/access_data/konkordanzen/"
konk = os.listdir(dir_path)
def get_frame(path):
    df = pd.read_excel(dir_path + path, engine="openpyxl")
    return df


konk_dic = {name[:-5]: get_frame(name) for name in konk}

k_in = konk_dic["Konkordanz-Inhaltsfelder-APIS-FWF-EXCEL_MK-MR-CS-2020-01-26"]
k_fun = konk_dic["Konkordanz_Access-APIS_Funktionen_18.08.2021"]
k_hof = konk_dic["Konkordanz_Access-APIS_Hofstäbe_12.07.2021"]
k_t_f = konk_dic["Konkordanz_Access-APIS_Titel zu Funktionen_01.07.2021"]
k_t_t = konk_dic["Konkordanz_Access-APIS_Titel zu Titel_17.08.2021"]
k_o_oha = konk_dic["Konkordanz_Access_Institutionen ohne oberstes Hofamt"]

tit_zu_tit_dic = {key:apis for idx, (key, apis) in k_t_t[["Siglen in Titelspalte Access", "Titel für APIS"]][1:].iterrows()}

df_hof = deepcopy(k_hof)
df_hof.columns = [list("abcdefghij")]
df_fun = deepcopy(k_fun)
df_fun.columns = [list("abcdefg")]
inst_without_oha_list = list(k_o_oha[1:].iloc(axis=1)[0])

def resolve_amt_super(el):
    """"
    Filters only the first "Oberstes Hofamt" from column "A"
    of Konkordanz_Access_APIS_Funktionen. Column "A"
    contains multiple values sometimes and values that are not to be transferred
    like "keine Zuordnung".
    """
    data = None
    if isinstance(el, str):
        data = el.split(";")[0].split(" ")[0].strip()

        if data in ["Verwaltung", "keine", "Verwaltung,"]:
            data = None

    return data


resolve_stab_dic = {stab: (amt, hs, kombiniert) for idx, (stab, amt, hs, kombiniert) in
                    df_hof[["a", "b", "c", "d"]].iterrows()}  # todo check for nan!
resolve_amt_dic = {funct: (apis, amt, resolve_amt_super(amt_super)) for idx, (funct, apis, amt, amt_super) in
                   df_fun[["b", "d", "e", "a"]][2:].fillna("").iterrows()}

resolve_jh_dic = {hs: ("Stab " + hs.rstrip("-JH").strip(), amt, jh) for idx, (amt, hs, jh) in
                  df_hof[["b", "c", "h"]].iterrows() if isinstance(hs, str) and hs.endswith("-JH")}

resolve_star_jh_dic = {hs: (amt, jh, dauer) for idx, (amt, hs, jh, dauer) in df_hof[["b", "c", "h", "g"]].iterrows() if
                       isinstance(hs, str) and hs.endswith("*")}

konfession_dic = {entry["Nr"]: entry["Konfession"] for entry in konf}
konfession_dic[0] = "Unbekannt"

lst_oberste_hofämter = ["OMeA",
                        "OKäA",
                        "OJäA",
                        "OMaA",
                        "OFaA",
                        "OStA",
                        ]


abbr_dic_m = {
    r"E.v.": "Edler von",
    r"E. v.": "Edler von",
    r"G.v.": "Graf von",
    r"G. v.": "Graf von",
    r"Fr.v.": "Freiherr von",
    r"Fr\. v\.": "Freiherr von",
    r"\(Fr\.\) v.": "Freiherr von",
    r"F v\.": "Fürst von",
    r"Caval\." : "Cavalier",
    "Landg.v.": "Landgraf von",
    r"\(G\.\) v.": "Graf von",
    r"\(E\.\) v.": "Edler von",
    r"\(später Fr\.\) v.": "Freiherr von",
    r"\(v\.\)": "von",
    r"G\. \(später F\.\) v\.": "Graf von; Freiherr von",
    r"G.\(später F\.\) v.": "Graf von; Freiherr von",
    r"Marquis\sv.": "Marquis von",
    "F.v.": "Fürst von",
    "P.v.": "Prinz von",
    "Rat": "Rat",
}

female_titels_dic = {
    "Graf von": "Gräfin von",
    "Freiherr von": "Freifrau von", # todo: wenn nicht verheiratet, Freiin
    "Fürst von" : "Fürstin von",
    "Marquis von": "Marquise von",
    "Landgraf von": "Landgräfin von",
    "Edler von": "Edle von",
    "Prinz von": "Prinzessin von",
    "Rat": "Rätin",
    "Cavalier":"Cavalier"
}

blank_names_list = ["N.", "N.N.", ""]

wR_dic = {3558: 1774,
 2726: 1752,
 585: 1732,
 3971: 1767,
 2818: 1772,
 3954: None,
 1831: 1757,
 2817: 1766,
 2720: 1762,
 1749: 1760,
 4554: 1780}

gender_dic = {
    0: "male",
    2: "male",
    20: "male",
    22: "male",
    1: "female"
}


