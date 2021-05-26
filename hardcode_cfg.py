import pathlib
from config import cfg
import pandas as pd
from import_src.file_processing import get_list_abbreviations, get_lst_hofst, get_aemter_index
path = pathlib.Path().absolute()

def run_hardcode():
    collection = "Import Collection"
    spacy_model = "de_VieCPro_HZAB"
    existing_annotations = str(path / "data/viecpro_HZAB_funktion_0.jsonl")
    path_df = str(path / "data/1_Hofzahlamtsbücher-HZAB-2020-12-06.xlsx")
    path_hofstaat = str(path / "data/Kürzel-Hofstaate-EX-ACC-2021-02-08.xlsx")
    path_aemter = str(path / "data/Kürzel-Ämter-ACC-EX-2021-02-08.xlsx")
    path_abbreviations = str(path / "data/EXCEL-ACCESS_Kürzel-Titel-Orden-2021-01-28.xlsx")
    compare = str(path / "tests/sample_data/comp_docs.pkl")

    cfg.df_aemter = pd.read_excel(path_aemter, header=2, engine="openpyxl")
    cfg.df_hofstaat = pd.read_excel(path_hofstaat, engine="openpyxl")
    cfg.df_abbreviations = pd.read_excel(path_abbreviations, sheet_name="Titel", header=3, engine="openpyxl")
    cfg.df = pd.read_excel(path_df, sheet_name="Original", engine="openpyxl")
    cfg.list_abbreviations = get_list_abbreviations(cfg.df_abbreviations)
    cfg.lst_hofst = get_lst_hofst(cfg.df_hofstaat)
    cfg.df_aemter_index = get_aemter_index(cfg.df_aemter)

    return cfg