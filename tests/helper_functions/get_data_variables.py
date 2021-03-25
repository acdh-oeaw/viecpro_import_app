import pandas as pd
import spacy, pathlib, pickle

try:
    from pipeline.file_processing import get_list_abbreviations, get_lst_hofst, get_aemter_index
except:
    from viecpro_import_app.pipeline.file_processing import get_list_abbreviations, get_lst_hofst, get_aemter_index

if __file__:
    path = pathlib.Path(__file__).parent.parent.parent
else:
    path = pathlib.Path().absolute()


collection = "Import Collection"
spacy_model = "de_VieCPro_HZAB"
existing_annotations = str(path / "data/viecpro_HZAB_funktion_0.jsonl")
path_df = str(path / "data/1_Hofzahlamtsbücher-HZAB-2020-12-06.xlsx")
path_hofstaat = str(path / "data/Kürzel-Hofstaate-EX-ACC-2021-02-08.xlsx")
path_aemter = str(path / "data/Kürzel-Ämter-ACC-EX-2021-02-08.xlsx")
path_abbreviations = str(path / "data/EXCEL-ACCESS_Kürzel-Titel-Orden-2021-01-28.xlsx")
compare = str(path / "tests/sample_data/comp_docs.pkl")

df_aemter = pd.read_excel(path_aemter, header=2, engine="openpyxl")
df_hofstaat = pd.read_excel(path_hofstaat, engine="openpyxl")
df_abbreviations = pd.read_excel(path_abbreviations, sheet_name="Titel", header=3, engine="openpyxl")
df = pd.read_excel(path_df, sheet_name="Original", engine="openpyxl")
list_abbreviations = get_list_abbreviations(df_abbreviations)
lst_hofst = get_lst_hofst(df_hofstaat)
df_aemter_index = get_aemter_index(df_aemter)

with open(compare, "rb") as file:
    comp_docs_dic = pickle.load(file)

nlp = spacy.load(spacy_model,
                 annotations=existing_annotations,
                 lst_hofst=lst_hofst,
                 df_aemter_index=df_aemter_index,
                 )

