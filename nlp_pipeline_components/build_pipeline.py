import sys, pathlib
parent_path = pathlib.Path().absolute().parent
sys.path.append(str(parent_path))
import spacy, pickle
from .components import *
from spacy.pipeline import EntityRuler
from viecpro_import_app.hardcode_cfg import run_hardcode


try:
    with open("../funktionenliste.pkl", "rb") as file:
         patterns = pickle.load(file)
except:
    with open("funktionenliste.pkl", "rb") as file:
        patterns = pickle.load(file)

def get_model(annotations=False, cfg=None):
    if not cfg:
        cfg = run_hardcode()

    #nlp = spacy.load("models/viecpro_ner_hzab_12-20/")
    nlp = spacy.load("models/viecpro_ner_hsv_5-21/")
    print("using this model: ", nlp)


    #use_existing_annotations = UseExistingAnnotations(nlp, "data/viecpro_HZAB_funktion_0.jsonl")
    use_existing_annotations = UseExistingAnnotations(nlp, "data/viecpro_HSV_0.jsonl")
    add_brackets = AddBrackets(nlp)
    rename_functions = RenameFunctions(nlp, lst_hofst=cfg.lst_hofst, df_aemter_index=cfg.df_aemter_index)
    date_prepocissions = DatePrepocissions(nlp)
    create_chunks = CreateChunks(nlp)
    extend_func_ents = ExtendFunctions(nlp)
    remove_names = RemoveNames(nlp)
    ruler = EntityRuler(nlp, overwrite_ents=False, patterns=patterns)

    if annotations:
        nlp.add_pipe(use_existing_annotations)

    #nlp.add_pipe(extend_func_ents, first=True)
    #nlp.add_pipe(ruler, first=True)
    nlp.add_pipe(add_brackets)
    nlp.add_pipe(rename_functions)
    nlp.add_pipe(remove_names)
    nlp.add_pipe(date_prepocissions)
    nlp.add_pipe(create_chunks)

    return nlp

if __name__ == "__main__":
    pass


