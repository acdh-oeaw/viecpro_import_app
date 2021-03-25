import pandas as pd
try :
    from get_data_variables import *
except:
    from tests.helper_functions import get_data_variables

from spacy.tokens import Doc

def run_nlp_on_sample(pipeline, sample):
    sample = pd.read_excel(sample, engine="openpyxl", index_col=0)
    nlp = pipeline
    Doc.set_extension("excel_row", default=-1, force=True)
    results = []
    for idx, row in zip(sample.index, sample.Funktion):
        doc = nlp.make_doc(row)
        doc._.excel_row = idx
        for name, proc in nlp.pipeline:
            doc = proc(doc)
        results.append(doc)
    return results

def convert_ents_to_string(doc, label=None):
    if label:
        return [ent.text.strip() for ent in doc.ents if ent.label_ == label]
    else:
        return [ent.text.strip() for ent in doc.ents]



