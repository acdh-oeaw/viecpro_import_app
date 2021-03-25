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



if __name__ == '__main__':
    res = run_nlp_on_sample(nlp, "/Users/gregorpirgie/gregor/arbeit/acdh/viecpro_import/viecpro_import_app/notebook/sample_data/prefix_sample_len_20_seed_145.xlsx" )
    for el in res:
        print(el, type(el))
        idx = el._.excel_row
        print(idx, el.ents)
        compare = {}
        reslist = []
        for part in el.ents:
            print(part, part.label_)
            if part.label_ == "FUNKTION":
                print(part, "renamed = ", part._.renamed)
                if part._.renamed:
                    reslist.append(part._.renamed)
                    compare.update({idx: reslist})
                else:
                    reslist.append(part)
                    compare.update({idx:reslist})
        print(compare)
        print("\n\n")
