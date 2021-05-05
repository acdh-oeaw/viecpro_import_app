from spacy.tokens import Span
import re

class RenameFunctions(object):
    name = "rename_functions"

    def __init__(self, nlp, lst_hofst=None, df_aemter_index=None, *args,  **kwargs):
        Span.set_extension("renamed", default=None, force=True)
        self._lst_remove_str = ["!!!!", "20 jähr.", ]
        self._lst_remove_patterns = ["^[A-Z\-]+$", "^[a-z\-]+$", "^[0-9]{1,3}$", "[\[\]]+", "^!+$"]
        nlp.get_pipe("ner").add_label("AMT")
        self._df_aemter_index =df_aemter_index
        self._lst_hofst = lst_hofst


    def __call__(self, doc):
        new_ents = []
        for ent in doc.ents:
            test = True
            if str(ent) in self._lst_remove_str:
                test = False
            for p in self._lst_remove_patterns:
                if re.match(p, str(ent).strip()):
                    test = False
            if len(str(ent)) < 4 and not (
                    str(ent).strip() in self._lst_hofst.keys() or str(ent).strip() in self._df_aemter_index.keys()):
                test = False
            else:
                test = True
            if test:
                new_ents.append(ent)
            else:
                print(f"removing {ent} from ents")
        doc.ents = new_ents
        new_ents = []
        for ent in doc.ents:
            if ent.label_ == "FUNKTION" and ent.text.strip()[-1] == "-" and "," in ent.text:
                txt_lst = ent.text.split(",")
                if len(txt_lst) == 2:
                    ent._.renamed = f"{txt_lst[-1][:-1]}{txt_lst[0].replace(',', '').strip().lower()}"
                new_ents.append(ent)
            elif ent.label_ == "HOFSTAAT" and ent.text not in self._lst_hofst.keys():
                if ent.text in self._df_aemter_index.keys():
                    s1 = Span(doc, start=ent.start, end=ent.end, label='AMT')
                    new_ents.append(s1)
                else:
                    new_ents.append(ent)
            else:
                new_ents.append(ent)
        doc.ents = new_ents

        return doc