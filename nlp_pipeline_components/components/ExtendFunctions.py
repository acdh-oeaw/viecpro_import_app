import re
from spacy.tokens import Span

class ExtendFunctions(object):
    name = "extend_func_ents"

    def __init__(self, nlp, *args, **kwargs):
        pass

    def __call__(self, doc):
        # old_ents = doc.ents #[func for func in doc.ents if func.label_ == "FUNKTION"]
        new_ents = []
        for ent in doc.ents:
            if ent.label_ == "FUNKTION":
                start = ent.start
                end = ent.end
                next2 = doc[end:end + 2]
                print(f"**{next2}**")
                if re.match(r",\s[a-zA-ZäöüÄÖÜ]{1,}-", str(next2)):
                    print(str(ent), "-----> next =", next2)
                    extended = Span(doc, start, end + 2, "FUNKTION")
                    new_ents.append(extended)
                    print("New ent ------> = ", extended)
                # doc.ents.replace(fun, extended)
                else:
                    new_ents.append(ent)
            else:
                new_ents.append(ent)
        doc.ents = tuple(new_ents)
        return doc