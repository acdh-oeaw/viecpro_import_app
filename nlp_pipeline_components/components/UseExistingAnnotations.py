import json
from spacy.tokens import Span


class UseExistingAnnotations(object):
    name = "use_existing_annotations"

    def __init__(self, nlp, annotations, *args, **kwargs):
        with open(annotations, "r") as inptf:
            annotations = [json.loads(jline) for jline in inptf.read().splitlines()]
            self._annotations = {}
            for ann in annotations:
                self._annotations[ann["meta"]["row"]] = ann["spans"]

    def __call__(self, doc):
        if doc._.excel_row + 1 in self._annotations.keys():
            print("compare docs")
            print(doc._.excel_row, doc.text, self._annotations[doc._.excel_row + 1]) # hier bricht was in der pipeline
            lst_ents = []
            for ent in self._annotations[doc._.excel_row + 1]:
                lst_ents.append(Span(doc, ent["token_start"], ent["token_end"] + 1, label=ent["label"]))
            doc.ents = lst_ents
        return doc