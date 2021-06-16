import json
from spacy.tokens import Span
import logging

logger = logging.getLogger("comp_logger")


class UseExistingAnnotations(object):
    name = "use_existing_annotations"

    def __init__(self, nlp, annotations, *args, **kwargs):
        with open(annotations, "r") as inptf:
            annotations = [json.loads(jline) for jline in inptf.read().splitlines()]
            self._annotations = {}
            for ann in annotations:
                self._annotations[ann["meta"]["row"]] = ann["spans"]

    def __call__(self, doc):
        logger.info("Entered call")
        if doc._.excel_row in self._annotations.keys(): # todo: note deleted excel row +1 because annotations are now also starting with 0
            logger.info(f"doc.ents at start of call = {doc.ents}")
            logger.info("compare docs")
            logger.info(f"THIS WAS LOGGED :{doc._.excel_row}, {doc.text}, {self._annotations[doc._.excel_row]}")
            lst_ents = []
            for ent in self._annotations[doc._.excel_row]:
                lst_ents.append(Span(doc, ent["token_start"], ent["token_end"] + 1, label=ent["label"]))
            doc.ents = lst_ents
            logger.info(f"lst_ents = {lst_ents}")
            logger.info(f"doc.ents at end of call = {doc.ents}")
        else:
            logger.info("doc._excel_row was not in annotations.keys")

            #todo: set an doc.extension for existing annotations.  implement check for that in post_processing (?)
        return doc