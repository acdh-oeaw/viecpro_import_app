from spacy.tokens import Span
import logging

logger = logging.getLogger("comp_logger")

class AddBrackets(object):
    name = "add_brackets"

    def __init__(self, nlp, *args, **kwargs):
        Span.set_extension("in_brackets", default=False, force=True)

    def __call__(self, doc):
        for ent in doc.ents:
            for t in doc[ent.start:]:
                if t.text == "(":
                    break
                elif t.text == ")":
                    ent._.in_brackets = True
        return doc

