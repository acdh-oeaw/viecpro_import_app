from spacy.tokens import Span
import logging

logger = logging.getLogger("comp_logger")

class ExtendEntities(object):
    name = "extend_entities"

    def __call__(self, doc):
        new_ents = []
        for ent in doc.ents:
            d_end = doc[ent.end - 1]
            while d_end.whitespace_ != " ":
                if len(doc) > d_end.i + 1:
                    d_end = doc[d_end.i + 1]
                else:
                    break
            lst_ids = []
            for ent2 in doc.ents:
                if ent2.start > ent.start:
                    lst_ids.extend(list(range(ent2.start, ent2.end)))
            if d_end.i != ent.end - 1 and str(d_end) in [".", "/"] and d_end.i not in lst_ids:
                s1 = Span(doc, start=ent.start, end=d_end.i + 1, label=ent.label_)
                new_ents.append(s1)
                logger.info(f"extended entity: old entity: {ent}, new entity: {s1}")
            else:
                new_ents.append(ent)
        doc.ents = new_ents
        return doc