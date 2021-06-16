from spacy.tokens import Span, Doc
from copy import deepcopy
import re
import logging

logger = logging.getLogger("comp_logger")

class CreateChunks(object):
    name = "create_chunks"

    def __init__(self, nlp, *args, **kwargs):
        Doc.set_extension("chunks", default=[], force=True)
        Doc.set_extension("excel_row", default=-1, force=True)
        self._ctrl = {"DATUM": [], "HOFSTAAT": None, "FUNKTION": [], "AMT": None}

    def __call__(self, doc):
        chunks = []
        chunk = deepcopy(self._ctrl)
        lst_chunks = re.finditer(r";", doc.text) # todo: OLD u. sometimes part of function name, exclude this case to not split then
        lst_chunks = [x.span()[0] for x in lst_chunks]
        if len(lst_chunks) > 0:
            c_idx = (0, lst_chunks[0])
        else:
            c_idx = None
        for ent in doc.ents:
            if c_idx is not None:
                if ent.end_char > c_idx[1]:
                    chunks.append(chunk)
                    chunk = deepcopy(self._ctrl)
                    if c_idx[0] + 1 < len(lst_chunks):
                        c_idx = (c_idx[0] + 1, lst_chunks[c_idx[0] + 1])
                    else:
                        c_idx = None
            if ent.label_ == "DATUM" and len(chunk["DATUM"]) < 3:
                d = ent._.date_apis.strip()
                if re.search("[0-9]", d):
                    m_years = re.match(r"([0-9]{4})-([0-9]{4})", d)
                    if m_years:
                        chunk["DATUM"].extend([m_years.group(1), m_years.group(2)])
                    elif re.search(r"<[0-9\-]*/", d):
                        chunk["DATUM"].append(re.sub(r"<.+>", "", d))
                    else:
                        chunk["DATUM"].append(d)
            elif ent.label_ == "FUNKTION":
                if ent._.renamed is None:
                    chunk["FUNKTION"].append(ent.text.strip())
                else:
                    chunk["FUNKTION"].append(ent._.renamed.strip())
            elif ent.label_ == "HOFSTAAT" and chunk["HOFSTAAT"] is None:
                chunk["HOFSTAAT"] = ent.text
                logger.info(f"Chunk HOFSTAAT = {chunk['HOFSTAAT']}")
            elif ent.label_ == "AMT" and chunk["AMT"] is None:
                chunk["AMT"] = ent.text
                logger.info(f"Chunk AMT = {chunk['AMT']}")

            else:
                chunks.append(chunk)
                logger.info(f"In else clause of chunks, chunk = {chunk}")
                chunk = deepcopy(self._ctrl) # todo why? this overwrites the chunks, or?
        chunks.append(chunk)
        doc._.chunks = chunks
        logger.info(f"this was finally written, chunk({len(chunks)}) = {chunks}")
        for pos, c in enumerate(chunks):
            logger.info(f"\t{pos}: chunk: {c}")

        return doc