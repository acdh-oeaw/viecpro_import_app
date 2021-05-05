from spacy.tokens import Doc, Token, Span
import re

class UseExistingAnnotations(object):
    name = "use_existing_annotations"

    def __init__(self, nlp, annotations, *args, **kwargs):
        with open(annotations, "r") as inptf:
            annotations = [json.loads(jline) for jline in inptf.read().splitlines()]
            self._annotations = {}
            for ann in annotations:
                self._annotations[ann["meta"]["row"]] = ann["spans"]

    def __call__(self, doc):
        if doc._.excel_row+1 in self._annotations.keys():
            print("compare docs")
            print(doc.text, self._annotations[doc._.excel_row+1])
            lst_ents = []
            for ent in self._annotations[doc._.excel_row+1]:
                lst_ents.append(Span(doc, ent["token_start"], ent["token_end"], label=ent["label"]))
            doc.ents = lst_ents
        return doc


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


class RenameFunctions(object):
    name = "rename_functions"

    def __init__(self, nlp, *args, **kwargs):
        Span.set_extension("renamed", default=None, force=True)

    def __call__(self, doc):
        for ent in doc.ents:
            if ent.label_ == "FUNKTION" and ent.text.strip()[-1] == "-" and "," in ent.text:
                txt_lst = ent.text.split(",")
                if len(txt_lst) == 2:
                    ent._.renamed = f"{txt_lst[-1][:-1]}{txt_lst[0].replace(',','').strip().lower()}"
        return doc


class RemoveNames(object):
    name = "remove_names"

    def __init__(self, nlp, lst_remove=["gewes."], *args, **kwargs):
        self._lst_remove = lst_remove

    def __call__(self, doc):
        for ent in doc.ents:
            for l in self._lst_remove:
                if ent._.renamed is not None:
                    if l in ent._.renamed:
                        ent._.renamed = ent._.renamed.replace(l, "")
                        ent._.renamed = ent._.renamed.replace("  ", " ").strip()
                else:
                    if l in ent.text:
                        ent._.renamed = ent.text.replace(l, "")
                        ent._.renamed = ent._.renamed.replace("  ", " ").strip()
        return doc


class DatePrepocissions(object):
    name = "date_prepocissions"

    def __init__(self, nlp, preproc=["ab", "bis", "bis mind.", "ab mind.", "?", "ab ?", "bis ?"], *args, **kwargs):
        Span.set_extension("date_preproc", default=None, force=True)
        Span.set_extension("date_apis", default=None, force=True)
        self._preproc = sorted(preproc, key=len)
        self._preproc_len = len(max(preproc, key=len))

    def __call__(self, doc):
        for ent in doc.ents:
            if ent.label_ == "DATUM":
                stc = int(ent.start_char) - (self._preproc_len + 2)
                if stc < 0:
                    stc = 0
                for p in self._preproc:
                    if p in doc.text[stc:ent.start_char]:
                        ent._.date_preproc = p
                        if len(ent.text) == 4:
                            ent._.date_apis = f"{p.strip()} {ent.text.strip()}<{ent.text.strip()}-06-30>"
                        else:
                            ent._.date_apis = f"{p.strip()} {ent.text.strip()}<{ent.text.strip()}>"
                if ent._.date_preproc is None:
                    ent._.date_apis = ent.text.strip()
                if "/" in ent._.date_apis:
                    s1 = re.search(r"([0-9]{4})/([0-9\-]+)", ent._.date_apis)
                    s2 = re.search(r"([0-9]{4}-[0-9]{2}-[0-9]{2})/([0-9]{2})", ent._.date_apis)
                    s3 = re.search(r"([0-9]{4}-[0-9]{2}-[0-9]{2})/([0-9]{4}-[0-9]{2}-[0-9]{2})", ent._.date_apis)
                    if s3:
                        ent._.date_apis = f"{ent._.date_apis.split('<')[0]}<{s3.group(1)} - {s3.group(2)}>"
                    elif s1:
                        ent._.date_apis = f"{ent._.date_apis.split('<')[0]}<{s1.group(1)}-06-30 - {s1.group(2)}>"
                    elif s2:
                        ent._.date_apis = f"{ent._.date_apis.split('<')[0]}<{s2.group(1)} - {'-'.join(s2.group(1).split('-')[:-1])+'-'+s2.group(2)}"

        return doc


class CreateChunks(object):
    name = "create_chunks"
    
    def __init__(self, nlp, *args, **kwargs):
        Doc.set_extension("chunks", default=[], force=True)
        self._ctrl = {"DATUM": [], "HOFSTAAT": None, "FUNKTION": []}
    
    def __call__(self, doc):
        chunks = []
        chunk = deepcopy(self._ctrl)
        lst_chunks = re.finditer(r";|u\.", doc.text)
        lst_chunks = [x.span()[0] for x in lst_chunks]
        if len(lst_chunks) > 0:
            c_idx = (0, lst_chunks[0])
        else:
            c_idx = None
        for ent in doc.ents:
            print(ent, ent)
            if c_idx is not None:
                if ent.end_char > c_idx[1]:
                    chunks.append(chunk)
                    chunk = deepcopy(self._ctrl)
                    if c_idx[0]+1 < len(lst_chunks):
                        c_idx = (c_idx[0]+1, lst_chunks[c_idx[0]+1])
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
            else:
                chunks.append(chunk)
                chunk = deepcopy(self._ctrl)
        chunks.append(chunk)
        doc._.chunks = chunks
        return doc




