from spacy.tokens import Span
import re

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
                        ent._.date_preproc = p #todo: is this used anywhere?
                        if len(ent.text) == 4:
                            ent._.date_apis = f"{p.strip()} {ent.text.strip()}<{ent.text.strip()}-06-30>" #it is used here.
                        else:
                            if ent.text.endswith("-00-00"):
                                ent._.date_apis = f"{p.strip()} {ent.text.strip()[:-6]}<{ent.text.strip()[:-6]+'-06-30'}>" # todo: note: appended this here
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
                        ent._.date_apis = f"{ent._.date_apis.split('<')[0]}<{s2.group(1)} - {'-'.join(s2.group(1).split('-')[:-1]) + '-' + s2.group(2)}"

        return doc