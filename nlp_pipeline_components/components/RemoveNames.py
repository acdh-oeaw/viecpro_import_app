
class RemoveNames(object):
    name = "remove_names"

    def __init__(self, nlp, lst_remove=["gewes.", "neu aufgenommener"], *args, **kwargs):
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
