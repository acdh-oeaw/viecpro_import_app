from spacy.lang.de import German
from spacy.pipeline import EntityRuler
from spacy.matcher import Matcher
from import_setup import k_t_f
from viecpro_import_app.file_sets.working_sets.SETS import F, P


####### NLP RULER ######

nlp2 = German()
ruler2 = EntityRuler(nlp2)

################### NLP Setup ####################

patterns = [{"label": "FUNKTION", "pattern": str(pat)} for pat in k_t_f["Siglen in Titelspalte"][1:] if pat != "Rat"]
patterns2 = [{"label": "FUNKTION", "pattern": pat} for pat in F]
in_places = [{"label": "PLACE", "pattern": f"in {pat}"} for pat in P]
in_places2 = [{"label": "PLACE", "pattern": f"in der {pat}"} for pat in P]
in_places3 = [{"label": "PLACE", "pattern": f"nach {pat}"} for pat in P]
siglen_dic = {sigle: (fun, amt, hs) for idx, (sigle, fun, amt, hs) in
              k_t_f[["Siglen in Titelspalte", "Funktionen", "Institutionen", "Hofstaat"]][1:].iterrows()}


for el in in_places3:
    patterns2.append(el)
for el in in_places2:
    patterns2.append(el)
for el in in_places:
    patterns2.append(el)

for pat in patterns2:
    patterns.append(pat)
ruler2.add_patterns(patterns)
nlp2.add_pipe(ruler2)

spanne_l = [[{"IS_DIGIT": True, "LENGTH": 4}, {"ORTH": "-"}, {"IS_DIGIT": True, "LENGTH": 4}, ]]
spanne_s = [[{"IS_DIGIT": True, "LENGTH": 4}, {"ORTH": "-"}, {"IS_DIGIT": True, "LENGTH": 2}, ]]
single_date = [[{"IS_DIGIT": True, "LENGTH": 4}, {
    "TEXT": {"REGEX": r"\s"}}]]

matcher2 = Matcher(nlp2.vocab)

matcher2.add("DATUM_SPAN_LONG", spanne_l)
matcher2.add("DATUM_SPAN_SHORT", spanne_s)


