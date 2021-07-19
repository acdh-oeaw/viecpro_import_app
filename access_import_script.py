import pandas as pd
import os
import jsonlines
from copy import deepcopy
import pandas as pd
import django
import re
from import_src.access_import.create_dataframes import istab, iamt, iper, per_amt, per_unt, konf, gesch, per
from viecpro_import_app.config import cfg
import spacy
from spacy.tokens.doc import Doc
from spacy.lang.de import German
from spacy.pipeline import EntityRuler
from spacy.matcher import Matcher
from file_sets.working_sets.SETS import F, P
cfg.set_settings("django_settings.hsv_settings")

from viecpro_import_app.import_src.create_database_objects import *
from apis_core.apis_entities.models import Person, Institution, Place
from apis_core.apis_vocabularies.models import Title, InstitutionType, PersonInstitutionRelation, PersonPlaceRelation, \
    PlaceType
from apis_core.apis_metainfo.models import Source, Text
from apis_core.apis_relations.models import PersonInstitution, InstitutionInstitution, PersonPlace
from apis_core.apis_labels.models import Label
from apis_core.apis_vocabularies.models import TextType, LabelType
dir_path = "../data/access_data/konkordanzen/"
konk = os.listdir(dir_path)

create_all()

def get_frame(path):
    df = pd.read_excel(dir_path+path, engine="openpyxl")
    return df

konk_dic = {name[:-5]: get_frame(name) for name in konk}

k_in = konk_dic["Konkordanz-Inhaltsfelder-APIS-FWF-EXCEL_MK-MR-CS-2020-01-26"]
k_fun = konk_dic["Konkordanz_Access-APIS_Funktionen_01.07.2021"]
k_hof = konk_dic["Konkordanz_Access-APIS_Hofstäbe_12.07.2021"]
k_t_f = konk_dic["Konkordanz_Access-APIS_Titel zu Funktionen_01.07.2021"]

gender_dic = {
    0 : "unknown", # todo: Wie unbekanntes Geschlecht angeben? einfach überspringen?
    2 : "male",
    20: "male",
    22: "male",
    1 : "female"
}

df_hof = deepcopy(k_hof)
df_hof.columns = [list("abcdefghij")]

df_fun = deepcopy(k_fun)
df_fun.columns = [list("abcdefg")]

resolve_stab_dic = {stab: (amt, hs, kombiniert) for idx, (stab, amt, hs, kombiniert) in df_hof[["a", "b", "c", "d"]].iterrows()} # todo check for nan!
resolve_amt_dic = {funct: (apis, amt) for idx, (funct, apis, amt) in df_fun[["b", "d", "e"]][2:].fillna("").iterrows()}
resolve_jh_dic = {hs:("Stab "+hs.rstrip("-JH").strip(),amt, jh ) for idx, (amt, hs, jh) in df_hof[["b","c","h"]].iterrows() if isinstance(hs, str) and hs.endswith("-JH")}
resolve_star_jh_dic = {hs:(amt, jh, dauer) for idx, (amt, hs, jh, dauer) in df_hof[["b","c","h","g"]].iterrows() if isinstance(hs, str) and hs.endswith("*")}

konfession_dic = {entry["Nr"] : entry["Konfession"] for entry in konf}
konfession_dic[0] = "Unbekannt" #todo: Sollen wir das so machen?

abbr_dic_m = {
    r"E.v." :"Edler von",
    r"E. v." :"Edler von",
    r"G.v.":"Graf von",
    r"G. v.":"Graf von",
    r"Fr.v.": "Freiherr von",
    r"Fr\. v\.":"Freiherr von",
    r"\(Fr\.\) v.": "Freiherr von",
    r"F v\.": "Freiherr von",
    #"Caval." : "?",
    "Landg.v." : "Landgraf von",
    r"\(G\.\) v.": "Graf von",
    r"\(E\.\) v.": "Edler von",
    r"\(später Fr\.\) v.": "Freiherr von",
    #r"\(v\.\)": "von",
    r"G\. \(später F\.\) v\.": "Graf von; Freiherr von",
    r"G.\(später F\.\) v.":"Graf von; Freiherr von",
    r"Marquis\sv.": "Marquis von",
    "F.v." : "Freiherr von",
    "P.v.": "Prinz von",
}

female_titels_dic = {
    "Graf von": "Gräfin von",
    "Freiherr von" :" Freiherrin von",
    "Marquis von": "Marquise von",
    "Landgraf von":"Landgräfin von",
    "Edler von": "Edle von",
    "Prinz von":"Prinzessin von",
}


def process_vorname(r_vor, gender, ält_jüng, titels):
    # todo look for "vacat" and resolve N. names

    r_clean = r_vor
    vor_alts = []

    for el in abbr_dic_m.keys():
        if re.search(el, r_clean):
            match = re.search(el, r_clean).group(0)
            tit_res = abbr_dic_m[el]
            if tit_res == "Graf von; Freiherr von":
                titels.append("Graf von")
                titels.append("Freiherr von")
            else:
                titels.append(tit_res)
            r_clean = r_clean.replace(match, "")

    r_clean = r_clean.strip()



    if "(" in r_clean:
        match = re.search(r"\(.*\)", r_clean)
        b_start = match.span()[0]
        b_end = match.span()[1]
        brackets = match.group(0).rstrip(")").lstrip("(")
        # todo: filter nicht-namen raus
        brackets_list = brackets.split(",")
        print("IN BRACKETS:  ", r_clean, "STRIPPED:  ", brackets)
        for el in brackets_list:
            vor_alts.append(el)
        r_clean = r_clean[:b_start] + r_clean[b_end:]
        r_clean = r_clean.replace("()", "")

    vorname = r_clean.strip()
    if "v." in vorname:
        vorname = vorname.replace("v.", "von")
        print(f"REPLACED: ---_> r_vor: {r_vor}, vorname: {vorname}, vor_alts: {vor_alts}, titels: {titels}")

    if gender == "female":
        tit_temp = []
        for tit in titels:
            try:
                tit_temp.append(female_titels_dic[tit])
            except KeyError as k:
                tit_temp.append(tit)
                print(f"handled exception {k} for key {tit}")

        titels = tit_temp

    if ält_jüng:
        vorname = vorname + " " + ält_jüng

    print(f"r_vor: {r_vor}, vorname: {vorname}, vor_alts: {vor_alts}, titels: {titels}")

    if r_vor == "(Johann) Karl (Carlo)":
        vorname = "Karl"
        vor_alts = ["Karl", "Carlo"]

    return vorname, vor_alts, titels


def process_geschlecht(r_ges):
    gender = gender_dic[r_ges]

    return gender


doubletten = ["(I)", "(II)", "(III)", "(IV)", "(V)", "(VI)", "(VII)"]


def process_nachname(r_nach, gender):
    # todo look for "vacat" and resolve N. names

    nachname, nach_alts, nach_verheiratet, ält_jüng = None, [], [], None
    r_clean = deepcopy(r_nach)
    stop = False
    titels = []


    def replace_ältere_jüngere(name, ält_jüng=ält_jüng):

        if "d.J." in name:
            ält_jüng = "d.J."


        elif "d.Ä." in name:
            ält_jüng = "d.Ä."

        name = name.replace("d.J.", "").replace("d.Ä.", "").strip()

        return name, ält_jüng

    for el in doubletten:
        if el in r_nach:
            stop = True
            nachname = r_nach
            if "(Keil)" in nachname:
                nachname = nachname.replace("(Keil)", "")
                nach_alts.append("Keil")

    if not stop:
        if "(" in r_clean:
            match =  re.search(r"\(.*\)", r_clean)
            brackets = match.group(0).rstrip(")").lstrip("(")
            b_start = match.span()[0]
            b_end = match.span()[1]
            brackets_list = brackets.split(",")
            for el in brackets_list:
                nach_alts.append(el)
            r_clean = r_clean[:b_start]+r_clean[b_end:]
            r_clean = r_clean.replace("()", "")

        nachname = r_clean.strip()

        if "geb." in nachname:
            # print("GEB:", gender, r_nach)

            if "," in nachname:
                splitted = nachname.split(",")

            else:
                splitted = nachname.split("geb.")

            verheirated = splitted[0]
            birthname = splitted[1].strip().lstrip("geb.")
            nach_verheiratet = verheirated
            nachname = birthname

            for el in abbr_dic_m.keys():
                # print(el)
                if re.search(el, nachname):
                    # print(el, nachname)
                    match = re.search(el, nachname).group(0)
                    tit_res = female_titels_dic[abbr_dic_m[el]]
                    titels.append(tit_res)
                    nachname = nachname.replace(match, "")
                    continue

            nach_verheiratet = nach_verheiratet.replace("v.", "von").replace("u.", "und").strip()
            nach_verheiratet, ält_jüng = replace_ältere_jüngere(nach_verheiratet)

        nachname = nachname.replace("v.", "von").replace("u.", "und").strip()
        nachname, ält_jüng = replace_ältere_jüngere(nachname)

        if "geb." in r_nach:
            # print(f"Orig: {r_nach}, nachname: |{nachname}| nach_alts: |{nach_alts}| nach_verheiratet: |{nach_verheiratet}| ält_jüng: {ält_jüng}, titels: {titels}")
            pass

    #       while nachname.startswith(" "):
    #          nachname = nachname.lstrip()

    # if ält_jüng:
    print(
        f"Orig: {r_nach}, nachname: |{nachname}| nach_alts: |{nach_alts}| nach_verheiratet: |{nach_verheiratet}| ält_jüng: {ält_jüng}")
    return nachname, nach_alts, nach_verheiratet, ält_jüng, titels


def check_key(key, dic):
    res = False
    if key in dic:
        res = True

    return res

def person_create_person_labels(per_obj, vorname, labels_fam, labels_fam_hzt):

    print(labels_fam)
    if not type(labels_fam) == list:
        labels_fam = list(labels_fam)

    if len(vorname) > 0:
        for v in vorname:
            Label.objects.create(
                label=v, label_type=lt_vorname_alternativ, temp_entity=per_obj
            )

    if len(labels_fam) > 0:
        for f in labels_fam:
            Label.objects.create(
                label=f, label_type=lt_nachname_alternativ, temp_entity=per_obj
            )


    if labels_fam_hzt: # todo check if there are mulitple fam hzt. in some cases !
        f = labels_fam_hzt
        Label.objects.create(
            label=f, label_type=lt_nachname_verheiratet, temp_entity=per_obj
        )




####### TITEL ######

nlp = German()
#tokenizer = nlp.tokenizer
#nlp.add_pipe(tokenizer)
ruler = EntityRuler(nlp)
patterns = [{"label":"FUNKTION", "pattern":str(pat)} for pat in k_t_f["Siglen in Titelspalte"][1:]]
patterns2 = [{"label":"FUNKTION", "pattern":pat} for pat in F]
in_places = [{"label":"PLACE", "pattern":f"in {pat}"} for pat in P]
in_places2 = [{"label":"PLACE", "pattern":f"in der {pat}"} for pat in P]
in_places3 = [{"label":"PLACE", "pattern":f"nach {pat}"} for pat in P]


for el in in_places3:
    patterns2.append(el)
for el in in_places2:
    patterns2.append(el)
for el in in_places:
    patterns2.append(el)

for pat in patterns2:
    patterns.append(pat)
ruler.add_patterns(patterns)
nlp.add_pipe(ruler)

spanne_l = [[{"IS_DIGIT":True, "LENGTH":4}, {"ORTH":"-"},{"IS_DIGIT":True, "LENGTH":4},]]
spanne_s = [[{"IS_DIGIT":True, "LENGTH":4}, {"ORTH":"-"},{"IS_DIGIT":True, "LENGTH":2},]]
single_date = [[{"IS_DIGIT":True, "LENGTH":4}, {"TEXT":{"REGEX":r"\s"}}]]#, {"IS_SPACE":True}]]#, {"ORTH":"-", "OP":"!", "LENGTH":1}]]#,{"IS_SPACE":True}]]

matcher = Matcher(nlp.vocab)

matcher.add("DATUM_SPAN_LONG", spanne_l)
matcher.add("DATUM_SPAN_SHORT", spanne_s)


siglen_dic = {sigle:(fun, amt, hs) for idx, (sigle, fun, amt, hs) in k_t_f[["Siglen in Titelspalte", "Funktionen",  "Institutionen","Hofstaat"]][1:].iterrows()}



def get_date(doc: object):
    start = None
    end = None
    matches = matcher(doc)
    if matches:

        for m in matches:
            match_id = m[0]
            string_id = nlp.vocab.strings[match_id]
            if string_id == "DATUM_SPAN_SHORT":
                date = str(doc[m[1]:m[2]])
                temp = date
                start = f"{temp}<{date[:4]}-06-30>"
                end = f"{date[:2]}{date[-2:]}<{date[:2]}{date[-2:]}-06-30>"

            else:
                date = str(doc[m[1]:m[2]])
                start = f"{date[:4]}<{date[:4]}-06-30>"
                end = f"{date[-4:]}<{date[-4:]}-06-30>"

    else:
        date = re.search(r"\d{4}", str(doc))
        if date:
            date = date.group(0)
            start = f"{date}<{date}-06-30>"

    return start, end



def process_title(r_tit: str, per_obj):
    r_tit = r_tit.replace("\n", " ")
    chunks = r_tit.split(",")

    for c in chunks:
        doc = nlp(c)
        start, end = get_date(doc)
        death_match = re.search(r"(starb)|(gest\.)", c)
        birth_match = re.search(r"(geb\.)", c)
        death, birth = False, False

        if death_match:
            if start:
                per_obj.end_date_written = start
                per_obj.save()
                print("FOUND DEATH DATE in :", c, "as :", death_match.group(0))
                death = True
            elif end:
                breakpoint()

        elif birth_match:
            if start:
                per_obj.start_date_written = start
                per_obj.save()
                print("FOUND Birth DATE in :", c, "as :", birth_match.group(0))
                birth = True

            elif end:
                breakpoint()

        for e in doc.ents:
            if e.label_ == "PLACE":
                rel_place = (str(e), start, end)
                write_place_relation(per_obj, rel_place, death, birth)
                print("REL_PLACE", rel_place)

            elif check_key(str(e), siglen_dic):
                func, amt, hs = siglen_dic[str(e)]
                relation = (func, amt, hs, None, start, end)
                write_relation(per_obj, relation)
                print(f"found: {e} --> resolved --> func: {func}, amt: {amt}, hs: {hs}")

            elif e.label_ == "FUNKTION" and not check_key(str(e), siglen_dic):
                rel_func = (str(e), None, None, "ACCESS TEST (TEST FROM TITLE)", start, end)  # todo: maybe ad another signal her
                write_relation(per_obj, rel_func)
                print("REL FUNC:", rel_func)


month_dic = {
    "januar": 1,
    "jänner":1,
    "jan." : 1,
    "feb" :2,
    "februar": 2,
    "feber":2,
    "merz":3,
    "märz": 3,
    "april":4,
    "apr":4,
    "dez":12,
    "dezember":12,
    "november":11,
    "Oktober":10,
    "Okt.":10,
}

def process_birth_or_death(per_obj, res, source, birth=None, death=None ):
    print(res, "source: ", source, "birth: ", birth,"death: ", death)

    nlp = German()
    ruler = EntityRuler(nlp)
    patterns = []
    keywords = ["vor", "ca", "ca.", "Anfang", "Ende", ]
    kw_patterns = [{"label":"KEYWORD", "pattern":pat} for pat in keywords]

    in_places = [{"label": "PLACE", "pattern": f"in {pat}"} for pat in P]
    in_places2 = [{"label": "PLACE", "pattern": f"in der {pat}"} for pat in P]

    for el in in_places2:
        patterns.append(el)
    for el in in_places:
        patterns.append(el)
    for el in kw_patterns:
        patterns.append(el)

    ruler.add_patterns(patterns)
    nlp.add_pipe(ruler)

    longdate = re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", res.replace(" ","").replace(" ", "")) #todo: check if this works. Want to get rid of the additional whitespaces in the dates"
    shortdate = re.search(r"\d{4}", res)

    kw = None
    place = None
    date_apis = None

    if longdate:
        m = longdate.group(0)
        temp = m.split(".")
        date = []
        for el in temp[::-1]:
            if len(el) == 1:
                new = "0"+el
                date.append(new)
            else:
                date.append(el)
        date = "-".join(date)
        date_apis = f"{date}<{date}>"

        print(m, "date_apis: ", date_apis)

        doc = nlp(res)
        for ent in doc.ents:
            if ent.label_ == "PLACE":
                place = str(ent).lstrip("in").strip()
            if ent.label_ == "KEYWORD":
                kw = str(ent)

    elif shortdate:
        date = shortdate.group(0)
        print(shortdate.group(0))
        date_apis = f"{date}<{date}-06-30>"

        doc = nlp(res)
        for ent in doc.ents:
            if ent.label_ == "PLACE":
                place = str(ent).lstrip("in").strip()
            if ent.label_ == "KEYWORD":
                kw = str(ent)

    if kw:
        date_apis = f"{kw} {date_apis}"

    if per_obj.source.orig_id == 3292:

        date_apis = f"1713<1713-06-30>"
        death = False
        birth = True

    if per_obj.source.orig_id in [19, ]:
        place = None

    if birth:
        if date_apis:
            per_obj.start_date_written = date_apis
            if source:
                notes = per_obj.notes
                if notes:
                    notes += f"Quelle Geburtsdatum: {source}"
                else:
                    notes = f"Quelle Geburtsdatum: {source}"
                per_obj.notes = notes
            per_obj.save()
        if place:
            rt, created = PersonPlaceRelation.objects.get_or_create(name="ist geboren in",
                                                                    name_reverse="ist Geburtsort von")
            pt, created = PlaceType.objects.get_or_create(name="Needs Specification") # todo: refactor type should be city, country, etc. not sterbeort!
            place_obj, created = Place.objects.get_or_create(name=place, kind=pt)

            pp = PersonPlace.objects.create(
                related_person=per_obj,
                related_place=place_obj,
                relation_type=rt,
                start_date_written=date_apis,
                notes=source,  # todo: write quelle here?
            )

    if death:
        if date_apis:
            per_obj.end_date_written = date_apis #todo check not to overwrite existing sd or ed
            if source:
                notes = per_obj.notes
                if notes:
                    notes += f"Quelle Todesdatum: {source}"
                else:
                    notes = f"Quelle Todesdatum: {source}"
                per_obj.notes = notes
            per_obj.save()
        if place:
            rt, created = PersonPlaceRelation.objects.get_or_create(name="ist gestorben in",
                                                                    name_reverse="war Sterbeort von")
            pt, created = PlaceType.objects.get_or_create(name="Needs Specification")
            place_obj, created = Place.objects.get_or_create(name=place, kind=pt)

            pp = PersonPlace.objects.create(
                related_person=per_obj,
                related_place=place_obj,
                relation_type=rt,
                start_date_written=date_apis,
                notes=source,  # todo: write quelle here?

            )


def process_bemerkungen(per_obj, r_bem):
    b = r_bem
    chunks = b.split(",")

    dmatch = re.search(r"(starb)|(starb am)|(gest\.)|(Starb)", b)
    bmatch = re.search(r"(geb\.\s)|(geb. am)", b)
    if dmatch:
        start = dmatch.span()[0]
        source = None
        if "(" in b[start:start + 150].split(",")[0]:
            res = b[start:start + 150].split(")")[0]
            try:
                res, source = res.split("(")
                print("DEATH", res, "source:", source)
            except:
                res = b[start:start + 150].split(",")[0]
                print("DEATH", res, "NO SOURCE")
        else:
            res = b[start:start + 150].split(",")[0]
            print("DEATH", res, "NO SOURCE")

        print(b[start:start + 30])

        process_birth_or_death(per_obj, res, source, death=True)


    if bmatch:
        start = bmatch.span()[0]
        source = None
        print("Birth", b)
        if "(" in b[start:start + 150].split(",")[0]:
            res = b[start:start + 150].split(")")[0]
            try:
                res, source = res.split("(")
                print("DEATH", res, "source:", source)
            except:
                res = b[start:start + 150].split(",")[0]
        else:
            res = b[start:start + 150].split(",")[0]

        process_birth_or_death(per_obj, res, source, birth=True)






def resolve_junge_herrschaft(per_obj, amt: str, hs: str, func_apis: str, start: str, end: str, src_inf):
    """
    Beschreibung:

    Ausgehend vom Hofstab kann ist der Hofstaat für apis in drei kategorien unterscheiden:

    1. Ohne Endung
    2. Ended auf JH
    3. Ended auf *

    Fall 1. Wie gehabt.
    Fall 2:

        Für diese Hofstaaten werden sog. Stäbe als Institution angelegt, die dem OMeA der jeweiligen JH (Spalte H)
        untergeordnet sind, z.B. wird „CL-JH“ zu „Stab CL“, dem „OMeA (JH (MTNS))“ untergeordnet

    Fall 3:

        Spalte g gibt dauer der JH an. Mit funktionsdauer abgleichen wie folgt:
        beispiel JH von 1745-1751

        - Sämtliche Funktionen, die spätestens 1751 enden in die JH
        - Funktionen die nach 1751 oder genau 1751 gebinnen in ErwachsenenHS
        - Funktionen, die vom letzten Jahr der JH bis letzem Jahr der JH gehen in die JH

        - Funktionen verdopplen, die über die junge herrschaft hinausgehen.


    Sonderfälle:

    FALL A: „Bubenhofstaat (Söhne Franz´ II.)“ sowie beim „Mädchenhofstaat (Töchter Franz´ II.)“;
    daher hier keine Stäbe anlegen und sämtliche Funktionen gleich
    beim Import in „OMeA (JH (MTNS))“ verschieben

    FALL B:  der „Hofstab M. Elisabeth (leopoldin.)“ wird mit dem bereits bestehenden Hofstaat ME (L1) bzw.
    OMeA (ME (L1)) zusammengeführt, ist der einzige Hofstab dieser Art, alle anderen werden neu eingeführt
    """

    amt, jh, dauer = resolve_star_jh_dic[hs]
    d_s, d_e = dauer.split("-")
    stab = "Stab " + hs.strip("*") #todo : ist das so richtig?
    ew_hs = hs.strip("*")


    def process_jh(hs=hs, func_apis=func_apis, start=start, end=end, auflösung=(stab, amt, jh), per_obj=per_obj, src_inf=src_inf):
        write_relation_with_stab(per_obj, hs, func_apis, start, end, auflösung, src_inf=src_inf)

    def process_ew(start=start, end=end, func=func_apis, hs=ew_hs, per_obj=per_obj, src_inf=src_inf):
        kombiniert = f"{amt} ({hs})"
        relation = (func, hs, amt, kombiniert, start, end)
        write_relation(per_obj, relation, src_inf)


    if int(end[:4]) <= int(d_e):
        # Ende der Funktion vor oder gleich Junge Herrschaft
        process_jh()

    elif int(start[:4]) > int(d_e):
        # Start der Funktion nach Ende der Herschaft
        process_ew()

    elif int(start[:4]) == int(d_e) < int(end[:4]):
        # Start der Funktion gleich ende JH aber ende funktion später
        process_ew()

    elif int(d_s) <= int(start[:4]) < int(d_e) < int(end[:4]):
        # Start der Funktion während JH, Ende der Funktion nachch Ende der JH
        split_date = f"{d_e}<{d_e}-06-30>"

        process_jh(end=split_date)
        process_ew(start=split_date)
        print("SPLIT FUNKTION FOR IDX: ", orig_idx)

    else:
        print("MISTAKE: NOT ALL CASES PROCESSED")
        breakpoint()



def write_relation_with_stab(per_obj, hs: str, func_apis:str, start: str, end: str, auflösung=None, src_inf=None):

    if not auflösung:
        stab, amt, jh = resolve_jh_dic[hs]
    else:
        stab, amt, jh = auflösung


    #### Create Stab Institution and Create PersonInstitution Relation for function with stab

    inst_type_stab, created = InstitutionType.objects.get_or_create(name="Stab Junge Herrschaft") # todo: maybe move to create database objects
    stab, created = Institution.objects.get_or_create(name=stab, kind=inst_type_stab)
    rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func_apis)


    func_rel = {
        "end_date_written": end,
        "start_date_written": start,
        "related_person": per_obj,
        "related_institution": stab,
        "relation_type": rel_t_obj,
        "notes": src_inf,  # todo: write quelle here?

    }

    PersonInstitution.objects.create(**func_rel)

    #### create hs junge herrschaft
    amt_plus_hs = f"OMeA ({jh})"

    inst_hs, created = Institution.objects.get_or_create(name=jh, kind=inst_type_hst)
    inst_amt_hs, created = Institution.objects.get_or_create(name=amt_plus_hs, kind=inst_type_hst) # muss ich den namen des hofstaates hier noch auflösen?
    stab_hs_rel, created = InstitutionInstitution.objects.get_or_create(related_institutionA_id=stab.pk, related_institutionB_id=inst_amt_hs.pk, relation_type=rl_teil_von)
    am_hs, created = PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=inst_hs, relation_type=rl_pers_hst)

    #todo: what relations to write?
    if src_inf:
        notes = per_obj.notes
        if notes:
            notes += f"Quelle {func_apis}: {src_inf}; "
        else:
            notes = f"Quelle {func_apis}: {src_inf}; "
        per_obj.notes = notes
        per_obj.save()


def write_place_relation(per_obj, rel_place, death, birth, src_inf=None):
    place, start, end = rel_place

    place = place.lstrip("in der").lstrip("in").strip()

    if death:
        rt, created = PersonPlaceRelation.objects.get_or_create(name="ist gestorben in", name_reverse="war Sterbeort von")
        pt, created = PlaceType.objects.get_or_create(name="Needs Specification")

    elif birth:
        rt, created = PersonPlaceRelation.objects.get_or_create(name="ist geboren in", name_reverse="ist Geburtsort von")
        pt, created = PlaceType.objects.get_or_create(name="Needs Specification")

    else:
        rt, created = PersonPlaceRelation.objects.get_or_create(name="hat sich aufgehalten in", name_reverse="war Aufenthaltsort von")
        pt, created = PlaceType.objects.get_or_create(name="Needs Specification")

    place_obj, created = Place.objects.get_or_create(name=place, kind=pt)

    pp = PersonPlace.objects.create(
        related_person=per_obj,
        related_place=place_obj,
        relation_type=rt,
        start_date_written=start,
        end_date_written=end,
    )


    print("CREAETED PLACE: ", place_obj, "and Relation: ", pp)


def write_relation(per_obj, relation: tuple, src_inf=None):
    func, amt, hs, kombi, start, end = relation
    print("RELATION FOR PERSON:", per_obj, "---->",func, amt, hs, kombi, start, end)

    if not amt:
        amt = "Amt Dummy"
    if not hs:
        hs = "Dummy Hofstaat"
    if not kombi:
        kombi = f"{amt} ({hs})"

    amt_hs, created = Institution.objects.get_or_create(name=kombi, kind=inst_type_hst)
    rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func)

    func_rel = {
        "end_date_written": end,
        "start_date_written": start,
        "related_person": per_obj,
        "related_institution": amt_hs,
        "relation_type": rel_t_obj,
        "notes": src_inf, #todo: write quelle here?
        }
    PersonInstitution.objects.create(**func_rel)

    inst_hs, created = Institution.objects.get_or_create(name=hs, kind=inst_type_hst)
    am_hs, created = PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=inst_hs, relation_type=rl_pers_hst)



    # todo: check for super hofstaat relation

    if src_inf:
        notes = per_obj.notes
        if notes:
            notes += f"Quelle {func}: {src_inf}; "
        else:
            notes = f"Quelle {func}: {src_inf}; "
        per_obj.notes = notes
        per_obj.save()

def process_relations(rel: list, orig_idx: int, per_obj):

    for r in rel:
        amt2 = None
        hs = None

        src_dic = {}
        src_inf = None
        for src_el in ["Von", "Bis"]:
            if check_key(f"Quelle{src_el}", r):
                q = r[f"Quelle{src_el}"]
                if q.startswith("P"):
                    src_dic[src_el] = q.replace("P", "HHStA HA OMeA Protokolle")
                    
                elif q.startswith("V"):
                    src_dic[src_el] = q.replace("V", "HHStA HA HStV")

        if src_dic:
            src_inf = [f"{key}: {value}" for key, value in src_dic.items()]
            src_inf = "; ".join(src_inf)
            print(src_inf)

        # print(r)
        if check_key("Amt", r):
            amt_nr = r["Amt"]
            # print(amt_nr)
            if check_key(amt_nr, iamt):
                if check_key("Amt", iamt[amt_nr]):
                    amt = iamt[amt_nr]
                    func = amt["Amt"]
                    try:
                        s = r["von"]
                        start = f"{s}<{s}-06-30>"
                    except KeyError:
                        start = None

                    try:
                        e = r["bis"]
                        end = f"{e}<{e}-06-30>"
                    except KeyError:
                        end = None

                    func_apis, amt = resolve_amt_dic[func]

                    if check_key("Stab", r):
                        stab_nr = r["Stab"]
                        # print(amt_nr)
                        if check_key(stab_nr, istab):
                            if check_key("Stab", istab[stab_nr]):
                                stab = istab[stab_nr]
                                stab = stab["Stab"]
                                # print(stab, func)
                                amt2, hs, kombi = resolve_stab_dic[stab]
                                if not amt:
                                    amt = amt2
                                # todo: junge herrschaft auflösen!
                                print(f"amt: {amt}, hs: {hs}, kombi: {kombi}")

                    # print(f"apis_func: {func_apis}, amt: {amt}, s: {start} e: {end}")
                    if isinstance(hs, str):
                        if stab == "Bubenhofstaat (Söhne Franz´ II.)" or stab == "Mädchenhofstaat (Töchter Franz´ II.)":
                            relation = (func_apis, "OMeA", "(JH (MTNS))", "OMeA (JH (MTNS))", start, end)
                            write_relation(per_obj, relation, src_inf)

                        elif stab == "Hofstab M. Elisabeth (leopoldin.)":
                            relation = (func_apis, "OMeA", "(ME (L1)", "OMeA (ME (L1))", start, end)
                            write_relation(per_obj, relation, src_inf)

                        elif hs.endswith("-JH"):
                            write_relation_with_stab(per_obj, hs, func_apis, start, end, src_inf=src_inf)

                        elif hs.endswith("*"):
                            resolve_junge_herrschaft(per_obj, amt, hs, func_apis, start, end, src_inf=src_inf)

                        else:
                            relation = (func_apis, amt, hs, kombi, start, end)
                            write_relation(per_obj, relation, src_inf)
                            print("USED AMT")
                            print(f"apis_func: {func_apis}, amt: {amt}, kombi: {kombi}, s: {start} e: {end}")

                    if not amt:
                        print(f"no amt and no stab index: {orig_idx}")
                        relation = (func_apis, amt, None, None, start, end)
                        write_relation(per_obj, relation, src_inf)


def process_untergebene(idx: int) -> list:
    data = per_unt[idx]
    untergebene = []

    for entry in data:
        start = None
        text = ""

        if check_key("Jahr", entry):
            date = entry["Jahr"]
            start = f"{date}<{date}-06-30>"
        if check_key("Anzahl", entry):
            count = entry["Anzahl"]
            if count == 1:
                text = "1 Untergebener"
            else:
                text = f"{count} Untergebene"

        if check_key("Bemerkungen", entry):
            text = text + ": " + entry["Bemerkungen"]

        untergebene.append((start, text))

    return untergebene


def create_all_textypes(person, src_base):
    all_fields = {}

    for key, value in person.items():
        all_fields[key] = value

    id = all_fields["Nr"]

    for idx, entry in enumerate(per_amt[id]):
        all_fields[f"Amt {idx}"] = ""

        keyList = ["Amt", "Stab", "von", "bis", "QuelleVon", "QuelleBis"]

        for k in keyList:
            if check_key(k, entry):
                try:
                    if k == "Amt":
                        value = iamt[entry[k]]["Amt"]
                    elif k == "Stab":
                        value = istab[entry[k]]["Stab"]
                    else:
                        value = entry[k]
                except KeyError:
                    value = entry[k]

                all_fields[f"Amt {idx}"] += f"{k}: '{str(value)}'     "
            else:
                all_fields[f"Amt {idx}"] += f"{k}: ''     "

        all_fields[f"Amt {idx}"] = all_fields[f"Amt {idx}"].rstrip("; ")


    for key, value in all_fields.items():
        tt, created = TextType.objects.get_or_create(name=key, entity="Person")
        src_base["pubinfo"] += f"/original text from field: {key}"
        if len(src_base["pubinfo"]) > 400:
            src_base["pubinfo"] = src_base["pubinfo"][:300] #todo: wie mit den zu langen angabgen verfahren?
        st = Source.objects.create(**src_base)
        text = Text.objects.create(text=value, source=st, kind=tt)
        per_obj.text.add(text)





for person in per:
    orig_idx: int = person["Nr"]
    inf_src = [] # add to this with tuple of (target_of information, source of information) like i.e. "inf_src.append((date_of_birth, "source of information))

   #if orig_idx not in [169]:
   #     continue

    pers = {}
    nach_alts = []
    nach_verheiratet = []
    vorname = ""
    gender = ""

    if check_key("Geschlecht", person):
        r_ges = person["Geschlecht"]
        gender = process_geschlecht(r_ges)
        pers["gender"] = gender

    if check_key("Nachname", person):
        r_nach = person["Nachname"]
        matches = re.finditer(r"\(", r_nach)
        if len(list(matches)) > 1:
            print("this is longer", r_nach)
        nachname, nach_alts, nach_verheiratet, ält_jüng, titels = process_nachname(r_nach, gender)
        pers["name"] = nachname
        if not pers["name"]:
            breakpoint()

    if check_key("Vorname", person):  # and nachname:
        r_vor = person["Vorname"]
        vorname, vor_alts, titels = process_vorname(r_vor, gender, ält_jüng, titels)
        pers["first_name"] = vorname

    lt_unt, created = LabelType.objects.get_or_create(name="Untergebene")

    if orig_idx in per_unt.keys():
        untergebene = process_untergebene(orig_idx)
        for u in untergebene:
            print("THIS IS UNTERGEBENE", u[0], "TEXT: ", u[1])
            Label.objects.create(label=u[1], label_type=lt_unt, temp_entity=per_obj, start_date_written=u[0])
    # todo: Processing of Quellen?

    src_base = {"orig_filename": "ACCESS DB", "pubinfo": f"DUMMY", "orig_id": orig_idx}
    src = Source.objects.create(**src_base)


    pers["source"] = src
    per_obj = Person.objects.create(**pers)
    print(per_obj, per_obj.name, per_obj.first_name)
    print(vor_alts, len(vor_alts))
    print(nach_alts, len(nach_alts))

    person_create_person_labels(per_obj, vor_alts, nach_alts, nach_verheiratet)


    for tit in titels:
        t, created = Title.objects.get_or_create(
            name=tit, # todo: Do we need to resolve titles here?
        )
        per_obj.title.add(t)

    per_obj.save()


    if check_key("Konfession", person):
        r_konf = person["Konfession"]
        konfession = konfession_dic[r_konf]
        if not konfession == "Unbekannt":
            lt_konf, created = LabelType.objects.get_or_create(name="Konfession")
            Label.objects.create(label=konfession, label_type=lt_konf, temp_entity=per_obj)

    for l in per_obj.label_set.all():
        print(l, l.label_type)

    if check_key("Titel", person):
        r_tit = person["Titel"]
        process_title(r_tit, per_obj)

    if check_key(orig_idx, per_amt):
        rel = per_amt[orig_idx]
        process_relations(rel, orig_idx, per_obj)

    create_all_textypes(person, src_base)

    if check_key("Bemerkungen", person):
        r_bem = person["Bemerkungen"]
        process_bemerkungen(per_obj, r_bem)
        # todo: extend with NLP to access additional information from Bemerkungen



if __name__ == "__main__":
    pass
