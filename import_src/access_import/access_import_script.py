import logging
import time
import re
import reversion
from collections import Counter
from viecpro_import_app.import_src.access_import.import_access_jsonlines import istab, iamt, per_amt, per_unt, per
from viecpro_import_app.file_sets.working_sets.SETS import P
from viecpro_import_app.import_src.file_processing import resolve_abbreviations_access
from viecpro_import_app.import_src.import_logger.import_logger_config import init_logger, init_complogger, \
    init_caselogger, init_funclogger
from import_setup import *
from access_nlp import nlp2, matcher2, siglen_dic, German, EntityRuler
from django.contrib.auth.models import User
from apis_core.apis_metainfo.models import Text
from apis_core.apis_entities.models import Person, Institution, Place
from apis_core.apis_vocabularies.models import Title, InstitutionType, PersonInstitutionRelation, PersonPlaceRelation, \
    PlaceType
from apis_core.apis_metainfo.models import Collection
from apis_core.apis_metainfo.models import Source
from apis_core.apis_relations.models import PersonInstitution, InstitutionInstitution, PersonPlace
from apis_core.apis_labels.models import Label
from apis_core.apis_vocabularies.models import TextType, LabelType


###### SETUP LOGGING #######
timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
logfolder = "access_logs"
logfile = f"logfiles/{logfolder}/{timestamp}.txt"
case_logfile = f"logfiles/case_logs/{timestamp}.txt"
logger_level = logging.INFO
init_logger(level=logger_level, logfile=logfile)
init_funclogger(level=logger_level, logfile=logfile)
init_complogger(level=logger_level, logfile=logfile)
init_caselogger(level=logger_level, logfile=case_logfile, case="test_case")

def overwrite_kind_of_hzab_hsv_inst(inst_name, inst_type_new):
    if Institution.objects.filter(name=inst_name).exists() and not Institution.objects.filter(name=inst_name, kind=inst_type_new).exists():
        name_match = Institution.objects.get(name=inst_name)
        old_type = name_match.kind
        name_match.kind = inst_type_new
        name_match.collection.add(col_inst_excel_and_acc)
        name_match.save()
        funclogger.info(f"Inst {inst_name} existed, changed type {old_type} to {inst_type_new}")


call_counter = Counter()
def relation_logger(func):
    def wrapper(*args, case=None, **kwargs):
        if case:
            funclogger.info(f"called: {func.__name__}, Case: {case}, args:{args}, kwargs:{kwargs}")
            call_counter[func.__name__] += 1
            call_counter[case] += 1
            funclogger.info(f"case: {case}, function: {func.__name__}")

            func(*args, **kwargs)
        else:
            func(*args, **kwargs)

    return wrapper

def log_metainfo(username, collection, msg, logger):
    """
    Logs metainfo for import to logger
    """
    metainfo_args = {"username": username,
                     "collection": collection,
                     "log_msg":msg,
                       }

    logger.info("----------------")
    logger.info("Import MetaInfo:\n")
    metainfo_msg = [f"{key}: {value}" for key, value in metainfo_args.items()]
    for m in metainfo_msg:
        logger.info(m)
    logger.info("----------------\n\n")


logger = logging.getLogger("import_logger")
funclogger = logging.getLogger("func_logger")
caselogger = logging.getLogger("case_logger")


nlp = German()
ruler = EntityRuler(nlp)
patterns = []
keywords = ["vor", "ca", "ca.", "Anfang", "Ende", ]
kw_patterns = [{"label": "KEYWORD", "pattern": pat} for pat in keywords]

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


def process_vorname(r_vor, gender, ält_jüng, titels):
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
        brackets_list = brackets.split(",")
        for el in brackets_list:
            vor_alts.append(el)
        r_clean = r_clean[:b_start] + r_clean[b_end:]
        r_clean = r_clean.replace("()", "")

    vorname = r_clean.strip()
    if "v." in vorname:
        vorname = vorname.replace("v.", "von")

    if gender == "female":
        tit_temp = []
        for tit in titels:
            try:
                tit_temp.append(female_titels_dic[tit])
            except KeyError as k:
                tit_temp.append(tit)

        titels = tit_temp

    if ält_jüng:
        vorname = vorname + " " + ält_jüng


    if r_vor == "(Johann) Karl (Carlo)":
        vorname = "Karl"
        vor_alts = ["Karl", "Carlo"]

    if vorname in blank_names_list:
        if vor_alts:
            temp = deepcopy(vor_alts)
            temp.reverse()
            vorname = temp.pop()
            temp.reverse()
            vor_alts = deepcopy(temp)
        else:
            vorname = "NN"

    if vor_alts:
        vor_alts = list(filter(lambda n: n not in blank_names_list, vor_alts))

    return vorname, vor_alts, titels


def process_geschlecht(r_ges):
    gender = gender_dic[r_ges]

    return gender


def process_nachname(r_nach, gender):
    doubletten = ["(I)", "(II)", "(III)", "(IV)", "(V)", "(VI)", "(VII)"]
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
            match = re.search(r"\(.*\)", r_clean)
            brackets = match.group(0).rstrip(")").lstrip("(")
            b_start = match.span()[0]
            b_end = match.span()[1]
            brackets_list = brackets.split(",")
            for el in brackets_list:
                nach_alts.append(el)
            r_clean = r_clean[:b_start] + r_clean[b_end:]
            r_clean = r_clean.replace("()", "")

        nachname = r_clean.strip()

        if "geb." in nachname:

            if "," in nachname:
                splitted = nachname.split(",")

            else:
                splitted = nachname.split("geb.")

            verheirated = splitted[0]
            birthname = splitted[1].strip().lstrip("geb.")
            nach_verheiratet = verheirated
            nachname = birthname

            for el in abbr_dic_m.keys():

                if re.search(el, nachname):
                    match = re.search(el, nachname).group(0)
                    tit_res = female_titels_dic[abbr_dic_m[el]]
                    titels.append(tit_res)
                    nachname = nachname.replace(match, "")
                    continue

            nach_verheiratet = nach_verheiratet.replace("v.", "von").replace("u.", "und").strip()
            nach_verheiratet, ält_jüng = replace_ältere_jüngere(nach_verheiratet)
            gender = "female"
        nachname = nachname.replace("v.", "von").replace("u.", "und").strip()
        nachname, ält_jüng = replace_ältere_jüngere(nachname)

    if nachname in blank_names_list:
        if nach_alts:
            temp = deepcopy(nach_alts)
            temp.reverse()
            nachname = temp.pop()
            temp.reverse()
            nach_alts = deepcopy(temp)
        else:
            nachname = "NN"

    if nach_alts:
        nach_alts = list(filter(lambda n: n not in blank_names_list, nach_alts))

    return nachname, nach_alts, nach_verheiratet, ält_jüng, titels, gender


def check_key(key, dic):
    res = False
    if key in dic:
        res = True

    return res


def person_create_person_labels(per_obj, vorname, labels_fam, labels_fam_hzt):
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

    if labels_fam_hzt:
        f = labels_fam_hzt
        Label.objects.create(
            label=f, label_type=lt_nachname_verheiratet, temp_entity=per_obj
        )


def get_date(doc: object):
    start = None
    end = None
    matches = matcher2(doc)
    if matches:

        for m in matches:
            match_id = m[0]
            string_id = nlp2.vocab.strings[match_id]
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


def write_titel_datiert_label(per_obj, tit, date_apis_start, date_apis_end):
    funclogger.info(f"tit was: {tit}")
    if tit == "Freifrau von":
        lt_set = [l.label_type for l in per_obj.label_set.all()]
        funclogger.info(f"tit was {tit} an label_types are: {lt_set}")
        if not lt_nachname_verheiratet in lt_set:
            tit = "Freiin von"
            funclogger.info(f"tit was Freifrau and lt_nachname_verheiratet was true, so changed tit to Freiin")
    tit = resolve_abbreviations_access(tit, cfg.list_abbreviations)
    funclogger.info(f"tit resolved is: {tit}")
    if date_apis_start:
        Label.objects.create(
            label_type=lt_titel_datiert,
            temp_entity=per_obj,
            label=tit,
            start_date_written=date_apis_start,
            end_date_written=date_apis_end
        )

    t2, created = Title.objects.get_or_create(
        name=tit
    )
    per_obj.title.add(t2)


def extract_title(chunk, tit, r_tit, gender, per_obj):
    date_apis_start, end = get_previous_date(chunk, r_tit)

    chunk = chunk.strip()
    tit_resolved = abbr_dic_m[tit]
    if gender == "female":
        tit_resolved = female_titels_dic[tit_resolved]

    if match := re.match(r"(seit mind. )*(\d{4})", chunk):
        date = match.group(2).strip()
        date_apis_start = f"{date}<{date}-06-30>"
        if match.group(1):
            date_apis_start = "mind. " + date_apis_start

    write_titel_datiert_label(per_obj, tit_resolved, date_apis_start, end)


def extract_titel_zu_titel(chunk, tit, r_tit, per_obj):

    date_apis_start, end = get_previous_date(chunk, r_tit)

    if match := re.match(r"(seit mind. )*(\d{4})", chunk):
        date = match.group(2).strip()
        date_apis_start = f"{date}<{date}-06-30>"
        if match.group(1):
            date_apis_start = "mind. " + date_apis_start

    write_titel_datiert_label(per_obj, tit, date_apis_start, end)


def get_previous_date(chunk, field):
    idx = field.find(chunk) + len(chunk)
    search_string = field[:idx]
    a,b = None, None
    start, end = None, None
    res = re.findall("(\d{4}-\d{2})|(\d{4})", search_string)
    if len(res) >= 1:
        res = res[-1]
    if res != []:
        a,b = res
    if a:
        date = a
        start = f"{date}<{date[:4]}-06-30>"
        end = f"{date[:2]}{date[-2:]}<{date[:2]}{date[-2:]}-06-30>"
    elif b:
        start = f"{b}<{b}-06-30>"


    return start, end


class global_he:
    data = []


def process_title(r_tit: str, per_obj, gender):
    r_tit = r_tit.replace("\n", " ")
    chunks = r_tit.split(",")

    def has_title(c):
        is_title = False

        for el in abbr_dic_m.keys():
            if el in c:
                extract_title(c, el, r_tit, gender=gender, per_obj=per_obj)
                is_title = True
                break
        return is_title

    def adjust_start_date_for_funktion(start):

        if start and len(start) == 16 and int(start[-11:-7]) > 1715:
            pass
            #start = f"ab ca. {start}"
        elif start and len(start) == 16:
            start = f"erw. {start}"

        return start

    for c in chunks:
        if has_title(c):
            pass
        if "h.e." in c:
            qs, qe = get_previous_date(c, r_tit)
            qs = qs[-11:-7]
            global_he.data.append(qs)

        else:
            for el in c.split(" "):
                 if el.strip() in tit_zu_tit_dic.keys():
                    tit = tit_zu_tit_dic[el.strip()]
                    extract_titel_zu_titel(c, tit, r_tit, per_obj=per_obj)
                    break

            doc = nlp2(c)
            start, end = get_date(doc)

            death_match = re.search(r"(starb)|(gest\.)", c)
            birth_match = re.search(r"(geb\.)", c)
            death, birth = False, False

            if death_match:
                if start:
                    per_obj.end_date_written = start
                    per_obj.save()
                    death = True
                elif end:
                    breakpoint()

            elif birth_match:
                if start:
                    per_obj.start_date_written = start
                    per_obj.save()
                    birth = True
                elif end:
                    breakpoint()

            if not start:
                start, end = get_previous_date(c, r_tit)


            for e in doc.ents:
                if e.label_ == "PLACE":
                    rel_place = (str(e), start, end)
                    write_place_relation(per_obj, rel_place, death, birth)

                elif check_key(str(e), siglen_dic):
                    func, amt, hs = siglen_dic[str(e)]
                    start = adjust_start_date_for_funktion(start)
                    relation = (func, amt, hs, None, start, end, None)
                    write_relation(per_obj, relation)

                elif e.label_ == "FUNKTION" and not check_key(str(e), siglen_dic):
                    start = adjust_start_date_for_funktion(start)
                    rel_func = (str(e), None, None, "ACCESS TEST (TEST FROM TITLE)", start, end,
                                None)
                    write_relation(per_obj, rel_func)


def process_birth_or_death(per_obj, res, source, birth=None, death=None):
    longdate = re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", res.replace(" ", "").replace(" ", ""))
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
                new = "0" + el
                date.append(new)
            else:
                date.append(el)
        date = "-".join(date)
        date_apis = f"{date}<{date}>"
        doc = nlp(res)

        for ent in doc.ents:
            if ent.label_ == "PLACE":
                place = str(ent).lstrip("in").strip()
            if ent.label_ == "KEYWORD":
                kw = str(ent)

    elif shortdate:
        date = shortdate.group(0)
        if 1550 < int(date) < 1850:
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
                references = per_obj.references
                if references:
                    references += f"Quelle Geburtsdatum: {source}"
                else:
                    references = f"Quelle Geburtsdatum: {source}"
                per_obj.references = references
            per_obj.save()
        if place:
            rt, created = PersonPlaceRelation.objects.get_or_create(name="ist geboren in",
                                                                    name_reverse="ist Geburtsort von")
            pt, created = PlaceType.objects.get_or_create(
                name="Needs Specification")
            place_obj, created = Place.objects.get_or_create(name=place, kind=pt)

            pp = PersonPlace.objects.create(
                related_person=per_obj,
                related_place=place_obj,
                relation_type=rt,
                start_date_written=date_apis,
                references=source,
            )

    if death:
        if date_apis:
            per_obj.end_date_written = date_apis
            if source:
                references = per_obj.references
                if references:
                    references += f"Quelle Todesdatum: {source}"
                else:
                    references = f"Quelle Todesdatum: {source}"
                per_obj.references = references
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
                references=source,
            )


def process_bemerkungen(per_obj, r_bem):
    b = r_bem
    dmatch = re.search(r"(starb)|(starb am)|(gest\.)|(Starb)|(stirbt)", b)
    bmatch = re.search(r"(geb\.\s)|(geb. am)", b)
    if dmatch:
        start = dmatch.span()[0]
        source = None
        if "(" in b[start:start + 150].split(",")[0]:
            res = b[start:start + 150].split(")")[0]
            try:
                res, source = res.split("(")
            except:
                res = b[start:start + 150].split(",")[0]
        else:
            res = b[start:start + 150].split(",")[0]


        process_birth_or_death(per_obj, res, source, death=True)

    if bmatch:
        start = bmatch.span()[0]
        source = None
        if "(" in b[start:start + 150].split(",")[0]:
            res = b[start:start + 150].split(")")[0]
            try:
                res, source = res.split("(")
            except:
                res = b[start:start + 150].split(",")[0]
        else:
            res = b[start:start + 150].split(",")[0]

        process_birth_or_death(per_obj, res, source, birth=True)

@relation_logger
def resolve_junge_herrschaft(per_obj, amt: str, hs: str, func_apis: str, start: str, end: str, src_inf, amt_super):
    amt, jh, dauer = resolve_star_jh_dic[hs]
    d_s, d_e = dauer.split("-")
    stab = "Stab " + hs.strip("*")
    ew_hs = hs.strip("*")

    def process_jh(hs=hs, func_apis=func_apis, start=start, end=end, auflösung=(stab, amt, jh), per_obj=per_obj,
                   src_inf=src_inf, amt_super=amt_super):
        write_relation_with_stab(per_obj, hs, func_apis, start, end, auflösung, src_inf=src_inf, amt_super=amt_super)

    def process_ew(start=start, end=end, func=func_apis, hs=ew_hs, per_obj=per_obj, src_inf=src_inf,
                   amt_super=amt_super):
        kombiniert = f"{amt} ({hs})"
        relation = (func, amt, hs, kombiniert, start, end, amt_super)
        write_relation(per_obj, relation, src_inf)

    if int(end[:4]) <= int(d_e):
        # Ende der Funktion vor oder gleich Junge Herrschaft
        process_jh()

    elif int(start[:4]) > int(d_e):
        # Start der Funktion nach Ende der Herrschaft
        process_ew()

    elif int(start[:4]) == int(d_e) < int(end[:4]):
        # Start der Funktion gleich Rnde JH aber Ende Funktion später
        process_ew()

    elif int(d_s) <= int(start[:4]) < int(d_e) < int(end[:4]):
        # Start der Funktion während JH, Ende der Funktion nach Ende der JH
        split_date = f"{d_e}<{d_e}-06-30>"

        process_jh(end=split_date)
        process_ew(start=split_date)

    else:
        breakpoint()

@relation_logger
def write_relation_with_stab(per_obj, hs: str, func_apis: str, start: str, end: str, auflösung=None, src_inf=None,
                             amt_super=None):

    if not auflösung:
        stab, amt, jh = resolve_jh_dic[hs]
    else:
        stab, amt, jh = auflösung

    funclogger.info(f"stab was {stab}")

    if stab.endswith("(Ehzgin.)"):
        stab = stab.rstrip("(Ehzgin.)").strip()
    if stab.endswith("(Ehzg.)"):
        stab = stab.rstrip("(Ehzg.)").strip()

    funclogger.info(f"stab stripped to: {stab}")


    hs_clean = hs.rstrip("-JH").rstrip("*")
    hs_mutter = jh.strip()[4:-1]
    unsicher, created = InstitutionType.objects.get_or_create(name="Access Amt (Zuordnung wird nachgetragen)", parent_class=inst_type_amt)


    overwrite_kind_of_hzab_hsv_inst(stab, inst_type_stab)
    stab, created = Institution.objects.get_or_create(name=stab, kind=inst_type_stab)
    rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func_apis)

    func_rel = {
        "end_date_written": end,
        "start_date_written": start,
        "related_person": per_obj,
        "related_institution": stab,
        "relation_type": rel_t_obj,
        "references": src_inf,

    }

    PersonInstitution.objects.create(**func_rel)

    amt_plus_hs = f"OMeA ({jh})"

    overwrite_kind_of_hzab_hsv_inst(jh, inst_type_hst)
    overwrite_kind_of_hzab_hsv_inst(hs_mutter, inst_type_hst)
    overwrite_kind_of_hzab_hsv_inst(amt_plus_hs, inst_type_oha)
    inst_hs_jh, created = Institution.objects.get_or_create(name=jh, kind=inst_type_hst)
    inst_hs_mutter, created = Institution.objects.get_or_create(name=hs_mutter, kind=inst_type_hst)
    inst_amt_hs, created = Institution.objects.get_or_create(name=amt_plus_hs,
                                                             kind=inst_type_oha)  # muss ich den namen des hofstaates hier noch auflösen?
    stab_hs_rel, created = InstitutionInstitution.objects.get_or_create(related_institutionA_id=stab.pk,
                                                                        related_institutionB_id=inst_amt_hs.pk,
                                                                        relation_type=rl_teil_von)

    oha_hs_jh_rel, created = InstitutionInstitution.objects.get_or_create(related_institutionA_id=inst_amt_hs.pk,
                                                                          related_institutionB_id=inst_hs_jh.pk,
                                                                          relation_type=rl_teil_von)

    hs_jh_hs_mutter_rel, created = InstitutionInstitution.objects.get_or_create(related_institutionA_id=inst_hs_jh.pk, related_institutionB_id=inst_hs_mutter.pk, relation_type=rl_teil_von)
    am_hs, created = PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=inst_hs_jh,
                                                             relation_type=rl_pers_hst)


def extract_date(date_apis):
    date = re.search(r"\d{4}", date_apis)
    if date:
        date = int(date.group(0))

    return date


def compare_date_with_sn_range(per_obj, relation, src_inf):
    func, amt, hs, kombi, start, end, amt_super = relation
    match = False
    if start and end:
        s = extract_date(start)
        e = extract_date(end)

        for pos, (ss, se, flag) in enumerate(global_supernum.lst):
            s = int(s)
            if e:
                e = int(e)
            ss = int(ss)
            if se:
                se = int(se)
            global_supernum.processed = True

            if s and e and s == ss <= e:
                if flag:
                    col_supernum_doubled, created = Collection.objects.get_or_create(name="Access Supernumerarius: Needs Manual Check")
                    per_obj.collection.add(col_supernum_doubled)

                if se and se == e:
                     func = func + ", sn."
                     write_relation(per_obj, (func, amt, hs, kombi, start, end, amt_super), src_inf)
                     match = True
                elif se and se < e:
                    end_sn = f"{se}<{se}-06-30>"
                    func_sn = func + ", sn."
                    relation_sn = func_sn, amt, hs, kombi, start, end_sn, amt_super
                    start_f = f"{se+1}<{se+1}-06-30>"
                    end_f = end
                    relation_f = func, amt, hs, kombi, start_f, end_f, amt_super
                    write_relation(per_obj, relation_sn, src_inf)
                    write_relation(per_obj, relation_f, src_inf)
                    match = True

                elif not se:
                    # this is the case when start date of sn equals start date of function, but there is not an end date.
                    end_sn = f"{ss}<{ss}-06-30>"
                    func_sn = func + ", sn."
                    relation_sn = func_sn, amt, hs, kombi, start, end_sn, amt_super
                    start_f = f"{ss + 1}<{ss + 1}-06-30>"
                    end_f = end
                    relation_f = func, amt, hs, kombi, start_f, end_f, amt_super
                    write_relation(per_obj, relation_sn, src_inf)
                    write_relation(per_obj, relation_f, src_inf)
                    match = True

            global_supernum.lst[pos] = (ss, se, True)
        global_supernum.processed = False

    return match



@relation_logger
def write_relation(per_obj, relation: tuple, src_inf=None):
    func, amt, hs, kombi, start, end, amt_super = relation
    stopflag = False

    if global_supernum.lst and not global_supernum.processed:
        stopflag = compare_date_with_sn_range(per_obj, relation, src_inf)

    if "\n" in func:
        split_funcs = func.split("\n")
        test = "\n" in amt
        split_amt = amt.split("\n")
        funclogger.info(f"newline in func, splitted funcs to {split_funcs}, also in amt (?) - {test}, amt was: {amt}")
        for f, a in zip(split_funcs, split_amt):
            relation_new = (f.strip(), a.strip(), hs, kombi, start, end, amt_super)
            funclogger.info(f"Split Function and Amt to: {f}, {a}")
            write_relation(per_obj, relation_new, src_inf)

        stopflag = True

    if not stopflag:
        zuordnung_amt = None #GP: Zuordnung was finally not used in given import

        unsicher, created = InstitutionType.objects.get_or_create(name="Access Amt (Zuordnung wird nachgetragen)", parent_class=inst_type_amt)

        if not amt:
            amt = "Amt Dummy"
        if not hs:
            hs = "Dummy Hofstaat"

        funclogger.info(f"Before overwriting kombi: amt is: {amt}, hs is: {hs}, kombi is: {kombi}")
        kombi = f"{amt} ({hs})"
        oh = f"{amt_super} ({hs})"
        funclogger.info(f"After overwriting kombi: kombi is: {kombi}, and oh is {oh}")



        resolved = False
        for element in lst_oberste_hofämter:
            # Checks if amt is already a "Oberstes Hofamt"
            if element in amt:
                overwrite_kind_of_hzab_hsv_inst(kombi, inst_type_oha)
                amt_hs, created = Institution.objects.get_or_create(name=kombi,
                                                                    kind=inst_type_oha)
                rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func)

                func_rel = {
                    "end_date_written": end,
                    "start_date_written": start,
                    "related_person": per_obj,
                    "related_institution": amt_hs,
                    "relation_type": rel_t_obj,
                    "references": src_inf,
                }
                PersonInstitution.objects.create(**func_rel)

                overwrite_kind_of_hzab_hsv_inst(hs, inst_type_hst)
                inst_hs, created = Institution.objects.get_or_create(name=hs, kind=inst_type_hst)
                per_hs, created = PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=inst_hs,
                                                                          relation_type=rl_pers_hst)
                InstitutionInstitution.objects.get_or_create(related_institutionA=amt_hs, related_institutionB=inst_hs,
                                                             relation_type=rl_teil_von)
                resolved = True



        if not resolved:

            if zuordnung_amt:

                amt_kat_type, created = InstitutionType.objects.get_or_create(name=zuordnung_amt, parent_class=inst_type_amt)
                overwrite_kind_of_hzab_hsv_inst(kombi, amt_kat_type)
                amt_hs, created = Institution.objects.get_or_create(name=kombi,
                                                                    kind=amt_kat_type)
            else:
                overwrite_kind_of_hzab_hsv_inst(kombi, unsicher)
                amt_hs, created = Institution.objects.get_or_create(name=kombi,
                                                                    kind=unsicher)
            rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func)

            func_rel = {
                "end_date_written": end,
                "start_date_written": start,
                "related_person": per_obj,
                "related_institution": amt_hs,
                "relation_type": rel_t_obj,
                "references": src_inf,
            }
            PersonInstitution.objects.create(**func_rel)
            overwrite_kind_of_hzab_hsv_inst(hs, inst_type_hst)
            inst_hs, created = Institution.objects.get_or_create(name=hs, kind=inst_type_hst)
            per_hs, created = PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=inst_hs,
                                                                      relation_type=rl_pers_hst)

            if amt_super:
                try:
                    overwrite_kind_of_hzab_hsv_inst(oh, inst_type_oha)
                    inst_super, created = Institution.objects.get_or_create(name=oh, kind=inst_type_oha)
                    funclogger.info("amt super written in try (line 849)")
                except:
                    inst_super = Institution.objects.filter(name=oh)[0]
                    funclogger.info("amt super written in except (line 852)")

                InstitutionInstitution.objects.get_or_create(related_institutionA=amt_hs, related_institutionB=inst_super,
                                                             relation_type=rl_teil_von)
                InstitutionInstitution.objects.get_or_create(related_institutionA=inst_super, related_institutionB=inst_hs, relation_type=rl_teil_von)

            else:
                funclogger.info(f"No amt_super")
                InstitutionInstitution.objects.get_or_create(related_institutionA=amt_hs, related_institutionB=inst_hs, relation_type=rl_teil_von)
                funclogger.info(f"wrote inst inst realation for amt {amt_hs} and hs {inst_hs}")

def write_place_relation(per_obj, rel_place, death, birth, src_inf=None):
    place, start, end = rel_place

    place = place.lstrip("in der").lstrip("nach").lstrip("in").strip()

    if death:
        rt, created = PersonPlaceRelation.objects.get_or_create(name="ist gestorben in",
                                                                name_reverse="war Sterbeort von")
        pt, created = PlaceType.objects.get_or_create(name="Needs Specification")

    elif birth:
        rt, created = PersonPlaceRelation.objects.get_or_create(name="ist geboren in",
                                                                name_reverse="ist Geburtsort von")
        pt, created = PlaceType.objects.get_or_create(name="Needs Specification")

    else:
        rt, created = PersonPlaceRelation.objects.get_or_create(name="hat sich aufgehalten in",
                                                                name_reverse="war Aufenthaltsort von")
        pt, created = PlaceType.objects.get_or_create(name="Needs Specification")

    place_obj, created = Place.objects.get_or_create(name=place, kind=pt)

    pp = PersonPlace.objects.create(
        related_person=per_obj,
        related_place=place_obj,
        relation_type=rt,
        start_date_written=start,
        end_date_written=end,
    )


def process_spalte_e(stab, amt_spalte_e):
    amt2, hs, kombi = resolve_stab_dic[stab]
    amt = amt_spalte_e
    amt_super = amt2
    funclogger.info(f"amt: {amt}, kombi: {kombi}, hs: {hs}, amt2 - is now amt_super: {amt2}")

    return amt, amt_super, hs, kombi


def process_relations(rel: list, orig_idx: int, per_obj):

    def is_djh(func):
        if func.startswith("djh"):
            return True
        else:
            return False

    for r in rel:
        logflag = False
        case = None
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
            else:
                date = None
                if check_key("Amt", r):
                    amt_nr = r["Amt"]
                    if check_key(amt_nr, iamt):
                        if check_key("Amt", iamt[amt_nr]):

                            try:
                                s = r["von"]
                            except KeyError:
                                s = None

                            try:
                                e = r["bis"]
                            except KeyError:
                                e = None

                    if src_el == "Von":
                        date = s
                    else:
                        date = e
                if date:
                    src_dic[src_el] = f"Hof- und Ehrenkalender "+str(date)

        if src_dic:
            src_inf = [f"{key}: {value}" for key, value in src_dic.items()]
            src_inf = "; ".join(src_inf)

        if check_key("Amt", r):
            amt_nr = r["Amt"]
            if check_key(amt_nr, iamt):
                if check_key("Amt", iamt[amt_nr]):
                    amt_acc = iamt[amt_nr]
                    func = amt_acc["Amt"]
                    funclogger.info(f"func is: {func}")
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

                    func_apis, amt, amt_super = resolve_amt_dic[func]
                    funclogger.info(f"After standard resolve of amt_dic amt is: {amt}, and amt super is: {amt_super}")

                    if check_key("Stab", r):
                        stab_nr = r["Stab"]
                        if check_key(stab_nr, istab):
                            if check_key("Stab", istab[stab_nr]):

                                stab = istab[stab_nr]
                                stab = stab["Stab"]
                                funclogger.info(f"stab was {stab}")
                                if amt:
                                    amt, amt_super, hs, kombi = process_spalte_e(stab, amt)

                                    if amt in inst_without_oha_list:
                                        funclogger.info(f"amt {amt} was in inst_without_oha_list")
                                        amt_super = None
                                    funclogger.info(f"Amt was true {amt}, spalte e was not none, orig_idx = {orig_idx}, amt_super set to: {amt_super}")

                                    logflag = True
                                else:
                                    funclogger.info(f"amt was not true: {amt}")
                                    logflag = False
                                    amt, hs, kombi = resolve_stab_dic[stab]
                                    funclogger.info(f"amt {amt}, hs {hs}, kombi {kombi} after elese in if amt in write relation. (line 993ff)")




                    if isinstance(hs, str):

                        was_djh_func = is_djh(func_apis)
                        if was_djh_func and not stab == "EH Rudolph (Sohn Franz´ II.)":
                            funclogger.info(f"func_apis before strip: {func_apis}")
                            func_apis = func_apis.lstrip("djh").strip()
                            funclogger.info(f"func_apis after strip: {func_apis}")


                        if hs == "MT-J2":
                            hs = "MT-J2 (Ks.)"

                        if stab == "Bubenhofstaat (Söhne Franz´ II.)" or stab == "Mädchenhofstaat (Töchter Franz´ II.)":
                            if logflag:
                                case = 1
                            jh = "JH (MTNS)"
                            hs_mutter = "MTNS"
                            relation = (func_apis, "OMeA", jh , "OMeA (JH (MTNS))", start, end, amt_super)
                            write_relation(per_obj, relation, src_inf, case=case)



                            overwrite_kind_of_hzab_hsv_inst(jh, inst_type_hst)
                            overwrite_kind_of_hzab_hsv_inst(hs_mutter, inst_type_hst)
                            inst_hs_jh, created = Institution.objects.get_or_create(name=jh,
                                                                                    kind=inst_type_hst)
                            inst_hs_mutter, created = Institution.objects.get_or_create(name=hs_mutter,
                                                                                        kind=inst_type_hst)
                            hs_jh_hs_mutter_rel, created = InstitutionInstitution.objects.get_or_create(
                                related_institutionA_id=inst_hs_jh.pk, related_institutionB_id=inst_hs_mutter.pk,
                                relation_type=rl_teil_von)

                        elif stab == "Hofstab Kaiserin Elisabeth Chr." and was_djh_func:
                            overwrite_kind_of_hzab_hsv_inst("EC", inst_type_hst)

                            amt_1, created = Institution.objects.get_or_create(name="OMeA (JH (EC))", kind=inst_type_oha)
                            amt_2, created = Institution.objects.get_or_create(name="JH (EC)", kind=inst_type_hst)
                            hs, created = Institution.objects.get_or_create(name="EC", kind=inst_type_hst)

                            rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func_apis)

                            func_rel = {
                                "end_date_written": end,
                                "start_date_written": start,
                                "related_person": per_obj,
                                "related_institution": amt_1,
                                "relation_type": rel_t_obj,
                                "references": src_inf,
                            }

                            PersonInstitution.objects.get_or_create(**func_rel)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=amt_1, related_institutionB=amt_2, relation_type=rl_teil_von)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=amt_2, related_institutionB=hs, relation_type=rl_teil_von)
                            PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=amt_2, relation_type=rl_pers_hst)



                        elif stab == "Hofstab Isabella" and was_djh_func:
                            overwrite_kind_of_hzab_hsv_inst("IS", inst_type_hst)

                            amt_1, created = Institution.objects.get_or_create(name="Stab MTE",
                                                                               kind=inst_type_stab)
                            amt_2, created = Institution.objects.get_or_create(name="OMeA (JH (IS))", kind=inst_type_oha)
                            hs, created = Institution.objects.get_or_create(name="JH (IS)", kind=inst_type_hst)
                            hs2, created = Institution.objects.get_or_create(name="IS", kind=inst_type_hst)

                            rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func_apis)

                            func_rel = {
                                "end_date_written": end,
                                "start_date_written": start,
                                "related_person": per_obj,
                                "related_institution": amt_1,
                                "relation_type": rel_t_obj,
                                "references": src_inf,
                            }

                            PersonInstitution.objects.get_or_create(**func_rel)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=amt_1,
                                                                         related_institutionB=amt_2,
                                                                         relation_type=rl_teil_von)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=amt_2,
                                                                         related_institutionB=hs,
                                                                         relation_type=rl_teil_von)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=hs,
                                                                         related_institutionB=hs2,
                                                                         relation_type=rl_teil_von)
                            PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=hs,
                                                                    relation_type=rl_pers_hst)


                        elif (stab == "Okstab MT u Franz St." or stab == "Zweiter OHMstab MT u Franz St." or stab == "Zweiter OHMstab MT u Joseph II.") and was_djh_func:
                            overwrite_kind_of_hzab_hsv_inst("MT", inst_type_hst)

                            amt_1, created = Institution.objects.get_or_create(name="OMeA (JH (MT))", kind=inst_type_oha)
                            amt_2, created = Institution.objects.get_or_create(name="JH (MT)", kind=inst_type_hst)
                            hs, created = Institution.objects.get_or_create(name="MT", kind=inst_type_hst)

                            rel_t_obj, created = PersonInstitutionRelation.objects.get_or_create(name=func_apis)

                            func_rel = {
                                "end_date_written": end,
                                "start_date_written": start,
                                "related_person": per_obj,
                                "related_institution": amt_1,
                                "relation_type": rel_t_obj,
                                "references": src_inf,
                            }

                            PersonInstitution.objects.get_or_create(**func_rel)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=amt_1,
                                                                         related_institutionB=amt_2,
                                                                         relation_type=rl_teil_von)
                            InstitutionInstitution.objects.get_or_create(related_institutionA=amt_2,
                                                                         related_institutionB=hs, relation_type=rl_teil_von)
                            PersonInstitution.objects.get_or_create(related_person=per_obj, related_institution=amt_2,
                                                                    relation_type=rl_pers_hst)


                        elif stab == "Hofstab M. Elisabeth (leopoldin.)":
                                    if logflag:
                                        case = 2
                                    relation = (func_apis, "OMeA", "ME (L1)", "OMeA (ME (L1))", start, end, amt_super)
                                    write_relation(per_obj, relation, src_inf, case=case)



                        elif hs.endswith("-JH"):
                            if logflag:
                                case = 3
                            write_relation_with_stab(per_obj, hs, func_apis, start, end, src_inf=src_inf,
                                                     amt_super=amt_super, case=case)


                        elif hs.endswith("*"):
                            if logflag:
                                case = 4
                            resolve_junge_herrschaft(per_obj, amt, hs, func_apis, start, end, src_inf=src_inf,
                                                     amt_super=amt_super, case=case)


                        elif hs == "MT-FS":
                            new_hs = "MT-FS (Ehzge.)"
                            hs_kais = "MT-FS (Ks.)"
                            sd = int(start[-11:-7])
                            ed = int(end[-11:-7])
                            funclogger.info(f"orig-idx = {orig_idx} in fall MT-FS daten sind: ")
                            if sd < 1745 and ed <= 1745:
                                if logflag:
                                    case = 5
                                relation = (func_apis, amt, new_hs, kombi, start, end, amt_super)
                                funclogger.info(f"daten sind: relation (func_apis, amt, new_hs, kombi, start, end, amt_super): {relation}")
                                write_relation(per_obj, relation, src_inf, case=case)

                            elif sd < 1745 and ed > 1745:
                                if logflag:
                                    case = 6
                                margin_date = "1745<1745-06-30>"

                                src = src_inf.split(";")
                                si = src[0].strip()
                                ei = src[1].strip()

                                s_new = "Von: Hof- und Ehrenkalender 1745"
                                e_new = "Bis: Hof- und Ehrenkalender 1745"

                                new_src_inf_1 = f"{si}; {e_new}"
                                new_src_inf_2 = f"{s_new}; {ei}"

                                relation_new = (func_apis, amt, new_hs, kombi, start, margin_date, amt_super)
                                write_relation(per_obj, relation_new, new_src_inf_1, case=case)

                                relation = (func_apis, amt, hs_kais, kombi, margin_date, end, amt_super)
                                write_relation(per_obj, relation, new_src_inf_2, case=case)


                            else:
                                if logflag:
                                    case = 7
                                relation = (func_apis, amt, hs_kais, kombi, start, end, amt_super)
                                write_relation(per_obj, relation, src_inf, case=case)




                        else:
                            if logflag:
                                case=8
                            relation = (func_apis, amt, hs, kombi, start, end, amt_super)
                            write_relation(per_obj, relation, src_inf, case=case)



                    if not amt:
                        if logflag:
                            case = 9
                        relation = (func_apis, amt, None, None, start, end, amt_super)
                        write_relation(per_obj, relation, src_inf, case=case)





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


def create_all_textypes(person: object, src_base: object):
    all_fields = {}

    for key, value in person.items():
        all_fields[key] = value

    id = all_fields["Nr"]

    if check_key("Geschlecht", all_fields):
        gender_index = all_fields["Geschlecht"]
        all_fields["Geschlecht"] = f"{gender_dic[gender_index]} (field value: {gender_index})"

    if check_key("Konfession", all_fields):
        konf_index = all_fields["Konfession"]
        all_fields["Konfession"] = f"{konfession_dic[konf_index]} (field value: {konf_index})"

    if check_key(person["Nr"], per_unt):
        all_fields["Untergebene"] = per_unt[person["Nr"]]

    else:
        all_fields["Untergebene"] = "-"



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
        fieldname = f"Acc. {key}"
        tt, created = TextType.objects.get_or_create(name=fieldname, entity="Person")
        src_base["pubinfo"] += f"/original text from field: {key}"
        if len(src_base["pubinfo"]) > 400:
            src_base["pubinfo"] = src_base["pubinfo"][:300]  #todo: wie mit den zu langen Angaben verfahren?
        st = Source.objects.create(**src_base)
        text = Text.objects.create(text=str(value), source=st, kind=tt)
        per_obj.text.add(text)


class global_supernum:
    lst = []
    processed = False


def get_supernum(person):
    for c in person["Titel"].split(","):
        c = c.strip()
        if "s.n." in c:
            start, end = None, None
            match = re.search(r"(\d{4}-\d{2})|(\d{4})", c)
            if match:
                date = match.group(0)
                if len(date) == 4:
                    start = int(date)
                else:
                    start = date[:4]
                    end = int(date[:2]+date[-2:])

                global_supernum.lst.append((start, end, False)) # the tird parameter signals if the sn was later written, if not, add them to a collection for manual check

collection_team=["MRomberg", "MKaiser", "CStandhartinger"]
TC = [member+" (ACCESS)" for member in collection_team]
splitcount = len(TC)
lst_offs = list(range(0, len(per), int(len(per) / splitcount) + 1))
collection_counter = zip(lst_offs, TC)

username = "GPirgie"
log_metainfo(username=username, collection="Import ACCESS full 12-10-21", msg="Test Import after Full import in DB with logging", logger=logger)

step_o_meter = 0
for idx5, col_tuple in enumerate(collection_counter):
    offs, split_collection = col_tuple
    if idx5 == len(lst_offs) - 1:
        off_end = len(cfg.df)
    else:
        off_end = lst_offs[idx5 + 1] - 1

    with reversion.create_revision():
        me = User.objects.get(username=username)

        if me:
            reversion.set_user(me)
        reversion.set_comment(f"Import ACCESS full 16-9-21")
        src_base = {
            "orig_filename": f"Kubiska-Scharl_Poelzl_Acess-DB_Wr-Hof_2020-09-01",
            "pubinfo": "",
        }

        col_vacat_acc, created = Collection.objects.get_or_create(name="Access DB 'VACAT'")


        for person in per[offs: off_end]:
            orig_idx: int = person["Nr"]
            print(f"{step_o_meter}/{len(per)} --- orig_idx: {orig_idx}")
            funclogger.info(f"\n\n{step_o_meter}/{len(per)} --> orig_idx:{orig_idx}")
            inf_src = []
            r_ges = None
            r_nach = None
            r_vor = None
            untergebene = None
            r_konf = None
            r_tit = None
            r_bem = None
            pers = {}
            nach_alts = []
            nach_verheiratet = []
            vorname = ""
            gender = ""
            titels = []
            global_he.data = []
            global_supernum.lst = []
            global_supernum.processed = False


            #if not str(orig_idx) in ["1881",]:
            #   continue

            if not check_key("Nachname", person) and not check_key("Vorname", person):
                continue

            if check_key("Geschlecht", person):
                r_ges = person["Geschlecht"]
                gender = process_geschlecht(r_ges)
                pers["gender"] = gender

            if check_key("Nachname", person):
                r_nach = person["Nachname"]
                matches = re.finditer(r"\(", r_nach)
                if len(list(matches)) > 1:
                    funclogger.info(f"this is longer {r_nach}")
                nachname, nach_alts, nach_verheiratet, ält_jüng, titels, gender = process_nachname(r_nach, gender)
                pers["name"] = nachname
                if not pers["name"]:
                    breakpoint()

            if check_key("Vorname", person):
                r_vor = person["Vorname"]
                vorname, vor_alts, titels = process_vorname(r_vor, gender, ält_jüng, titels)
                if re.search(r"\svon", vorname) or re.search(r"\.von", vorname):
                    vorname_new = vorname.strip(" von").strip()
                    titels.append("v.")
                    funclogger.info(f"von was in vorname, vorname new: {vorname_new}, alt:{vorname}, titels: {titels}")
                    vorname = vorname_new
                    if vorname == "Maria Anna von d.J.":
                        vorname = "Maria Anna d.J."

                pers["first_name"] = vorname


            src_base = {"orig_filename": "ACCESS DB", "pubinfo": f"DUMMY", "orig_id": str(orig_idx)}
            src = Source.objects.create(**src_base)

            pers["source"] = src
            per_obj = Person.objects.create(**pers)


            person_create_person_labels(per_obj, vor_alts, nach_alts, nach_verheiratet)

            lt_unt, created = LabelType.objects.get_or_create(name="Untergebene")
            if orig_idx in per_unt.keys():
                untergebene = process_untergebene(orig_idx)
                for u in untergebene:
                    Label.objects.create(label=u[1], label_type=lt_unt, temp_entity=per_obj, start_date_written=u[0])

            for tit in titels:
                if not nach_verheiratet and tit == "Freifrau von":
                    tit = "Freiin v."
                    funclogger.info(f"nach_verheiratet was not true and: {nach_verheiratet}")
                    funclogger.info(f"tit changed to: {tit}")

                tit_res = resolve_abbreviations_access(tit, cfg.list_abbreviations)
                t, created = Title.objects.get_or_create(
                    name=tit_res,
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
                pass

            if check_key("Titel", person):
                if "s.n." in person["Titel"]:
                    get_supernum(person)
                if orig_idx in wR_dic.keys():
                    start = wR_dic.get(orig_idx)
                    if start:
                        start = "erw. " + str(start) + f"<{start}-06-30>"
                    relation = ("Rat, wirkl.", None, None, None, start, None, None)
                    #func, amt, hs, kombi, start, end, amt_super
                    write_relation(per_obj, relation)
                    write_titel_datiert_label(per_obj, "wirkl. Rat", start, None)


                if per_obj.name == "vacat":
                    acc_tit, created = LabelType.objects.get_or_create(name="vacat (ACCESS) Titel")
                    Label.objects.create(label=person["Titel"], label_type=acc_tit, temp_entity=per_obj)


                r_tit = person["Titel"]
                process_title(r_tit, per_obj, gender)

            if check_key(orig_idx, per_amt):
                rel = per_amt[orig_idx]
                process_relations(rel, orig_idx, per_obj)

            create_all_textypes(person, src_base)

            if check_key("Bemerkungen", person):
                if per_obj.name == "vacat":
                    acc_bem, created = LabelType.objects.get_or_create(name="vacat (ACCESS) Bemerkungen")
                    Label.objects.create(label="Check Bemerkungen Field below", label_type=acc_bem, temp_entity=per_obj)
                r_bem = person["Bemerkungen"]
                process_bemerkungen(per_obj, r_bem)

            if per_obj.name == "vacat":
                per_obj.collection.add(col_vacat_acc)

            if global_supernum.lst:
                """ 
                Checks if Supernum with Date was mapped to a Function. 
                If not, person is added to a collection for manual check.
                """
                for (x, y, flag) in global_supernum.lst:
                    if not flag:
                        col_supernum_check, created = Collection.objects.get_or_create(name="Access Supernumerarius: Needs Manual Check")
                        per_obj.collection.add(col_supernum_check)

            col_access_imp, created = Collection.objects.get_or_create(name="Import ACCESS full 16-9-21")

            if global_he.data:
                for r in per_obj.personinstitution_set.all():
                    sd = r.start_date
                    ed = r.end_date
                    for he_date in global_he.data:
                        if sd and ed:
                            if int(sd.year) <= int(he_date) <= int(ed.year):
                                quelle = f"{he_date} h.e."
                                if r.references:
                                    references = r.references + f"; {quelle}"
                                else:
                                    references = f"{quelle}"
                                r.references = references
                                r.save()
                        elif sd:
                            if int(sd.year) == int(he_date):

                                quelle = f"{he_date} h.e."
                                if r.references:
                                    references = r.references + f"; {quelle}"
                                else:
                                    references = f"{quelle}"

                                r.references = references

                                r.save()
            if per_obj.name == "uit de Souches":
                per_obj.name = "Ratuit de Souches"
                per_obj.title.remove(Title.objects.get(name="Rätin"))
                per_obj.save()

            per_obj.collection.add(col_access_imp)
            s_col, crt = Collection.objects.get_or_create(name=split_collection)
            per_obj.collection.add(s_col)
            step_o_meter += 1
            funclogger.info(f"ID in DB is: {per_obj.id}")
            funclogger.info(call_counter)
            print(call_counter)

