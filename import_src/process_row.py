import os, django, re
os.environ["DJANGO_SETTINGS_MODULE"] = "django_settings.viecpro_testing"
django.setup()
from copy import deepcopy

from tests.helper_functions.get_data_variables import *
from import_src.file_processing import (
    resolve_abbreviations,
)
from apis_core.apis_entities.models import Person
from apis_core.apis_vocabularies.models import Title
from apis_core.apis_metainfo.models import Source, Text
from apis_core.apis_relations.models import (
    PersonInstitution,
    InstitutionInstitution,
)
from apis_core.apis_labels.models import Label
from apis_core.apis_vocabularies.models import (
    TextType,
)

from import_src.create_database_objects import *
import logging

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def person_process_field_vorname(r_vor):
    logger.debug("person_process_field_vorname")
    if isinstance(r_vor, str):
        vorname = r_vor.split(",")
    else:
        vorname = ["NN"]

    return vorname


def person_process_field_familienname(r_fam, idx):
    if ", de" in r_fam:
        logger.debug(f"Init test: 'de in fam' INPUT: {r_fam} END INPUT, ROW:{idx}")
    if ", von" in r_fam:
        logger.debug(f"Init test: 'von in fam' INPUT: {r_fam} END INPUT, ROW:{idx}")
    familienname = r_fam
    if isinstance(familienname, str):
        labels_fam_hzt = []
        labels_fam = False
        for idx_fam, familienname in enumerate(familienname.split(", oo")): # todo: split on "Ioo or IIoo also"
            if "(" in familienname:
                fam1_ = familienname.split(" (")[0] #todo: also process "[" or "]" maybe simply replace [ with ( at the start
                fam2 = re.search(r"\(([^\)]+)\)", familienname) # todo: looks for closing ) but there are nested () like so: (idx = 12180) (Carraffa, Carraffa et Eril (1690, 1691))
                logger.debug(f"here: {fam2.group(0)}")
                if fam2:
                    fam2 = fam2.group(1).split(",") # todo: split on semikolon also / only
                    fam2 = [x.strip() for x in fam2]
            else: # todo: don't split ,von , de
                fam1_ = familienname
                fam2 = False
            if idx_fam == 0:
                fam1 = fam1_
                labels_fam = fam2
            else:
                fam1 = "NEEDS REVIEW"
                labels_fam_hzt.append((fam1_, fam2))
    else:
        fam1 = "NEEDS REVIEW"

    #todo: maybe split function here
    sn, sn_alternative = helper_process_names(fam1)
    if sn_alternative is not None:
        if not labels_fam:
            labels_fam = []
        else:
            prep_proc_name = []
            for idx4, sn_a in enumerate(labels_fam):
                sn_a_1, sn_a_2 = helper_process_names(sn_a)
                if sn_a_2 is not None:
                    labels_fam[idx] = sn_a_1
                    prep_proc_name.append(sn_a_2)
            if len(prep_proc_name) > 0:
                labels_fam.extend(prep_proc_name)
        labels_fam.append(sn_alternative)

    if ", de" in r_fam:
        logger.debug(f"sn {sn}, labels_fam {labels_fam}, labels_fam_hzt {labels_fam_hzt}")
    if ", von" in r_fam:
        logger.debug(f"sn {sn}, labels_fam {labels_fam}, labels_fam_hzt {labels_fam_hzt}")

    return sn, labels_fam, labels_fam_hzt


def helper_process_names(name):
    s1 = re.search(r"([a-zA-Z][a-z]+)/((.+)/)", name)
    if s1:
        return s1.group(1), f"{s1.group(1)}{s1.group(2)}"
    else:
        return name, None


def person_process_field_gender(r_ges, pers_dic):
    if isinstance(r_ges, str):
        if r_ges == "M":
            res = "male"
        elif r_ges == "W":
            res = "female"

        pers_dic["gender"] = res

    return pers_dic


def person_process_field_lebensdaten(r_leb, pers_dic):
    # todo: process this better, with HSV cases in mind
    if isinstance(r_leb, str):
        if "gest. " in r_leb:
            pers_dic["end_date_written"] = r_leb.replace("gest. ", "")

    return pers_dic


def person_process_field_titel(r_tit, pers):
    t_list, subst = re.subn(r"\(.*\)", "", r_tit)
    t_list = [x.strip() for x in t_list.split(",")]
    for tit in t_list:
        t2, created = Title.objects.get_or_create(
            name=resolve_abbreviations(tit, list_abbreviations)
        )
        pers.title.add(
            t2)


def person_create_person_labels(pers, vorname, labels_fam, labels_fam_hzt):
    if len(vorname) > 1:
        for v in vorname[1:]:
            Label.objects.create(
                label=v, label_type=lt_vorname_alternativ, temp_entity=pers
            )

    if labels_fam:
        for f in labels_fam:
            Label.objects.create(
                label=f, label_type=lt_nachname_alternativ, temp_entity=pers
            )

    for f in labels_fam_hzt:
        Label.objects.create(
            label=f[0], label_type=lt_nachname_verheiratet, temp_entity=pers
        )

        if f[1]:
            for f2 in f[1]:
                Label.objects.create(
                    label=f2,
                    label_type=lt_nachname_alternativ_verheiratet,
                    temp_entity=pers,
                )

def create_and_process_doc(r_F, idx, nlp):
    doc = nlp.make_doc(r_F)

    doc._.excel_row = idx

    for name, proc in nlp.pipeline:
        doc = proc(doc)

    return doc


def chunk_process_datum(c_D, rel):
    if len(c_D) == 1:
        if "bis" in c_D[0]:
            rel["end_date_written"] = c_D[0]
        else:
            rel["start_date_written"] = c_D[0]
    elif len(c_D) == 2:
        rel["start_date_written"] = c_D[0]
        rel["end_date_written"] = c_D[1]

    return rel


def chunk_get_nm_hst(c_H, r_H, idx_chunk):
    test_hs = False
    if c_H:
        if c_H in lst_hofst.keys():
            nm_hst = lst_hofst[c_H][0]
        else:
            nm_hst = c_H
    elif isinstance(r_H, str) and idx_chunk == 0:  # write first hofstaat - no longer needed in hsv
        test_hs = True
        nm_hst = r_H.strip()
        for hst in [x.strip() for x in r_H.split(";")]:
            if hst in lst_hofst.keys():
                nm_hst = lst_hofst[hst][0]  # potentially overwrites already found nm_hst (?)
    else:
        nm_hst = "Dummy Hofstaat"

    return nm_hst, test_hs

def chunk_create_institution(nm_hst, test_hs, pers):
    inst, created = Institution.objects.get_or_create(
        name=nm_hst, kind=inst_type_hst

    )

    inst.collection.add(col_unsicher)
    if test_hs:
        PersonInstitution.objects.get_or_create(
            related_institution=inst, related_person=pers, relation_type=rl_pers_hst
        )

    return inst


def get_or_create_amt(row, df_aemter_index, lst_hofst):
    beh_name = re.match(r"^(.+)[/\\(]", row["Amt/Behörde"])
    if beh_name:
        amt = beh_name.group(1).strip()
    else:
        amt = row["Amt/Behörde"].strip()
    try:
        row_target = df_aemter_index[amt]
    except:
        raise ValueError(f"couldnt find matching Amt entry for {amt}")
    if isinstance(row.iloc[11], str):
        if row.iloc[11] in lst_hofst.keys():
            name_hst_1 = lst_hofst[row.iloc[11]][0]
        else:
            name_hst_1 = row.iloc[11]
    else:
        name_hst_1 = "Dummy Hofstaat"
    test_unsicher = False
    if name_hst_1 == "UNSICHER - Collection, manuelle Entscheidung":
        name_hst_1 = "UNSICHER"
        test_unsicher = True
    name_hst_1 = name_hst_1.replace('Hofstaat', '').strip()
    inst_type, c3 = InstitutionType.objects.get_or_create(name=row_target.iloc[7])
    amt_ent, created = Institution.objects.get_or_create(name=f'{row_target["APIS-Vereinheitlichung"]} ({name_hst_1})', kind=inst_type)
    amt_ent.collection.add(col_unsicher)
    amt_super = False
    created2 = False
    if created:
        if row_target.iloc[8] in lst_hofst.keys():
            name_hst_2 = lst_hofst[row_target.iloc[8]][0]
        else:
            name_hst_2 = row_target.iloc[8]
        amt_super, created2 = Institution.objects.get_or_create(name=name_hst_2, kind=inst_type)
        InstitutionInstitution.objects.create(related_institutionA_id=amt_ent.pk, related_institutionB_id=amt_super.pk, relation_type=rl_teil_von)
    return amt_ent, amt_super, created2



def create_text_entries(row, pers, df, src_base, map_text_type, columns=(5,18)):
    for idx in range(5,18):
        if isinstance(row.iloc[idx], str):
            c_name = df.columns[idx]
            if c_name not in map_text_type.keys():
                tt, created = TextType.objects.get_or_create(name=c_name, entity="Person")
                map_text_type[c_name] = tt
            src_base_1 = deepcopy(src_base)
            src_base_1["pubinfo"] += f"/noriginal text from: {c_name} column"
            st = Source.objects.create(**src_base_1)
            t_fin = Text.objects.create(text=row.iloc[idx], source=st, kind=map_text_type[c_name])
            pers.text.add(t_fin)

    return map_text_type


def chunk_process_amt(c_A, r_A, c_H, inst, nm_hst, idx_chunk, row):
    if isinstance(r_A, str) and idx_chunk == 0:
        try:
            inst2, inst3, created = get_or_create_amt(
                row, df_aemter_index, lst_hofst
            )
            if created:
                InstitutionInstitution.objects.create(
                    related_institutionA=inst3,
                    related_institutionB=inst,  # inst comes again
                    relation_type=rl_teil_von,
                )
        except Exception as e:
            inst2 = amt_dummy
            inst3 = False
            #logger.debug(f"Exception in Amt function: {e}")
            logger.debug(f"Exception in Amt function: {e}")

    elif c_A is not None:
        if c_A.strip() in df_aemter_index.keys():
            amt = df_aemter_index[c_A.strip()].iloc[8]
        else:
            amt = c_A.strip()

        if idx_chunk == 0 or c_H is not None:
            amt_name = f"{amt} ({nm_hst})"
        else:
            amt_name = f"{amt} (Dummy Hofstaat)"
        logger.debug(amt_name)
        if len(amt_name) > 254:
            logger.debug(f"len amtname {(len(amt_name) - 250)}")
            ln_minus = len(f"{amt_name} ({nm_hst})") - 250
            logger.debug(f"{amt_name[:-ln_minus]} ({nm_hst})")
            amt_name = f"{amt_name[:-ln_minus]} ({nm_hst})"
        inst2, created = Institution.objects.get_or_create(name=amt_name)
    else:
        inst2, created = Institution.objects.get_or_create(
            name=f"Dummy Amt ({nm_hst})"
        )
    return inst2

def chunk_create_relations(c_F, rel):
    if len(c_F) > 0:
        for rel_type in c_F:
            try:
                (rel_t_obj,
                 created,
                 ) = PersonInstitutionRelation.objects.get_or_create(name=rel_type)
                rel["relation_type"] = rel_t_obj
                PersonInstitution.objects.create(**rel)

            except Exception as e:
                logger.debug(f"Exception: {e}")


def process_chunks(doc, pers, row):
    for idx_chunk, c in enumerate(doc._.chunks):

        c_D = c["DATUM"]
        c_F = c["FUNKTION"]
        c_H = c["HOFSTAAT"]
        c_A = c["AMT"]
        r_H = row["Hofstaat"]
        r_A = row["Amt/Behörde"]

        rel = {"related_person": pers}
        if c_D is not None:
            rel = chunk_process_datum(c_D, rel)

        nm_hst, test_hs = chunk_get_nm_hst(c_H, r_H, idx_chunk)

        test_unsicher = False
        if nm_hst == "UNSICHER - Collection, manuelle Entscheidung": #todo: isn't there a new collection unsicher only for Hofstaate?
            nm_hst = "UNSICHER"
            test_unsicher = True

        inst = chunk_create_institution(nm_hst, test_hs, pers)

        inst2 = chunk_process_amt(c_A, r_A, c_H, inst, nm_hst, idx_chunk, row)


        rel["related_institution"] = inst2

        chunk_create_relations(c_F, rel)

    return pers


def process_row(idx, row, src_base, col, nlp):
    r_vor = row["Vorname"]
    r_fam = row["Familienname"]
    r_ges = row["Geschlecht"]
    r_leb = row["Lebensdaten"]
    r_fun = row["Funktion"]
    r_tit = row["Titel"]

    vorname = person_process_field_vorname(r_vor)

    vn, vn_alternative = helper_process_names(vorname[0]) #--------> here is already a helper function
    if vn_alternative:
        vorname.append(vn_alternative)

    sn, labels_fam, labels_fam_hzt = person_process_field_familienname(r_fam, idx)

    pers_dic = {"first_name": vn, "name": sn}
    pers_dic = person_process_field_gender(r_ges, pers_dic)
    pers_dic = person_process_field_lebensdaten(r_leb, pers_dic)

    src_base["orig_id"] = idx
    src = Source.objects.create(**src_base)

    pers_dic["source"] = src

    pers = Person.objects.create(**pers_dic)

    if isinstance(r_tit, str):
        person_process_field_titel(r_tit, pers)

    person_create_person_labels(pers, vorname, labels_fam, labels_fam_hzt)

    map_text_type = {} #Brauchen wir die mehrmals oder kann die in der Funktion einfach kreiert werden?
    map_text_type = create_text_entries(row, pers, df, src_base, map_text_type)
    col, created = Collection.objects.get_or_create(name="Import HZAB full 10-3-21")
    pers.collection.add(col)

    if not isinstance(r_fun, str):
        return pers

    doc = create_and_process_doc(r_fun, idx, nlp)
    pers = process_chunks(doc, pers, row)

    return pers
