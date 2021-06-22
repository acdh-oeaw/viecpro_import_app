import re
import logging
import pickle

from viecpro_import_app.import_src.create_database_objects import create_all
from copy import deepcopy
from import_src.file_processing import resolve_abbreviations

from apis_core.apis_entities.models import Person
from apis_core.apis_vocabularies.models import Title, InstitutionType
from apis_core.apis_metainfo.models import Source, Text
from apis_core.apis_relations.models import PersonInstitution, InstitutionInstitution
from apis_core.apis_labels.models import Label
from apis_core.apis_vocabularies.models import TextType
from import_src.create_database_objects import *

global cfg

logger = logging.getLogger("import_logger")
funclogger = logging.getLogger("func_logger")
caselogger = logging.getLogger("case_logger")

def person_process_field_vorname(r_vor):
    funclogger.info(f"r_vor")
    keep = [", sen.", ", jun.", ", der jüngere", ", die jüngere"]
    if isinstance(r_vor, str):
        temp = r_vor.replace("(?)", "?")
        for k in keep:
            c = k.replace(",", "#")
            temp = re.sub(k, c, temp)
        vorname = re.split(r",|;|\(|\[|\)|\]", temp)

        while "" in vorname:
            vorname.remove("")

        vorname = [v.replace("#", ",") if "#" in v else v for v in vorname]

    else:
        vorname = ["NN"]

    return vorname


def person_process_field_familienname(r_fam, idx):
    funclogger.info(cfg.df.Familienname.iloc[idx])
    if isinstance(r_fam, str):
        if ", de" in r_fam:
            funclogger.info(f"Init test: 'de in fam' INPUT: {r_fam} END INPUT, ROW:{idx}")
        if ", von" in r_fam:
            funclogger.info(f"Init test: 'von in fam' INPUT: {r_fam} END INPUT, ROW:{idx}")
        familienname = r_fam
        familienname = familienname.replace("(?)", "?")
    else:
        familienname = None
    labels_fam_hzt = []
    labels_fam = False
    if isinstance(familienname, str):
        #labels_fam_hzt = []
        #labels_fam = False
        for idx_fam, familienname in enumerate(familienname.split(", oo")):
            if "(" in familienname:
                fam1_ = familienname.split(" (")[0].strip()
                fam2 = re.search(r"\(([^\)]+)\)", familienname)
                funclogger.info(f"fam1: {fam1_}")
                funclogger.info(f"fam2: {fam2}")
                if fam2:
                    if ";" in fam2.group(1):
                        fam2 = fam2.group(1).split(";")
                        fam2 = [x.strip() for x in fam2]
                    elif not ", von" in fam2.group(1) and not ", de" in fam2.group(1) and not ", d'" in fam2.group(1): # todo added this to catch , d' names. are there other variants?
                        fam2 = fam2.group(1).split(",")
                        fam2 = [x.strip() for x in fam2]
                    else:
                        temp_ = []
                        temp_.append(fam2.group(1).strip())
                        fam2 = temp_
            elif "[" in familienname:
                fam1_ = familienname.split(" [")[0].strip()
                fam2 = re.search(r"\[([^\]]+)\]", familienname)
                funclogger.info(f"fam1: {fam1_}")
                funclogger.info(f"fam2: {fam2}")
                if fam2:
                    if ";" in fam2.group(1):
                        fam2 = fam2.group(1).split(";")
                        fam2 = [x.strip() for x in fam2]
                    elif not ", von" in fam2.group(1) and not ", de" in fam2.group(1) and not ", d'" in fam2.group(1):
                        fam2 = fam2.group(1).split(",")
                        fam2 = [x.strip() for x in fam2]
                    else:
                        temp_ = []
                        temp_.append(fam2.group(1).strip())
                        fam2 = temp_
            else:
                fam1_ = familienname.strip()
                funclogger.info(f"Used fam1_ = familienname.strip(), fam1_ = {fam1_}")
                if "," in fam1_:
                    funclogger.info(f"WHAT TO DO WITH THESE NAME: {fam1_}")
                fam2 = False
            if fam2:
                fam_temp = deepcopy(fam2)
                for f in fam2:
                    if "/" in f:
                        if not re.search(r"\s", f):
                            fam_temp.remove(f)
                            fam_temp += f.split("/")
                        else:
                            pattern = re.compile(r"\s*[A-Za-zÄÖÜäöüß]+/[A-Za-zÄÜÖäöüß]+\s*")
                            match = re.search(pattern, f)
                            match = match.group(0)
                            m_split = match.split("/")
                            fam_temp.remove(f)
                            for m in m_split:
                                if not m.endswith(" "):
                                    m = m + " "
                                variant = re.sub(pattern, m, f)
                                fam_temp.append(variant)
                fam2 = fam_temp

            if idx_fam == 0:
                fam1 = fam1_
                labels_fam = fam2
            else:
                fam1 = "NEEDS REVIEW"
                labels_fam_hzt.append((fam1_, fam2))
    else:
        fam1 = "NEEDS REVIEW"


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
    if isinstance(r_fam, str):
        if ", de" in r_fam:
            funclogger.debug(f"sn {sn}, labels_fam {labels_fam}, labels_fam_hzt {labels_fam_hzt}")
        if ", von" in r_fam:
            funclogger.debug(f"sn {sn}, labels_fam {labels_fam}, labels_fam_hzt {labels_fam_hzt}")

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
    if isinstance(r_leb, str):
        pattern = re.compile(r"((\d{4}-\d{2}-\d{2})|(\d{4}))(\sbis\s(\d{4}-\d{2}-\d{2})|(\d{4}))")
        match = re.search(pattern, r_leb)
        if match:
            b = match.group(1)
            d = match.group(4).strip()
            if b.endswith("-00-00"):
                birth = b + f"<{b[0:4]}-06-30>"
            else:
                birth = b + f"<{b}>"

            if d.endswith("-00-00"):
                death = d[4:8] + f"<{d[4:8]}-06-30>"
            else:
                death = d[4:] + f"<{d[4:]}>"
        elif re.match(r"(tot v.)|(gest. )|(tot n.)", r_leb):
            date = re.search(r"((\d{4}-\d{2}-\d{2})|(\d{4}))", r_leb).group(0)
            if date.endswith("-00-00"):
                death = r_leb + f"<{date[0:4]}-06-30>"
            elif len(date) == 4:
                death = r_leb + f"<{date}-06-30>"
            else:
                death = r_leb + f"<{date}>"
            birth = None

        else:
            funclogger.info(f"NOT PARSED LEBENSDATEN -> {r_leb}")
            birth = None
            death = None
        if birth:
            pers_dic["start_date_written"] = birth
        if death:
            pers_dic["end_date_written"] = death
    return pers_dic


def person_process_field_titel(r_tit, pers):

    change = {"Reichsgraf; Freiherr zu Hollenburg, Finkenstein u. Thalberg, Erbschenk u. Oberstlandjägermeister in Kärnten, Herr der Herrschaft Schuckenau, Großpriesten, Obermarkersdorf u. Fugau"
              :"Reichsgraf; Freiherr zu Hollenburg, Finkenstein u. Thalberg; Erbschenk u. Oberstlandjägermeister in Kärnten; Herr der Herrschaft Schuckenau, Großpriesten, Obermarkersdorf u. Fugau",
              }

    dont_split = ["Herr der Herrschaft Eschelberg, Liechtenhag u. Pottendorf",
                  "Herr der Herrschaft Wildberg, Rindegg, Reichenau",
                  "Graf, de Capelli",
                  "Freiherr zu Hollenburg, Finkenstein u.Thalberg",
                  "Herr der Herrschaft Schuckenau, Großpriesten, Obermarkersdorf u. Fugau",
                  'Freiherr 1710 - 06 - 28 "Edler Herr von Ludwigstorff, Freiherr von Goldlamb',
                  ]

    funclogger.warning(f"t_tit = {r_tit}")
    if r_tit in change.keys():
        r_tit = change[r_tit]
    t_list, subst = re.subn(r"\(.*\)", "", r_tit)
    t_list = [x.strip() for x in t_list.split(";")] # changed from "," to ";" for HSV
    funclogger.warning(f"t_list = {t_list}")

    def create_title(tit, pers):
        if re.search(r"\d{4}", tit):

            tit_res = resolve_abbreviations(tit, cfg.list_abbreviations)
            date = re.search(r"(\d{4}){1}(-\d{2}-\d{2}){0,1}", tit).group(0)
            date = helper_hsv_post_process_dates(date)
            funclogger.warning(f"tit_res: {tit_res}, date after processing: {date}")
            Label.objects.create(
                label_type=lt_titel_datiert,
                temp_entity=pers,
                label=tit_res,
                start_date_written=date
            )

        t2, created = Title.objects.get_or_create(
            name=resolve_abbreviations(tit, cfg.list_abbreviations)
        )
        pers.title.add(t2)

        return pers

    for tit in t_list:
        if "," in tit and tit not in dont_split:
            funclogger.warning(f"Komma in tit: {tit}")
            for t in tit.split(","):
                t = t.strip()
                pers = create_title(t, pers)
                funclogger.warning(f"CREATING SPLIT TITLE {t} from {tit}")
        else:
            pers = create_title(tit, pers)

    return pers




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

def create_and_process_doc(r_F, idx):
    doc = cfg.nlp.make_doc(r_F)

    doc._.excel_row = idx

    for name, proc in cfg.nlp.pipeline:
        doc = proc(doc)

    return doc

def helper_hsv_post_process_dates(date):
    if "<" in date and ">" in date:
        i_date = re.search(r"<.*>", date).group(0)
        funclogger.info(f"inner date: {i_date}")
        new_i_date = deepcopy(i_date)
        new_i_date = new_i_date.replace("-00-00", "-06-30")
        funclogger.info(f"new_i_date: {new_i_date}")
        new_date = re.sub(i_date, new_i_date, date)
    else:
        if len(date.strip()) == 10:
            if "-00-00" in date:
                i_date = "<"+date[:4]+"-06-30>" #note: sliced down to replace orig  f.e. 1667-00-00 with 1667<1667-06-30>
                new_date = date[:4]+i_date
            else:
                new_date = f"{date}<{date}>"
        elif len(date.strip()) == 4:
            new_date = f"{date}<{date}-06-30>"
        else:
            new_date = date

    funclogger.info(f"old: {date}, new: {new_date}")

    return new_date


def chunk_process_datum(c_D, rel):

    if len(c_D) == 1:
        c_D[0] = helper_hsv_post_process_dates(c_D[0])
        if "bis" in c_D[0]:
            rel["end_date_written"] = c_D[0]
            funclogger.info(f"rel, chunk edw: {c_D[0]}")
        else:
            rel["start_date_written"] = c_D[0]
            funclogger.info(f"rel, chunk sdw: {c_D[0]}")

    elif len(c_D) == 2:
        c_D[0] = helper_hsv_post_process_dates(c_D[0])
        c_D[1] = helper_hsv_post_process_dates(c_D[1])
        rel["start_date_written"] = c_D[0]
        funclogger.info(f"rel, chunk sdw: {c_D[0]}")

        rel["end_date_written"] = c_D[1]
        funclogger.info(f"rel, chunk edw: {c_D[0]}")
    funclogger.info(f" this is the full relation: {rel}")

    return rel


def chunk_get_nm_hst(c_H):
    funclogger.info(f"c_H = {c_H}")
    if c_H in cfg.lst_hofst.keys():
        nm_hst = cfg.lst_hofst[c_H][0]
    else:
        nm_hst = c_H

    return nm_hst

def chunk_create_institution(nm_hst, pers):
    inst_type_hst, created = InstitutionType.objects.get_or_create(name="Hofstaat")
    inst, created = Institution.objects.get_or_create(
        name=nm_hst, kind=inst_type_hst
    )

    inst.collection.add(col_unsicher) # todo: ?

    #This is used now instead of test_hs
    if nm_hst != "Dummy Hofstaat":
        funclogger.info(f"nm_hst is not Dummy Hofstaat: {nm_hst}")
        PersonInstitution.objects.get_or_create(
                 related_institution=inst, related_person=pers, relation_type=rl_pers_hst
             )

    return inst



def get_or_create_amt(amt_name, row_target):

    inst_type, c3 = InstitutionType.objects.get_or_create(name=row_target.iloc[7])
    amt_ent, created = Institution.objects.get_or_create(name=amt_name, kind=inst_type)
    amt_ent.collection.add(col_unsicher)
    amt_super = False
    created2 = False
    if created:
        if row_target.iloc[8] in cfg.lst_hofst.keys():
            name_hst_2 = cfg.lst_hofst[row_target.iloc[8]][0]
        else:
            name_hst_2 = row_target.iloc[8]
        amt_super, created2 = Institution.objects.get_or_create(name=name_hst_2, kind=inst_type)
        InstitutionInstitution.objects.create(related_institutionA_id=amt_ent.pk, related_institutionB_id=amt_super.pk, relation_type=rl_teil_von)

    funclogger.info(f"Created amt_ent {amt_ent}, amt_super {amt_super}")
    return amt_ent, amt_super, created2

def create_text_entries(row, pers, src_base, map_text_type, columns=(5,18)):
    for idx in range(len(cfg.df.columns)): # todo note: changed. to import all columns
        if isinstance(row.iloc[idx], str):
            col_name = cfg.df.columns[idx]
            if col_name not in map_text_type.keys():
                tt, created = TextType.objects.get_or_create(name=col_name, entity="Person")
                map_text_type[col_name] = tt
            src_base_1 = deepcopy(src_base)
            src_base_1["pubinfo"] += f"/noriginal text from: {col_name} column"
            st = Source.objects.create(**src_base_1)
            t_fin = Text.objects.create(text=row.iloc[idx], source=st, kind=map_text_type[col_name])
            pers.text.add(t_fin)

    return map_text_type


def db_deduplicate_aemter(amt_name):

    duplicate_list = list(Institution.objects.filter(name=amt_name))
    keep = duplicate_list.pop(0)
    for el in duplicate_list: # re_write PersonInstitution Relations
        pi_set = PersonInstitution.objects.filter(related_institution=el)
        for entry in pi_set:
            old = entry.related_institution
            entry.related_institution=keep
            entry.save()
            funclogger.info(f"Set PersInst Relation for {el} from {old}, {old.id} to {keep}, {keep.id}")

    for el in duplicate_list: # re_write InstitutionInstitution Relations
        ii_set = InstitutionInstitution.objects.filter(related_institutionA=el)
        for entry in ii_set:
            old = entry.related_institutionA
            entry.related_institutionA=keep
            entry.save()
            funclogger.info(f"Set InstInst Relation for {el} from {old}, {old.id} to {keep}, {keep.id}")

    while duplicate_list: # delete obsolete Institutions
        remove = duplicate_list.pop()
        remove.delete()
        funclogger.info(f"Deleted obsolete Institution {remove}")



def chunk_process_amt(c_A, inst, nm_hst):
    row_target = None
    if c_A:
        funclogger.info(f"c_A in if c_A true: {c_A}")

        amt = c_A.strip()
        if amt in cfg.df_aemter_index.keys():
            row_target = cfg.df_aemter_index[amt]
            resolved = row_target.iloc[5] #todo: note: used column 9 first, which is "Bedeutung (Auflösung)", changed to column 5 "APIS_Vereinheitlichung"

            if isinstance(resolved, str):
                amt = resolved
            else:
                funclogger.info(f"Resolved amt was nan: {resolved}")

            funclogger.info(f"c_A was: {c_A} and amt after matching with amt index is: {amt}, type amt: {type(amt)}")
        else:
            funclogger.info(f"amt {amt} was not in df_aemter_index")
            if not amt == "Dummy Amt":
                caselogger.info(f"{amt}")
        amt_name = f"{amt} ({nm_hst})"

        if len(amt_name) > 254:
            funclogger.debug(f"len amtname {(len(amt_name) - 250)}")
            ln_minus = len(f"{amt_name} ({nm_hst})") - 250
            funclogger.debug(f"{amt_name[:-ln_minus]} ({nm_hst})")
            amt_name = f"{amt_name[:-ln_minus]} ({nm_hst})"
        try:
            inst2, created = Institution.objects.get_or_create(name=amt_name)

        except Exception as e:
            funclogger.info(f"Exception caught for duplicates for amt_name {amt_name}. Calling db_deduplicate_aemter")
            db_deduplicate_aemter(amt_name)

            try:
                inst2, created = Institution.objects.get_or_create(name=amt_name)
                funclogger.info(f"Exception resolved: inst2 is for {amt_name} is {inst2}")


            except Exception as e:
                 funclogger.info(f"Exception: {e}")
                 funclogger.info(f"exception caught and handled here. Multiple Institutions for {amt_name}")

        if not "Dummy" in amt_name and row_target is not None:
            try:
                inst2, inst3, created = get_or_create_amt(amt_name, row_target)
                if created:
                    InstitutionInstitution.objects.create(
                        related_institutionA=inst3,
                        related_institutionB=inst,
                        relation_type=rl_teil_von,
                    )
            except Exception as e:
                #inst2 = amt_dummy
                #inst3 = False
                funclogger.info(f"Exception in Amt function: {e}")
        else:
            if row_target is None:
                funclogger.info(f"Skipped writing InstitutionInstituion relation because row_target was None for amt {amt_name}")
            else:
                funclogger.info(f"Skipped writing InstitutionInstitution relation for amt_name {amt_name}")

    else:
        funclogger.info(f"Else was called, c_A was false (is this even possible?) c_A = '{c_A}'")
        inst2, created = Institution.objects.get_or_create(
            name=f"Dummy Amt ({nm_hst})"
        )

    funclogger.info(f" Return value of inst2 = {inst2}")
    return inst2


def db_deduplicate_person_institution_relation(rel_type):
    duplicate_list = list(PersonInstitutionRelation.objects.filter(name=rel_type))
    keep = duplicate_list.pop(0)
    funclogger.info(keep)
    keep_id = keep.id
    for el in duplicate_list:
        rel_id = el.id
        per_inst_set = PersonInstitution.objects.filter(relation_type_id=rel_id)
        funclogger.info(per_inst_set)
        for entry in per_inst_set:
            old_id = entry.relation_type_id
            entry.relation_type_id = keep_id
            funclogger.info(f"changed to id: {entry.relation_type_id} from {old_id}")
            entry.save()

    while duplicate_list:
        remove = duplicate_list.pop()
        remove.delete()
        funclogger.info(f"Deleted obsolete PersonInstitutionRelation: {remove}")


def chunk_create_relations(c_F, rel):
    funclogger.info(f"create relations called for c_F {c_F}")
    if len(c_F) > 0:
        for rel_type in c_F:
            try:
                (rel_t_obj,
                 created,
                 ) = PersonInstitutionRelation.objects.get_or_create(name=rel_type)
                rel["relation_type"] = rel_t_obj
                PersonInstitution.objects.create(**rel)

                if created:
                    funclogger.info(f"PersonInstitutionRelation created returned: {created}; rel['relation_type'] = {rel_t_obj}")
                else:
                    funclogger.info(f"PersonInstitutionRelation created returned: {created}; rel['relation_type'] = {rel_t_obj}")


            except Exception as e:
                funclogger.info(f"Exception for rel_type |{rel_type}| in Progres. Calling db_deduplicate_person_institution_relation")
                db_deduplicate_person_institution_relation(rel_type)
                try:
                    (rel_t_obj,
                     created,
                     ) = PersonInstitutionRelation.objects.get_or_create(name=rel_type)
                    rel["relation_type"] = rel_t_obj
                    PersonInstitution.objects.create(**rel)
                    funclogger.info(f"Excption resolved, set PersonInstitution relation for rel_type {rel_type}")
                except Exception as e:
                    funclogger.info(f"Exception: {e}, rel_type was: |{rel_type}| Could not be resolved!")


def helper_hsv_match_amt_with_funct(doc, r_A):

    first_amt = None
    funclogger.info(f"r_A = {r_A}")

    if isinstance(r_A, str):
        ab_split = r_A.split(";")
        first_amt = ab_split[0].strip()
        first_amt = first_amt.split("/")[0]
        b = re.sub(r"\(.*\)", "", first_amt).strip()
        funclogger.info(f"replaced amt '{first_amt}' with '{b}'")
        first_amt = b

        if len(ab_split) == len(doc._.chunks):

            for c, a in zip(doc._.chunks, ab_split):
                a = a.split("/")[0]
                b = re.sub(r"\(.*\)","",a).strip()
                funclogger.info(f"replaced amt '{a}' with '{b}'")
                a = b
                if not c["AMT"]:
                    c["AMT"] = a
                funclogger.info(f"r_A equals len(Chunks) -> c[amt] = {c['AMT']}")
        else:
            for idx, c in enumerate(doc._.chunks):
                if not c["AMT"] and idx == 0 and first_amt:
                    c["AMT"] = first_amt
                elif not c["AMT"]:
                    c["AMT"] = "Dummy Amt"


    for c in doc._.chunks:
        funclogger.info(f"Before check for empty amt, c[amt] = '{c['AMT']}'")
        if not c["AMT"] or c["AMT"] == "" or c["AMT"] == " ":
            c["AMT"] = "Dummy Amt"
            funclogger.info(f"Caught Empty Amt -> c[amt] set to = {c['AMT']}")

    return doc


def helper_hsv_match_hofstaate(doc, r_H):
    first_hs = None
    funclogger.info(f"r_H = {r_H}")
    if isinstance(r_H, str):
        hs_split = r_H.split(";")
        first_hs = hs_split[0].strip()

        if len(hs_split) == len(doc._.chunks):

            for c, h in zip(doc._.chunks, hs_split):
                h = h.split("/")[0] #
                funclogger.warning(f"HOFSTAATE PROCESSING ----> h: {h}")
                if not c["HOFSTAAT"]:
                    funclogger.warning("NO CHUNK HOFSTAAT")
                    c["HOFSTAAT"] = h.strip()
                    funclogger.warning(f"chunk Hofstaat set to: {c['HOFSTAAT']}")

        elif first_hs:
            for c in doc._.chunks:
                if not c["HOFSTAAT"]:
                    c["HOFSTAAT"] = first_hs

    else:
        for c in doc._.chunks:
            if not c["HOFSTAAT"]:
                c["HOFSTAAT"] = "Dummy Hofstaat"

            funclogger.info(f"chunk Hofstaat = {c['HOFSTAAT']}")

    for c in doc._.chunks:
        funclogger.info(f"chunk is -- > {c}")

    return doc




def process_chunks(doc, pers, row, idx):

    if isinstance(row["Amt/Behörde"], str):
        aemter_behörde = row['Amt/Behörde'].split(";")
    else:
        aemter_behörde = []

    r_H = row["Hofstaat"]
    r_A = row["Amt/Behörde"]

    funclogger.info(f"len_doc_chunks: {len(doc._.chunks)}, len Ämter-Spalte: {len(aemter_behörde)}")

    doc = helper_hsv_match_hofstaate(doc, r_H)
    doc = helper_hsv_match_amt_with_funct(doc, r_A)


    for idx_chunk, c in enumerate(doc._.chunks):

        c_D = c["DATUM"]
        c_F = c["FUNKTION"]
        c_H = c["HOFSTAAT"]
        c_A = c["AMT"]


        with open("data/whitespace_funcs.pkl", "rb") as file:
            missing_funcs_dict = pickle.load(file)

        if idx_chunk == 0 and idx in missing_funcs_dict.keys():
            funclogger.info(f"idx_chunk == 0 and idx in missing funcs keys.")
            funclogger.info(f"Before check for empty c_F: c_F = '{c_F}'")
            if c_F == []:
                funclogger.info(f"c_F is []")
                funct = missing_funcs_dict[idx]
                if not funct in ["Entlassung", "Landschaft", "Landesh", "Ritterstand"]:
                    c_F = [funct]
                    funclogger.info(f"Filled in Function with missing whitespace, set c_F to {c_F}")


        funclogger.info(f"cD {c_D}, cF {c_F}, cH {c_H}, CA {c_A}")

        rel = {"related_person": pers}
        if c_D is not None:
            rel = chunk_process_datum(c_D, rel)


        nm_hst = chunk_get_nm_hst(c_H)

        test_unsicher = False
        if nm_hst == "UNSICHER - Collection, manuelle Entscheidung":
            nm_hst = "UNSICHER"
            test_unsicher = True

        inst = chunk_create_institution(nm_hst, pers) # first inst ist der hofstaat

        inst2 = chunk_process_amt(c_A, inst, nm_hst)

        rel["related_institution"] = inst2

        chunk_create_relations(c_F, rel)
        try:
            funclogger.info(f" Source Person Relation: {row.orig_index} -- {pers} -- {rel}")
        except AttributeError as e:
            funclogger.info(f" Source Person Relation: {pers.source.orig_id} -- {pers} -- {rel}")

    return pers


def process_row(idx, row, src_base, conf, split_collection):
    global cfg
    cfg = conf

    if cfg.create_all:
        create_all()
        cfg.create_all = False

    r_vor = row["Vorname"]
    r_fam = row["Familienname"]
    r_ges = row["Geschlecht"]
    r_leb = row["Lebensdaten"]
    r_fun = row["Funktion"]
    r_tit = row["Titel"]

    if isinstance(r_fun, str):
        # this is a rough solution for the problem with missing whitespaces after ')' or ',' etc.
        # todo: make sure that references to the source entry are not using the substitute r_fun value created here
        # todo: something similar should be run as the very first pipeline component, as the NER can't resolve these cases!
        def replacer(match):
            if not match.group(0) == ");":
                replacement = match.group(0)[:-1]+" "+match.group(0)[-1:]
                funclogger.info(f"r_fun: replaced {match.group(0)} with: {replacement}")
            else:
                replacement = match.group(0)
            return replacement
        pattern = re.compile(r"(\)|\,|;)\S")
        r_fun = re.sub(pattern, replacer, r_fun)

    vorname = person_process_field_vorname(r_vor)

    vn, vn_alternative = helper_process_names(vorname[0])
    if vn_alternative:
        vorname.append(vn_alternative)

    sn, labels_fam, labels_fam_hzt = person_process_field_familienname(r_fam, idx)

    if re.match(r"\[.*\]", sn):
        new = sn[1:-1]
        funclogger.info(f"Replaced name in brackets: old name: {sn} -- new name:  {sn}")
        sn = new

    pers_dic = {"first_name": vn, "name": sn}
    pers_dic = person_process_field_gender(r_ges, pers_dic)
    pers_dic = person_process_field_lebensdaten(r_leb, pers_dic)


    src_base["orig_id"] = idx
    src = Source.objects.create(**src_base)

    pers_dic["source"] = src

    pers = Person.objects.create(**pers_dic)

    if isinstance(r_tit, str):
        pers = person_process_field_titel(r_tit, pers)


    person_create_person_labels(pers, vorname, labels_fam, labels_fam_hzt)

    map_text_type = {}
    map_text_type = create_text_entries(row, pers, src_base, map_text_type)
    col, created = Collection.objects.get_or_create(name=cfg.collection)
    s_col, crt = Collection.objects.get_or_create(name=split_collection)

    pers.collection.add(col)
    pers.collection.add(s_col)

    if not isinstance(r_fun, str):
        return pers

    doc = create_and_process_doc(r_fun, idx)
    pers = process_chunks(doc, pers, row, idx)



