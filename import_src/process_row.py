import os, django, re

from viecpro_import_app.import_src.create_database_objects import create_all

os.environ["DJANGO_SETTINGS_MODULE"] = "django_settings.viecpro_testing"
django.setup()
from copy import deepcopy
import time

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
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
filehandler = logging.FileHandler(f"import_src/logfiles/log_main_{timestamp}.txt", mode="w")
logger.addHandler(filehandler)
logger.setLevel(logging.INFO)


global cfg


def person_process_field_vorname(r_vor):
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
    logger.info(cfg.df.Familienname.iloc[idx])
    if isinstance(r_fam, str):
        if ", de" in r_fam:
            logger.info(f"Init test: 'de in fam' INPUT: {r_fam} END INPUT, ROW:{idx}")
        if ", von" in r_fam:
            logger.info(f"Init test: 'von in fam' INPUT: {r_fam} END INPUT, ROW:{idx}")
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
                fam1_ = familienname.split(" (")[0]
                fam2 = re.search(r"\(([^\)]+)\)", familienname)
                logger.info(f"fam1: {fam1_}")
                logger.info(f"fam2: {fam2}")
                if fam2:
                    if ";" in fam2.group(1):
                        fam2 = fam2.group(1).split(";")
                        fam2 = [x.strip() for x in fam2]
                    elif not ", von" in fam2.group(1) and not ", de" in fam2.group(1):
                        fam2 = fam2.group(1).split(",")
                        fam2 = [x.strip() for x in fam2]
                    else:
                        temp_ = []
                        temp_.append(fam2.group(1).strip())
                        fam2 = temp_
            elif "[" in familienname:
                fam1_ = familienname.split(" [")[0]
                fam2 = re.search(r"\[([^\]]+)\]", familienname)
                logger.info(f"fam1: {fam1_}")
                logger.info(f"fam2: {fam2}")
                if fam2:
                    if ";" in fam2.group(1):
                        fam2 = fam2.group(1).split(";")
                        fam2 = [x.strip() for x in fam2]
                    elif not ", von" in fam2.group(1) and not ", de" in fam2.group(1):
                        fam2 = fam2.group(1).split(",")
                        fam2 = [x.strip() for x in fam2]
                    else:
                        temp_ = []
                        temp_.append(fam2.group(1).strip())
                        fam2 = temp_
            else:
                fam1_ = familienname
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
            logger.info(f"NOT PARSED LEBENSDATEN -> {r_leb}")
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

    logger.warning(f"t_tit = {r_tit}")
    if r_tit in change.keys():
        r_tit = change[r_tit]
    t_list, subst = re.subn(r"\(.*\)", "", r_tit)
    t_list = [x.strip() for x in t_list.split(";")] # changed from "," to ";" for HSV
    logger.warning(f"t_list = {t_list}")

    def create_title(tit):
        if re.search(r"\d{4}", tit):

            tit_res = resolve_abbreviations(tit, cfg.list_abbreviations)
            date = re.search(r"(\d{4}){1}(-\d{2}-\d{2}){0,1}", tit).group(0)
            date = helper_hsv_post_process_dates(date)
            logger.warning(f"tit_res: {tit_res}, date after processing: {date}")
            Label.objects.create(label_type=lt_titel_datiert, temp_entity=pers, label=tit_res, start_date_written=date)

        t2, created = Title.objects.get_or_create(
            name=resolve_abbreviations(tit, cfg.list_abbreviations)
        )
        pers.title.add(
            t2)

    for tit in t_list:
        if "," in tit and tit not in dont_split:
            logger.warning(f"Komma in tit: {tit}")
            for t in tit.split(","):
                t = t.strip()
                create_title(t)
                logger.warning(f"CREATING SPLIT TITLE {t} from {tit}")
        else:
            create_title(tit)




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
        logger.info(f"inner date: {i_date}")
        new_i_date = deepcopy(i_date)
        new_i_date = new_i_date.replace("-00-00", "-06-30")
        logger.info(f"new_i_date: {new_i_date}")
        new_date = re.sub(i_date, new_i_date, date)
    else:
        if len(date) == 10:
            if "-00-00" in date:
                i_date = "<"+date[:4]+"-06-30>"
                new_date = date+i_date
            else:
                new_date = f"{date}<{date}>"
        elif len(date) == 4:
            new_date = f"{date}<{date}>"
        else:
            new_date = date

    logger.info(f"old: {date}, new: {new_date}")

    return new_date


def chunk_process_datum(c_D, rel):

    if len(c_D) == 1:
        c_D[0] = helper_hsv_post_process_dates(c_D[0])
        if "bis" in c_D[0]:
            rel["end_date_written"] = c_D[0]
            logger.info(f"rel, chunk edw: {c_D[0]}")
        else:
            rel["start_date_written"] = c_D[0]
            logger.info(f"rel, chunk sdw: {c_D[0]}")

    elif len(c_D) == 2:
        c_D[0] = helper_hsv_post_process_dates(c_D[0])
        c_D[1] = helper_hsv_post_process_dates(c_D[1])
        rel["start_date_written"] = c_D[0]
        logger.info(f"rel, chunk sdw: {c_D[0]}")

        rel["end_date_written"] = c_D[1]
        logger.info(f"rel, chunk edw: {c_D[0]}")
    logger.info(f" this is the full relation: {rel}")

    return rel


def chunk_get_nm_hst(c_H, r_H, idx_chunk, first_hs):
    test_hs = False
    if c_H:
        test_hs = True
        if c_H in cfg.lst_hofst.keys():
            nm_hst = cfg.lst_hofst[c_H][0]
        else:
            nm_hst = c_H
    elif first_hs:
        test_hs = True
        nm_hst = first_hs
        logger.warning(f"USED first hofstaat in chunk get nm hst: {nm_hst}")

    # elif isinstance(r_H, str) and idx_chunk == 0:  # write first hofstaat - no longer needed in hsv #todo commented out because no lonnger needed
    #     test_hs = True
    #     nm_hst = r_H.strip()
    #     for hst in [x.strip() for x in r_H.split(";")]:
    #         if hst in cfg.lst_hofst.keys():
    #             nm_hst = cfg.lst_hofst[hst][0]
    else:
        nm_hst = "Dummy Hofstaat"

    return nm_hst, test_hs

def chunk_create_institution(nm_hst, test_hs, pers):
    #inst_type_hst, created = InstitutionType.objects.get_or_create(name="Hofstaat")
    inst, created = Institution.objects.get_or_create(
        name=nm_hst, kind=inst_type_hst

    )

    inst.collection.add(col_unsicher)
    if test_hs:
        PersonInstitution.objects.get_or_create(
            related_institution=inst, related_person=pers, relation_type=rl_pers_hst
        )

    return inst


def get_or_create_amt(row):
    beh_name = re.match(r"^(.+)[/\\(]", row["Amt/Behörde"])
    if beh_name:
        amt = beh_name.group(1).strip()
    else:
        amt = row["Amt/Behörde"].strip()
    try:
        row_target = cfg.df_aemter_index[amt]
    except:
        raise ValueError(f"couldnt find matching Amt entry for {amt}")

    if isinstance(row.iloc[11], str):
        logger.warning(f" row iloc 11, row iloc 7: {row.iloc[11]} --- {row.iloc[7]}")
        if row.iloc[11] in cfg.lst_hofst.keys():
            name_hst_1 = cfg.lst_hofst[row.iloc[11]][0]
        else:
            name_hst_1 = row.iloc[11]
    else:
        name_hst_1 = "Dummy Hofstaat"

    test_unsicher = False
    if name_hst_1 == "UNSICHER - Collection, manuelle Entscheidung":
        name_hst_1 = "UNSICHER"
        test_unsicher = True
    name_hst_1 = name_hst_1.replace('Hofstaat', '').strip()

    logger.warning(f"row target_iloc[7]: {row_target.iloc[7]}")
    inst_type, c3 = InstitutionType.objects.get_or_create(name=row_target.iloc[7])
    amt_ent, created = Institution.objects.get_or_create(name=f'{row_target["APIS-Vereinheitlichung"]} ({name_hst_1})', kind=inst_type)
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
    return amt_ent, amt_super, created2


def create_text_entries(row, pers, src_base, map_text_type, columns=(5,18)):
    for idx in range(5,18):
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


def chunk_process_amt(c_A, r_A, c_H, inst, nm_hst, idx_chunk, row, amt_test):

    if c_A is not None and amt_test:
        logger.warning("c_A is not none and amt_test is True")
        if c_A.strip() in cfg.df_aemter_index.keys():
            amt = cfg.df_aemter_index[c_A.strip()].iloc[8]
        else:
            amt = c_A.strip()

        if c_H is not None: # removed: idx_chunk == 0 or
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

    elif isinstance(r_A, str) and idx_chunk == 0:
        try:
            inst2, inst3, created = get_or_create_amt(
                row)
            if created:
                InstitutionInstitution.objects.create(
                    related_institutionA=inst3,
                    related_institutionB=inst,
                    relation_type=rl_teil_von,
                )
        except Exception as e:
            inst2 = amt_dummy
            inst3 = False
            logger.debug(f"Exception in Amt function: {e}")

    elif c_A is not None:
        logger.warning("c_A is not none")
        if c_A.strip() in cfg.df_aemter_index.keys():
            amt = cfg.df_aemter_index[c_A.strip()].iloc[8]
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


def helper_hsv_match_amt_with_funct(doc, r_A, r_H):
    amt_test = False

    if isinstance(r_A, str):
        ab_split = r_A.split(";")

        if len(ab_split) == len(doc._.chunks):

            for c,a in zip(doc._.chunks, ab_split):
                a = a.split("/")[0]
                if c["AMT"]:
                    logger.warning(f"chunk Amt already exists: {c['AMT']}")
                c["AMT"] = a

                logger.warning(c["FUNKTION"])
            amt_test = True

    return doc, amt_test

def helper_hsv_match_hofstaate(doc, r_H):
    first_hs = None
    if isinstance(r_H, str):
        hs_split = r_H.split(";")
        first_hs = hs_split[0].strip()

        if len(hs_split) == len(doc._.chunks):

            for c, h in zip(doc._.chunks, hs_split):
                h = h.split("/")[0] #
                logger.warning(f"HOFSTAATE PROCESSING ----> h: {h}")
                if c["HOFSTAAT"]:
                    # if chunk hofstaat already exists, don't match with hofstaatsspalte.
                    logger.warning(f"chunk Hofstaat already exists: {c['HOFSTAAT']}")
                    pass
                else:
                    logger.warning("NO CHUNK HOFSTAAT")
                    c["HOFSTAAT"] = h.strip()
                    logger.warning(f"chunk Hofstaat set to: {c['HOFSTAAT']}")

    logger.warning(f"FIRST HOFSTAAT = {first_hs}")

    return doc, first_hs


def process_chunks(doc, pers, row):

    if isinstance(row["Amt/Behörde"], str):
        aemter_behörde = row['Amt/Behörde'].split(";")
    else:
        aemter_behörde = []

    r_H = row["Hofstaat"]
    r_A = row["Amt/Behörde"]

    logger.info(f"len_doc_chunks: {len(doc._.chunks)}, len Ämter-Spalte: {len(aemter_behörde)}")


    doc, first_hs = helper_hsv_match_hofstaate(doc, r_H)
    doc, amt_test =  helper_hsv_match_amt_with_funct(doc, r_A, r_H)



    for idx_chunk, c in enumerate(doc._.chunks):


        c_D = c["DATUM"]
        c_F = c["FUNKTION"]
        c_H = c["HOFSTAAT"]
        c_A = c["AMT"]


        rel = {"related_person": pers}
        if c_D is not None:
            rel = chunk_process_datum(c_D, rel)

        nm_hst, test_hs = chunk_get_nm_hst(c_H, r_H, idx_chunk, first_hs)

        test_unsicher = False
        if nm_hst == "UNSICHER - Collection, manuelle Entscheidung":
            nm_hst = "UNSICHER"
            test_unsicher = True

        inst = chunk_create_institution(nm_hst, test_hs, pers) # first inst ist der hofstaat

        inst2 = chunk_process_amt(c_A, r_A, c_H, inst, nm_hst, idx_chunk, row, amt_test)

        rel["related_institution"] = inst2

        chunk_create_relations(c_F, rel)
        try:
            logger.debug(f"{row.orig_index} -- {pers} -- {rel}")
        except AttributeError as e:
            logger.debug(f"{pers.source.orig_id} -- {pers} -- {rel}")



    return pers


def process_row(idx, row, src_base, conf):
    global cfg
    cfg = conf

    if idx == 20000:
        return None
    else:
        logger.info(f"working on row: {idx}")
        create_all()

        r_vor = row["Vorname"]
        r_fam = row["Familienname"]
        r_ges = row["Geschlecht"]
        r_leb = row["Lebensdaten"]
        r_fun = row["Funktion"]
        r_tit = row["Titel"]

        if isinstance(r_fun, str):
            # this is a rough solution for the problem with missing whitespaces after ')' or ',' etc.
            # todo: make sure that references to the source entry are not using the substitute r_fun value created here
            def replacer(match):
                replacement = match.group(0)[:-1]+" "+match.group(0)[-1:]
                logger.info(f"r_fun: replaced {match.group(0)} with: {replacement}")
                return replacement
            pattern = re.compile(r"(\)|\,|;)\S")
            r_fun = re.sub(pattern, replacer, r_fun)

        vorname = person_process_field_vorname(r_vor)

        vn, vn_alternative = helper_process_names(vorname[0])
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

        map_text_type = {}
        map_text_type = create_text_entries(row, pers, src_base, map_text_type)
        col, created = Collection.objects.get_or_create(name="Import HZAB full 10-3-21")
        pers.collection.add(col)

        if not isinstance(r_fun, str):
            return pers

        doc = create_and_process_doc(r_fun, idx)
        pers = process_chunks(doc, pers, row)


        return pers
