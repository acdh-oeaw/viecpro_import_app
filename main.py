import spacy
import os
import reversion
import pandas as pd
import subprocess
import plac
from pathlib import Path
import sys

import importlib
import django
from django.conf import settings

from pipeline.components.components import *

from yaml import load_all, Loader

# reversion = data[1] # todo: refactor reversion

# VAR = sharepoint["var_password"] # todo: implement environment-variable password
# pw = os.environ[VAR]


map_text_type = {}


def process_row(idx, row, src_base):
    src_base["orig_id"] = idx
    src = Source.objects.create(**src_base)
    if isinstance(row["Vorname"], str):
        vorname = row["Vorname"].split(",")
    else:
        vorname = ["NN"]
    familienname = row["Familienname"]
    if isinstance(familienname, str):
        labels_fam_hzt = []
        labels_fam = False
        for idx_fam, familienname in enumerate(familienname.split(", oo")):
            if "(" in familienname:
                fam1_ = familienname.split(" (")[0]
                fam2 = re.search(r"\(([^\)]+)\)", familienname)
                if fam2:
                    fam2 = fam2.group(1).split(",")
                    fam2 = [x.strip() for x in fam2]
            else:
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
    vn, vn_alternative = process_names(vorname[0])
    if vn_alternative is not None:
        vorname.append(vn_alternative)
    sn, sn_alternative = process_names(fam1)
    if sn_alternative is not None:
        if not labels_fam:
            labels_fam = []
        else:
            prep_proc_name = []
            for idx4, sn_a in enumerate(labels_fam):
                sn_a_1, sn_a_2 = process_names(sn_a)
                if sn_a_2 is not None:
                    labels_fam[idx] = sn_a_1
                    prep_proc_name.append(sn_a_2)
            if len(prep_proc_name) > 0:
                labels_fam.extend(prep_proc_name)
        labels_fam.append(sn_alternative)
    pers = {"first_name": vn, "name": sn}
    if isinstance(row["Geschlecht"], str):
        if row["Geschlecht"] == "M":
            pers["gender"] = "male"
        elif row["Geschlecht"] == "W":
            pers["gender"] = "female"
    ld = row["Lebensdaten"]
    if isinstance(ld, str):
        if "gest. " in ld:
            pers["end_date_written"] = ld.replace("gest. ", "")
    pers["source"] = src
    pers = Person.objects.create(**pers)
    if isinstance(row["Titel"], str):
        t_list, subst = re.subn(r"\(.*\)", "", row["Titel"])
        t_list = [x.strip() for x in t_list.split(",")]
        for tit in t_list:
            t2, created = Title.objects.get_or_create(
                name=resolve_abbreviations(tit, list_abbreviations)
            )
            pers.title.add(t2)
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
    create_text_entries(row, pers, df, src_base)
    pers.collection.add(col)
    if not isinstance(row["Funktion"], str):
        return pers
    # doc = nlp(row["Funktion"])
    doc = nlp.make_doc(row["Funktion"])

    # Associate the metadata
    doc._.excel_row = idx

    # Run each component
    for name, proc in nlp.pipeline:
        doc = proc(doc)
    print("funktion: ", row["Funktion"])
    for idx3, c in enumerate(doc._.chunks):
        test_hs = False
        rel = {"related_person": pers}
        if c["DATUM"] is not None:
            if len(c["DATUM"]) == 1:
                if "bis" in c["DATUM"][0]:
                    rel["end_date_written"] = c["DATUM"][0]
                else:
                    rel["start_date_written"] = c["DATUM"][0]
            elif len(c["DATUM"]) == 2:
                rel["start_date_written"] = c["DATUM"][0]
                rel["end_date_written"] = c["DATUM"][1]
        if c["HOFSTAAT"] is not None:
            if c["HOFSTAAT"] in lst_hofst.keys():
                nm_hst = lst_hofst[c["HOFSTAAT"]][0]
            else:
                nm_hst = c["HOFSTAAT"]
        elif isinstance(row["Hofstaat"], str) and idx3 == 0:
            test_hs = True
            nm_hst = row["Hofstaat"].strip()
            for hst in [x.strip() for x in row["Hofstaat"].split(";")]:
                if hst in lst_hofst.keys():
                    nm_hst = lst_hofst[hst][0]
        else:
            nm_hst = "Dummy Hofstaat"
        test_unsicher = False
        if nm_hst == "UNSICHER - Collection, manuelle Entscheidung":
            nm_hst = "UNSICHER"
            test_unsicher = True
        inst, created = Institution.objects.get_or_create(
            name=nm_hst, kind=inst_type_hst
        )
        inst.collection.add(col_unsicher)
        if test_hs:
            PersonInstitution.objects.get_or_create(
                related_institution=inst, related_person=pers, relation_type=rl_pers_hst
            )

        if isinstance(row["Amt/Behörde"], str) and idx3 == 0:
            try:
                inst2, inst3, created = get_or_create_amt(
                    row, df_aemter_index, lst_hofst
                )
                if created:
                    InstitutionInstitution.objects.create(
                        related_institutionA=inst3,
                        related_institutionB=inst,
                        relation_type=rl_teil_von,
                    )
            except Exception as e:
                inst2 = amt_dummy
                inst3 = False
                print(f"Exception in Amt function: {e}")
        elif c["AMT"] is not None:
            if c["AMT"].strip() in df_aemter_index.keys():
                amt = df_aemter_index[c["AMT"].strip()].iloc[8]
            else:
                amt = c["AMT"].strip()
            if idx3 == 0 or c["HOFSTAAT"] is not None:
                amt_name = f"{amt} ({nm_hst})"
            else:
                amt_name = f"{amt} (Dummy Hofstaat)"
            print(amt_name)
            if len(amt_name) > 254:
                print(f"len amtname {(len(amt_name)-250)}")
                ln_minus = len(f"{amt_name} ({nm_hst})") - 250
                print(f"{amt_name[:-ln_minus]} ({nm_hst})")
                amt_name = f"{amt_name[:-ln_minus]} ({nm_hst})"
            inst2, created = Institution.objects.get_or_create(name=amt_name)
        else:
            inst2, created = Institution.objects.get_or_create(
                name=f"Dummy Amt ({nm_hst})"
            )
        rel["related_institution"] = inst2
        if len(c["FUNKTION"]) > 0:
            for rel_type in c["FUNKTION"]:
                try:
                    (
                        rel_t_obj,
                        created,
                    ) = PersonInstitutionRelation.objects.get_or_create(name=rel_type)
                    rel["relation_type"] = rel_t_obj
                    PersonInstitution.objects.create(**rel)
                except Exception as e:
                    print(f"Exception: {e}")
    return pers


# todo: create a helper function that splits import into collections
@plac.annotations(
    path_df=("Path of the DF to import, defaults to HZAB", "option", None, Path, None),
    path_hofstaat=(
        "Path of the Hofstaat list to use, defaults to data/Kürzel-Hofstaate-EX-ACC-2021-02-08.xlsx",
        "option",
        None,
        Path,
        None,
    ),
    path_aemter=(
        "Path of the Ämter list to use, defaults to data/Kürzel-Ämter-ACC-EX-2021-02-08.xlsx",
        "option",
        None,
        Path,
        None,
    ),
    path_abbreviations=(
        "Path of the Abkürzungen list to use, defaults to data/EXCEL-ACCESS_Kürzel-Titel-Orden-2021-01-28.xlsx",
        "option",
        None,
        Path,
        None,
    ),
    username=("username to use in reversions", "option", None, str, None),
    django_settings=(
        "Dot notation of django settings to use",
        "option",
        None,
        str,
        None,
    ),
    spacy_model=(
        "Path of spacy model to use. Defaults to viecpro_import/models/viecpro_ner_hzab_12-20/",
        "option",
        None,
        Path,
        None,
    ),
    existing_annotations=(
        "Path of jsonl file with existing annotations to use. Defaults to viecpro_import/data/viecpro_HZAB_funktion_0.jsonl",
        "option",
        None,
        Path,
        None,
    ),
)
def run_import(
    username=None,
    django_settings="django_settings.viecpro_testing",
    collection="Import Collection",
    spacy_model="viecpro_import/models/viecpro_ner_hzab_12-20/",
    existing_annotations="viecpro_import/data/viecpro_HZAB_funktion_0.jsonl",
    path_df="data/1_Hofzahlamtsbücher-HZAB-2020-12-06.xlsx",
    path_hofstaat="data/Kürzel-Hofstaate-EX-ACC-2021-02-08.xlsx",
    path_aemter="data/Kürzel-Ämter-ACC-EX-2021-02-08.xlsx",
    path_abbreviations="data/EXCEL-ACCESS_Kürzel-Titel-Orden-2021-01-28.xlsx",
):
    if os.getenv("DJANGO_SETTINGS_MODULE") is None:
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", django_settings)
    django.setup()

    from pipeline.helper_functions.helper_functions import (
        get_or_create_amt,
        process_names,
        create_text_entries,
    )
    from pipeline.file_processing import (
        get_list_abbreviations,
        get_lst_hofst,
        get_aemter_index,
        resolve_abbreviations,
    )

    from apis_core.apis_entities.models import Person, Institution, Title
    from apis_core.apis_metainfo.models import Source
    from apis_core.apis_relations.models import (
        PersonInstitution,
        InstitutionInstitution,
    )
    from apis_core.apis_labels.models import Label
    from apis_core.apis_vocabularies.models import PersonInstitutionRelation
    from django.contrib.auth.models import User

    df_aemter = pd.read_excel(path_aemter, header=2)
    df_hofstaat = pd.read_excel(path_hofstaat)
    df_abbreviations = pd.read_excel(path_abbreviations, sheet_name="Titel", header=3)
    df = pd.read_excel(path_df, sheet_name="Original")
    list_abbreviations = get_list_abbreviations(df_abbreviations)
    lst_hofst = get_lst_hofst(df_hofstaat)
    df_aemter_index = get_aemter_index(df_aemter)

    nlp = spacy.load(
        spacy_model,
        annotations=existing_annotations,
        lst_hofst=lst_hofst,
        df_aemter_index=df_aemter_index,
    )
    print(nlp)

    me = False
    if username is not None:
        me = User.objects.get(username=username)
    Doc.set_extension("excel_row", default=-1, force=True)
    lst_offs = list(range(0, len(df), int(len(df) / 4) + 1))[3:]
    for idx5, offs in enumerate(lst_offs):
        if idx5 == len(lst_offs) - 1:
            off_end = len(df)
        else:
            off_end = lst_offs[idx5 + 1]
        with reversion.create_revision():
            if me:
                reversion.set_user(me)
            reversion.set_comment(f"import rows {offs} - {off_end}")
            src_base = {
                "orig_filename": "{path_df}",
                "pubinfo": f"File from GIT commit {subprocess.check_output(['git', 'describe']).strip()}",
            }
            for idx, row in df.loc[
                offs:off_end,
            ].iterrows():
                print(f"working on row {idx}")
                p1 = process_row(idx, row, src_base)


print("running import")

if __name__ == "__main__":
    plac.call(run_import)
