import os, django, re

os.environ["DJANGO_SETTINGS_MODULE"] = "django_settings.viecpro_testing"
django.setup()

import reversion, subprocess, plac, spacy, logging
import pandas as pd
from config import cfg

from import_src.file_processing import (
    get_list_abbreviations,
    get_lst_hofst,
    get_aemter_index,
)
from import_src.process_row import process_row
from nlp_pipeline_components.build_pipeline import get_model

from spacy.tokens import Doc
from pathlib import Path

from apis_core.apis_metainfo.models import Collection
from django.contrib.auth.models import User


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


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
        str,
        None,
    ),
    existing_annotations=(
        "Path of jsonl file with existing annotations to use. Defaults to viecpro_import/data/viecpro_HZAB_funktion_0.jsonl",
        "option",
        None,
        Path,
        None,
    ),
    use_local_pipeline=(
            "loads local model from pipeline components in /pipeline/build_pipeline.py. Defaults to true",
            "option",
            None,
            bool,
            None,
    ),
)
def run_import(

    #todo: how to resolve this with get_data_variables
    username=None,
    django_settings="django_settings.viecpro_testing",
    collection="Import Collection",
    spacy_model="de_VieCPro_HZAB",
    existing_annotations="data/viecpro_HZAB_funktion_0.jsonl",
    #path_df="data/1_Hofzahlamtsbücher-HZAB-2020-12-06.xlsx",
    path_df="data/3_HSV-angepasst-IMPORT.xlsx",
    path_hofstaat="data/Kürzel-Hofstaate-EX-ACC-2021-02-08.xlsx",
    path_aemter="data/Kürzel-Ämter-ACC-EX-2021-02-08.xlsx",
    path_abbreviations="data/EXCEL-ACCESS_Kürzel-Titel-Orden-2021-01-28.xlsx",
    use_local_pipeline=True,
):

   
    cfg.collection = collection
    cfg.df_aemter = pd.read_excel(path_aemter, header=2, engine="openpyxl")
    cfg.df_hofstaat = pd.read_excel(path_hofstaat, engine="openpyxl")
    cfg.df_abbreviations = pd.read_excel(
        path_abbreviations, sheet_name="Titel", header=3, engine="openpyxl"
    )
    try:
        cfg.df = pd.read_excel(path_df, sheet_name="Original", engine="openpyxl")
    except:
        cfg.df = pd.read_excel(path_df, engine="openpyxl")

    cfg.list_abbreviations = get_list_abbreviations(cfg.df_abbreviations)
    cfg.lst_hofst = get_lst_hofst(cfg.df_hofstaat)
    cfg.df_aemter_index = get_aemter_index(cfg.df_aemter)

    if use_local_pipeline:
        logger.debug("USING LOCAL MODEL")
        cfg.nlp = get_model(cfg=cfg)
    else:
        cfg.nlp = spacy.load(spacy_model,
                         annotations=existing_annotations,
                              lst_hofst=cfg.lst_hofst,
                              df_aemter_index=cfg.df_aemter_index,
                         )

    me = False
    if username is not None:
        me = User.objects.get(username=username)
    Doc.set_extension("excel_row", default=-1, force=True)
    lst_offs = list(range(0, len(cfg.df), int(len(cfg.df) / 4) + 1))[:1]

    for idx5, offs in enumerate(lst_offs):
        if idx5 == len(lst_offs) - 1:
            off_end = len(cfg.df)
        else:
            off_end = lst_offs[idx5 + 1]
        with reversion.create_revision():
            col, crt = Collection.objects.get_or_create(name=cfg.collection)
            if me:
                reversion.set_user(me)
            reversion.set_comment(f"import rows {offs} - {off_end}")
            src_base = {
                "orig_filename": f"{path_df}", # todo: cut path to filename only
                "pubinfo": f"File from GIT commit {subprocess.check_output(['git', 'show-ref', 'HEAD']).strip()}",
            }
            for idx, row in cfg.df.loc[
                offs:off_end,
            ].iterrows():
                logger.debug(f"working on row {idx}")
                p1 = process_row(idx,
                                 row, 
                                 src_base,
                                 cfg)


logger.debug("FROM LOGGER: running import")

if __name__ == "__main__":
    plac.call(run_import)
