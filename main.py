import reversion
import subprocess
from copy import deepcopy
import plac
import spacy
import logging
import time
import pandas as pd
from import_src.import_logger.import_logger_config import init_logger, init_funclogger, init_complogger
from import_src.file_processing import (
    get_list_abbreviations,
    get_lst_hofst,
    get_aemter_index,
)


from spacy.tokens import Doc
from pathlib import Path
from chain_tests import run_chained_tests


@plac.annotations(
    path_df=("Path of the DF to import, defaults to HSV", "option", None, Path, None),
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
    collection=(
        "Name of the collection for the whole import",
        "option",
        "col",
        str,
        None
    ),
    spacy_model=(
        "If set with path to model, the model is imported instead of building the local pipeline in nlp_pipeline_components",
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
    logger_level =(
        "sets the level for the import logger. Defaults to logger.INFO",
        "option",
        None,
        object,
        None
    ),
    collection_team=("List of Teammembers for whom the import should be split into equal sized collections.",
                     "option",
                     None,
                     str,
                     None),
    use_stopvalues=(
        "Defaults to False. If set to True, the stop-values for already manually corrected entries will be applied and not overwritten",
        "flag",
        None,
        bool,
        None
    ),
    is_test=(
            "run only test_import on selected rows",
            "flag",
            "t",
            bool,
            None
    ),
    sample_frame=(
            "sample to run test_import on.",
            "option",
            "s",
            object,
            None
    ),
    without_testing=(
        "Defaults to False. Usually, tests are run before the actual import. Set to True to run import without testing first",
        "flag",
        "w",
        bool,
        None
    ),
    log_msg=(
      "Allows for a short comment at the beginning of a logfile. Defaults to None",
      "option",
      "m",
      str,
      None
    ),

)
def run_import(

    username=None,
    django_settings="django_settings.hsv_settings",
    collection="Sample HSV Import 14-6-21 – Amt/Hofstaat bugfix; CStandhartinger",
    spacy_model=None, # Expects an imported Model, that already contains the pipeline components.
    existing_annotations="data/viecpro_HSV_0.jsonl",
    path_df="data/3_HSV-angepasst-IMPORT.xlsx",
    path_hofstaat="data/Kürzel-Hofstaate-EX-ACC-2021-06-02.xlsx",
    path_aemter="data/Kürzel-Ämter-ACC-EX-2021-02-08.xlsx",
    path_abbreviations="data/EXCEL-ACCESS_Kürzel-Titel-Orden-2021-01-28.xlsx",
    logger_level=logging.INFO,
    collection_team=["MRomberg", "MKaiser", "CStandhartinger"],
    use_stopvalues=False,
    is_test = False,
    sample_frame = None,
    without_testing = False,
    log_msg = None

):


    # ////////////// SET DJANGO SETTINGS MODULE ENV VARIABLE /////////////////
    from config import cfg

    cfg.set_settings(django_settings)

    timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())

    if is_test:
        logfolder = "test_logs"
        without_testing = True

    elif sample_frame is not None:
        logfolder = "sample_import_logs"

    else:
        logfolder = "import_logs"

    logfile = f"import_src/logfiles/{logfolder}/{timestamp}.txt"

    #////////////// LOG IMPORT METAINFO /////////////////
    init_logger(level=logger_level, logfile=logfile)
    init_funclogger(level=logger_level, logfile=logfile)
    init_complogger(level=logger_level, logfile=logfile)

    logger = logging.getLogger("import_logger")
    funclogger = logging.getLogger("func_logger")
    using_local_model = lambda x=spacy_model: "using local model" if not x else x
    using_sample_frame = lambda : "using sample frame" if sample_frame is not None else "None"

    metainfo_args = { "username":username,
            "django_settings":django_settings,
            "collection":collection,
            "spacy_model": using_local_model(),
            "existing_annotations":existing_annotations,
            "path_df": path_df,
            "path_hofstaat":path_hofstaat,
            "path_aemter":path_aemter,
            "path_abbreviations":path_abbreviations,
            "logger_level":logger_level,
            "collection_team":collection_team,
            "use_stopvalues":use_stopvalues,
            "is_test":is_test,
            "sample_frame":using_sample_frame(),
            "without_testing": without_testing,
            "log_msg":log_msg,
                      }

    logger.info("----------------")
    logger.info("Import MetaInfo:\n")
    metainfo_msg = [f"{key}: {value}" for key, value in metainfo_args.items()]
    for m in metainfo_msg:
        logger.info(m)
    logger.info("----------------\n\n")


    # Check if tests should be run before import can start. If tests fail, import will be stopped.
    if not without_testing:
        exitcode = run_chained_tests()

        if exitcode == 1:
            with open(f"tests/test_logfiles/error_logs/error_control.txt", "r") as errorfile:
                errormessage = errorfile.readlines()
                logger.info("/////////  TESTS FAILED /////////")

                for line in errormessage:
                    logger.info(line)
            raise Exception(f"\n\nTESTS FAILED: {line}")

        else:
            logger.info("/////////  ALL TESTS PASSED  /////////\n\n\n")

    from django.contrib.auth.models import User
    from nlp_pipeline_components.build_pipeline import get_model, log_pipeline_info


    #////////////// RUN GLOBAL CONFIGURATION /////////////////
    # Makes the following Objects accessible across files

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
    cfg.annotations = existing_annotations


    # If spacy_model is not given, the local ner will be combined with the pipeline components in nlp_pipeline_components
    if not spacy_model:
        cfg.nlp = get_model(annotations=cfg.annotations, cfg=cfg)

    else:
        cfg.nlp = spacy.load(spacy_model,
                         annotations=existing_annotations,
                              lst_hofst=cfg.lst_hofst,
                              df_aemter_index=cfg.df_aemter_index,
                         )
        log_pipeline_info(spacy_model, cfg.nlp, local_model=False)

    from import_src.process_row import process_row # todo: this is an antipattern, I know,  should be fixed
    from import_src.exclude_manually_corrected_entries import create_ignorelist

    # If use_stopvalues is True, Calculate List of Rows that will be ignored during re-import
    ignore_list = []
    if use_stopvalues:
        id_R = None
        id_K = None
        id_S = None
        ignore_list = create_ignorelist(id_R=id_R, id_K=id_K, id_S=id_S)

    # Create Collection Names for Splitting the Collection for the Team-Members
    TC = [member+" (HSV)" for member in collection_team]

    if sample_frame is not None:
        run_sample_import(sample_frame, logger, funclogger, process_row, cfg, tstamp=timestamp)
    else:
        run_full_import(cfg, username, path_df, logger, process_row, User, TC, ignore_list=ignore_list)

#Sample import doesn't use collections and has a separate plac interface
#Sample import plac interface allows to import another df, a slice of the orig df, a list of rows or a single row
#Sample import should be used to check specific rows.
#Sample import is logged to the import_log directory
#Sample import goes into the "SAMPLE IMPORT COLLECTION + timestamp of logfile" collection
def run_sample_import(sample_frame, logger, funclogger, process_row, cfg, tstamp):
    sample = sample_frame
    src_base = {
        "orig_filename": f"TestRun",
        "pubinfo": f"File from GIT commit {subprocess.check_output(['git', 'show-ref', 'HEAD']).strip()}",
    }

    split_collection = f"SAMPLE IMPORT COLLECTION {tstamp}"

    if isinstance(sample, pd.Series):
        row = sample
        idx = sample.name
        logger.info(f"\n--------- Start of row | {idx} | -------------- ")
        funclogger.info("Working with pd-Series")
        process_row(idx, row, src_base, cfg, split_collection)  #todo Problem: normally, the import starts with 0, but here i enter the correct excel row
    else:
        for idx, row in sample.iterrows():
            logger.info(f"\n--------- Start of row | {idx} | -------------- ")
            process_row(idx, row, src_base, cfg, split_collection)

#Full Import is the original import
#Full Import is split into a specific Collection for each team_member
def run_full_import(cfg, username, path_df, logger, process_row, User, TC, ignore_list):

    splitcount = len(TC)
    me = False
    if username is not None:
        me = User.objects.get(username=username)
    Doc.set_extension("excel_row", default=-1, force=True)
    lst_offs = list(range(0, len(cfg.df), int(len(cfg.df) / splitcount) + 1))#[:1] #todo: if slicing should be allowed, it would need to be added at another point. here it will break zip
    collection_counter = zip(lst_offs, TC)
    loglist = deepcopy(collection_counter)
    logger.info(f"collection_counter = {list(loglist)}")

    for idx5, col_tuple in enumerate(collection_counter):
        offs, split_collection = col_tuple
        if idx5 == len(lst_offs) - 1:
            off_end = len(cfg.df)
        else:
            off_end = lst_offs[idx5+1]-1 # todo: there are 2 or 3 people twice in the database after the import! I think this happens here.
        #if idx5 == 2:
        with reversion.create_revision():
            if me:
                reversion.set_user(me)
            reversion.set_comment(f"Second full HSV test-import. Fixed model and nlp-pipeline. Importing rows {offs} - {off_end}")
            src_base = {
                "orig_filename": f"{path_df}",
                "pubinfo": f"File from GIT commit {subprocess.check_output(['git', 'show-ref', 'HEAD']).strip()}",
            }

            for idx, row in cfg.df.loc[offs:off_end].iterrows():
                if idx in ignore_list:
                    #todo: implement that re-imported persons are first deleted! Use orig_id + collection (!!) to delete them first
                    logger.info(f"-----> IGNORING row | {idx}|")
                else:
                    logger.info(f"\n--------- Start of row | {idx} | -------------- ")
                    process_row(idx, row, src_base, cfg, split_collection) # todo: note removed return pers here


if __name__ == "__main__":
    plac.call(run_import)
