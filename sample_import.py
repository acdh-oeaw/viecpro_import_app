from main import run_import
import pandas as pd
from pathlib import Path
from copy import deepcopy
import plac

@plac.annotations(
    path_df=("Path of the DF to import, defaults to HSV", "option", None, Path, None),
    sample_start=("Start index of sample, defaults to 0","option", "s", int, None),
    sample_end=("Start index of sample defaults to full DataFrame", "option", "e", int, None),
    sample_list=("List of rows to take the sample from", "option", "l", str, None),
    without_testing =("Defaults to False. Usually, tests are run before the actual import. Set to True to run import without testing first", "flag", "w", bool, None)
)

def run_sample_import(path_df="data/3_HSV-angepasst-IMPORT.xlsx",
             sample_start=0,
             sample_end=0,
             sample_list=None,
             without_testing=False):

    df = pd.read_excel(path_df, engine="openpyxl")
    sample = deepcopy(df)

    if sample_list:
        msg = "Using sample list"

        list = deepcopy(sample_list)
        try:
            list = list[1:-1].split(",")
            sample_list = [int(element.strip()) for element in list]
            sample = sample.iloc[sample_list]
        except:
            raise ValueError("Couldn't interpret list, use format [int,int,int,int] or '[int, int, int, int]'")
    else:
        if sample_start == 0 and sample_end == 0 :
            msg = "Using full sample"
            sample = sample
        elif sample_start == 0 and sample_end != 0:
            msg = "Using sample range"
            sample = sample.iloc[0:sample_end]
        elif sample_start == sample_end:
            msg="Using single row"
            sample = sample.iloc[sample_start]

        elif sample_start != 0 and sample_end == 0:
            msg="Only start given, importing 1 row"
            sample = sample.iloc[sample_start]

        elif sample_end < len(df):
            msg = "Using sample range"
            sample = sample.iloc[sample_start:sample_end]
        else:
            msg = "Couldn't interpret sample range"

    print(msg, end="\n\n\n\n")
    run_import(sample_frame=sample, without_testing=True)


if __name__ == "__main__":
    plac.call(run_sample_import)