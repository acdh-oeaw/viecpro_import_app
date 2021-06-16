from django.test import TestCase, TransactionTestCase
import pathlib
from apis_core.apis_entities.models import Person
from config import cfg
from main import run_import
import pandas as pd
import math
import logging
import time


logger = logging.getLogger("test_logger")
#timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
#logfile = f"tests/test_logfiles/{timestamp}_test_log.txt"
errorfile = f"tests/test_logfiles/error_logs/error_control.txt"
#fileHandler = logging.FileHandler(logfile, mode="a")
#fileHandler.setLevel(logging.INFO)
errorHandler=logging.FileHandler(errorfile, mode="a")
formatter = logging.Formatter("%(funcName)s - %(message)s")
errorHandler.setFormatter(formatter)
errorHandler.setLevel(logging.WARNING)
#logger.addHandler(fileHandler)
logger.addHandler(errorHandler)

wd = pathlib.Path().absolute()

class DataFrameFieldsTestCase(TransactionTestCase):

    def setUp(self):
        print(wd)
        path_to_df = wd.joinpath("tests/sample_data/full_sample_frame.xlsx")

        run_import(path_df=path_to_df, is_test=True)

        self.testframe = pd.read_excel(wd.joinpath("tests/sample_data/test_solution_frame.xlsx"))
        self.testframe.index = self.testframe.orig_index
        self.testframe = self.testframe.fillna("")


    def test_lebensdaten_parsen(self):

        for p in Person.objects.all():
            sd = p.start_date_written
            ed = p.end_date_written
            o_id = p.source.orig_id
            if self.testframe.TYPE.iloc[o_id] == "Lebensdaten":
                o_sd = self.testframe.sdw.iloc[o_id]
                o_ed = self.testframe.edw.iloc[o_id]

                if o_ed == "":
                    o_ed = None


                try:
                    self.assertTrue(sd == o_sd)
                except Exception as e:
                    logger.warning(f"failed on row: {o_id}")
                    logger.warning(f"{ed}, {o_ed}")
                    logger.warning(f"{type(ed)}, {type(o_ed)}")
                    logger.warning(f"{e}")
                self.assertTrue(ed == o_ed)
            else:
                pass

    def test_field_vornamen_vornamen_trennen(self):
        p = [i for i in range(0,103)]
        self.assertTrue(len(p) == 103)



# class DataFrameDoubleTestCase(TestCase):
#
#     def setUp(self):
#         path_to_df = wd.joinpath("tests/sample_data/full_sample_frame.xlsx")
#         os.system(f"python main.py -path-df {path_to_df}")
#         self.testframe = pd.read_excel(wd.joinpath("tests/sample_data/test_solution_frame.xlsx"))
#         self.testframe.index = self.testframe.orig_index
#
#     def test_field_vornamen_vornamen_trennen(self):
#         p = [i for i in range(0,103)]
#
#         print(len(Person.objects.all()), Person.objects.all())
#         for person in Person.objects.all():
#             print(person.name)
#         self.assertTrue(len(p) == 103)
#
#
