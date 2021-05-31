from django.test import TestCase, TransactionTestCase
import pathlib, django, os
from apis_core.apis_entities.models import Person
#os.environ["DJANGO_SETTINGS_MODULE"] = "django_settings.viecpro_testing"
#django.setup()
import pandas as pd
from main import run_import
from import_src.create_database_objects import create_all
wd = pathlib.Path().absolute()

class DataFrameFieldsTestCase(TestCase):

    def setUp(self):
        serialized_rollback = True

        path_to_df = wd.joinpath("tests/sample_data/full_sample_frame.xlsx")
        #create_all()
        #os.system("python manage.py migrate --database default")
        run_import(path_df=path_to_df)
        self.testframe = pd.read_excel("~/Desktop/myframe.xlsx")
        self.testframe.index = self.testframe.orig_index


    def test_lebensdaten_parsen(self):
        #os.system("python manage.py migrate --database default")

        for p in Person.objects.all():
            sd = p.start_date_written
            ed = p.end_date_written
            o_id = p.source.orig_id
            if self.testframe.TYPE.iloc[o_id] == "Lebensdaten":
                o_sd = self.testframe.sdw.iloc[o_id]
                o_ed = self.testframe.edw.iloc[o_id]

                try:
                    self.assertTrue(sd == o_sd)
                except Exception as e:
                    print("failed on row: ",o_id )
                    print(sd, o_sd)
                self.assertTrue(ed == o_ed)
            else:
                pass


class DataFrameDoubleTestCase(TestCase):

    def setUp(self):
        serialized_rollback = True

        #os.system("python manage.py migrate --database default")
        path_to_df = wd.joinpath("tests/sample_data/full_sample_frame.xlsx")
        #create_all()
        run_import(path_df=path_to_df)
        self.testframe = pd.read_excel("~/Desktop/myframe.xlsx")
        self.testframe.index = self.testframe.orig_index

    def test_field_vornamen_vornamen_trennen(self):
        #os.system("python manage.py migrate --database default")
        p = Person.objects.all()
        print(len(p))
        self.assertTrue(len(p) == 103)



