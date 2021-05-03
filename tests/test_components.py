from django.test import TestCase
from viecpro_import_app.tests.helper_functions.functions import run_nlp_on_sample, convert_ents_to_string
from viecpro_import_app.tests.helper_functions.get_data_variables import *

class PrefixNamesTestCase(TestCase):
    def setUp(self):
        #nlp.disable_pipes("rename_functions") # uncommend to see if test fails as expected
        sample = "tests/sample_data/prefix_sample_len_20_seed_145.xlsx"
        res = run_nlp_on_sample(nlp, sample)

        self.res_dic = {}
        for doc in res:
            idx = doc._.excel_row
            ent_list = [ent._.renamed.strip() for ent in doc.ents if ent.label_ == "FUNKTION" and ent._.renamed]
            self.res_dic.update({idx: ent_list})

    def test_prefixes(self):
        for idx, doc in comp_docs_dic.items():
            for ent in doc.ents:
                try:
                    if ent._.renamed:
                        self.assertTrue(str(ent._.renamed).strip() in self.res_dic[idx])

                except AssertionError as e:
                    print(f"test_prefixes --> Failed on row: {idx}: '{ent._.renamed}' not in results.")
                    raise e

class ApisDateTestCase(TestCase):
    def setUp(self):
        sample = "tests/sample_data/prefix_sample_len_20_seed_145.xlsx"

        disabled = nlp.disable_pipes("create_chunks")
        res = run_nlp_on_sample(nlp, sample)
        disabled.restore()

        self.res_dic = {}
        for doc in res:
            self.res_dic.update({doc._.excel_row:doc})

    def test_apis_date_len(self):
        for key in comp_docs_dic.keys():
            try:
                self.assertEqual(len(comp_docs_dic[key]), len(self.res_dic[key]))
            except AssertionError as e:
                print(f"test_date_format (len) --> Failed on row: {key}.")
                print("compared this: ", len(comp_docs_dic[key]), "to this: ", len(self.res_dic[key]))
                raise e

    def test_apis_date_content(self):
        for idx, comp_doc in comp_docs_dic.items():
            res_doc = self.res_dic[idx]
            for ent in comp_doc.ents:
                ent_list = convert_ents_to_string(res_doc, "DATUM")
                if ent.label_ == "DATUM":
                    try:
                        self.assertTrue(ent.text.strip() in ent_list)
                    except AssertionError as e:
                        print(f"test_date_format (apis_date_content) --> Failed on row: {idx}: {ent.text.strip()} not in results.")
                        raise e


class ChunksTestCase(TestCase):
    def setUp(self):

        sample = "tests/sample_data/prefix_sample_len_20_seed_145.xlsx"
        # nlp.disable_pipes("rename_functions") # uncomment to test if tests fails correctly.
        res = run_nlp_on_sample(nlp, sample)

        self.res_chunk_dic = {}
        for doc in res:
            chunk_list = doc._.chunks
            self.res_chunk_dic.update({doc._.excel_row: chunk_list})

    def test_chunks_by_label(self):
        for idx, doc in comp_docs_dic.items():
            comp_chunks = doc._.chunks
            for index, chunk in enumerate(comp_chunks):
                for label, value in chunk.items():
                    try:
                        self.assertEqual(value, self.res_chunk_dic[idx][index][label])
                    except AssertionError as e:
                        print(f"test_chunks_by_label --> Failed on row: {idx}. Chunks didn't match.")
                        raise e


class PipelineTestCase(TestCase):
    def setUp(self):
        pass

    def test_pipeline(self):
        pass

