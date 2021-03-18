from django.test import TestCase
import os
from django.conf import settings
import spacy


class ExtractionTestCase(TestCase):
    def setup(self):
        nlp_set = settings.get("NLP_SETTINGS")

        nlp = spacy.load(nlp_set["model_path"])