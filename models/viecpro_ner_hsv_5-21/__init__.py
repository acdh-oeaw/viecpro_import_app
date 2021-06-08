from pathlib import Path
from spacy.util import load_model_from_init_py, get_model_meta

from spacy.language import Language
from .additional_components import *

__version__ = get_model_meta(Path(__file__).parent)['version']


def load(**overrides):
    Language.factories["use_existing_annotations"] = lambda nlp, "data_viecpro/viecpro_HZAB_funktion_0.jsonl", **cfg: UseExistingAnnotations(nlp, "data_viecpro/viecpro_HZAB_funktion_0.jsonl", **cfg)
    Language.factories["add_brackets"] = lambda nlp, **cfg: AddBrackets()
    Language.factories["rename_functions"] = lambda nlp, **cfg: RenameFunctions()
    Language.factories["date_prepocissions"] = lambda nlp, **cfg: DatePrepocissions()
    Language.factories["create_chunks"] = lambda nlp, **cfg: CreateChunks()
    return load_model_from_init_py(__file__, **overrides)
