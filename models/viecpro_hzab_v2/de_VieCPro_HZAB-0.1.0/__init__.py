from pathlib import Path
from spacy.util import load_model_from_init_py, get_model_meta

from spacy.language import Language
from .components import *

__version__ = get_model_meta(Path(__file__).parent)["version"]


def load(**overrides):
    Language.factories[
        "use_existing_annotations"
    ] = lambda nlp, **cfg: UseExistingAnnotations(nlp, **cfg)
    Language.factories["add_brackets"] = lambda nlp, **cfg: AddBrackets(nlp, **cfg)
    Language.factories["extend_entities"] = lambda nlp, **cfg: ExtendEntities(
        nlp, **cfg
    )
    Language.factories["rename_functions"] = lambda nlp, **cfg: RenameFunctions(
        nlp, **cfg
    )
    Language.factories["date_prepocissions"] = lambda nlp, **cfg: DatePrepocissions(
        nlp, **cfg
    )
    Language.factories["create_chunks"] = lambda nlp, **cfg: CreateChunks(nlp, cfg)
    return load_model_from_init_py(__file__, **overrides)
