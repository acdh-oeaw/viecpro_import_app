from .AddBrackets import AddBrackets
from .CreateChunks import CreateChunks
from .DatePrepocissions import DatePrepocissions
from .ExtendEntities import ExtendEntities
from .ExtendFunctions import ExtendFunctions
from .RemoveNames import RemoveNames
from .RenameFunctions import RenameFunctions
from .UseExistingAnnotations import UseExistingAnnotations

component_dic = {
    "add_brackets":AddBrackets,
    "create_chunks":CreateChunks,
    "date_prepocissions":DatePrepocissions,
    "extend_entities": ExtendEntities,
    "extend_functions":ExtendFunctions,
    "remove_names":RemoveNames,
    "rename_functions":RenameFunctions,
    "use_existing_annotations":UseExistingAnnotations
}