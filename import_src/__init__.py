import pathlib, sys

parent_path = pathlib.Path().absolute().parent
sys.path.append(str(parent_path))
apis_core = parent_path.joinpath("apis_core")
sys.path.append(str(apis_core))
