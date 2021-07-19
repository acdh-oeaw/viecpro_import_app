try:
    from viecpro_import_app.file_sets.FileSet import FileSet, current_paths, save_to_current_paths
except:
    from file_sets.FileSet import FileSet, current_paths, save_to_current_paths

for name in ["funktionen", "places", "hofstaate"]:
    if not name in current_paths.keys():
        print(f"Name not in paths, save called for {name}, added empty path.")
        current_paths[name] = ""
        save_to_current_paths()

print(current_paths)

F = FileSet(name="funktionen", file=current_paths["funktionen"])
P = FileSet(name="places", file=current_paths["places"])
H = FileSet(name="hofstaate", file=current_paths["hofstaate"])
FK = FileSet(name="FunktionKombiniert", file=current_paths["FunktionKombiniert"])

if __name__ == "__main__":
    pass

