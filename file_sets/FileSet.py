import pickle
import os
import pathlib
import json

file_path = pathlib.Path(__file__).parent
data_path = file_path.joinpath("data/current_paths.json")

with open(data_path, "r") as jsonfile: #todo set this to a fixed path or absolute path.notebbok calls it from another working directory
    current_paths = json.load(jsonfile)

def save_to_current_paths():
    """
    Stores last known path for instances of registered FileSets in a file.
    If existing FileSet is saved to a new location, the new path will be stored here.
    """
    with open(data_path, "w") as file:
        data = json.dumps(current_paths)
        file.write(data)


# todo add current path json file that is tracked and overwritten in this class

class FileSet(set):
    """
    Extends the build-in set-class. The instances have to be initialised with a 'name' argument.
    The class is like a multi-variant Singleton: there can always only be one instance with a given name.

    The sets contents are stored in a pickled-file and loaded upon re-initialisation.
    Changes to the set in memory are mirrored to the file. Therefore these sets methods are extended to enable
    this behavior:

    - add()
    - remove()
    - discard()
    - pop()
    - union()

    todo: add method that converts and exports the sets content into a excel or csv-file for external use.
    """
    __instances = {}

    def __init__(self, *args, name, file, **kwargs):

        if name in FileSet.__instances.keys():
            """
            Prevents the initialisation of two Instances with the same name (like Hofstaate). 
            Doesn't prevent yet that two FileSets write to the same file (and maybe no need for that?).
            """
            print("check_instances called", "instance already exists")

        else:
            self._name = name
            self._file = file
            print(self._file)
            pickle_path = file_path.joinpath(self._file)
            if not os.path.exists(pickle_path):
                print(f"init called for {self._name}")
                super().__init__(*args, **kwargs)
                self._update_current_path(file) # todo test if this works

            else:
                print(f"self._load() called for {self._name}")
                self.init_load()
            FileSet.__instances.update({self._name:self})

    def init_load(self):
        pickle_path = file_path.joinpath(self._file)
        with open(pickle_path, "rb") as file:
            data = pickle.load(file)
        super().__init__(data) # todo let _load return the data

    def reload(self):
        self.init_load()

    def save(self, path=None):
        if path:
            self._file = file_path.joinpath(path)  #todo write test for this
            self._update_current_path(path) #todo test if this works
        else:
            path = file_path.joinpath(self._file)

        with open(path, "wb") as store:
            pickle.dump(list(self), store)

    def union(self, *args, **kwargs):
        """
        Overwrites set method union, so that the current FileSet is updated and returned, not a set-object.
        This behavior could made optional.
        """
        result = super().union(*args, **kwargs)
        for element in result:
            super().add(element)
        return self

    def add(self, *args, **kwargs):
        super().add(*args, **kwargs)
        self.save()

    def discard(self, *args, **kwargs):
        super().discard(*args, **kwargs)
        self.save()

    def pop(self, *args, **kwargs):
        super().pop()
        self.save()

    def remove(self, *args, **kwargs):
        try:
            super().remove(*args, **kwargs)
            self.save()
        except KeyError:
            print(f"KeyError handled, {args} was not in keys. \nNothing changed.\n Consider using 'discard(key)' instead.")
            pass

    def _update_current_path(self, path):
        """
        If a new path is set for the Instance, the path will be stored in an external file and loaded upon re-Initialisation.
        """
        current_paths[self._name] = path
        save_to_current_paths()


if __name__ == "__main__":
    from working_sets.SETS import F

    for element in F:
        print(element)





