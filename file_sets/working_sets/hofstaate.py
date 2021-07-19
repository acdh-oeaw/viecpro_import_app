import pandas as pd
import pickle
import re


with open("test_data/HZAB.pkl", "rb") as file:
    df = pickle.load(file)


# This function is unnecessary now, it has been run in the notebook already and its content added to the FileSet H.
# if run again, discard("NN")
pattern = re.compile(r"[A-Z]{1,}")
caught = set()
for entry in df.Hofstaat:
    if isinstance(entry, str):
        matches = pattern.finditer(entry)
        for match in matches:
            caught.add(match.group(0))
