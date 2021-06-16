
def get_list_abbreviations(df_abbreviations):
    list_abbreviations = []
    for idx, row in df_abbreviations.iterrows():
        abb = row.iloc[0]
        if isinstance(abb, str):
            abb = abb.split(";")
            for abb_1 in abb:
                list_abbreviations.append((abb_1.strip(), row.iloc[2].strip()))
        list_abbreviations.sort(key=lambda tup: len(tup[0]), reverse=True)

    return list_abbreviations

def resolve_abbreviations(txt, list_abbreviations):
    for r1 in list_abbreviations:
        if r1[0] in txt:
            txt = txt.replace(r1[0], r1[1])
    return txt

def get_lst_hofst(df_hofstaat):
    lst_hofst = dict()
    for idx, row in df_hofstaat.iterrows():
        if isinstance(row.iloc[1], str):
            hst = [x.strip() for x in row.iloc[1].split(";")]
            alt_name = None
        else:
            breakpoint()
            pass
        if isinstance(row.iloc[2], str):
            alt_name = row.iloc[2]
        for ab in hst:
            lst_hofst[ab] = (row.iloc[4], idx, alt_name)
    return lst_hofst


def get_aemter_index(df_aemter):
    df_aemter_index = dict()
    for idx, row in df_aemter.iterrows():
        if isinstance(row.iloc[3], str):
            for sc in ["\n", ";"]:
                lst = row.iloc[3].split(sc)
                if len(lst) > 1:
                    for ent in lst:
                        df_aemter_index[ent.strip()] = row
                    break
    return df_aemter_index