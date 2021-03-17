import re
from apis_core.apis_entities.models import Person, Institution, Title
from apis_core.apis_metainfo.models import Collection, Source, Text
from apis_core.apis_relations.models import PersonInstitution, InstitutionInstitution
from apis_core.apis_vocabularies.models import TextType, InstitutionType, LabelType, InstitutionInstitutionRelation, PersonInstitutionRelation
from copy import deepcopy

inst_type_hst, created = InstitutionType.objects.get_or_create(name="Hofstaat")
inst_type_beh, created = InstitutionType.objects.get_or_create(name="Amt/Behörde")
lt_vorname_alternativ, created = LabelType.objects.get_or_create(name="alternativer Vorname")
lt_nachname_alternativ, created = LabelType.objects.get_or_create(name="alternativer Nachname")
lt_nachname_verheiratet, created = LabelType.objects.get_or_create(name="Nachname verheiratet")
lt_nachname_alternativ_verheiratet, created = LabelType.objects.get_or_create(name="Nachname alternativ vergeiratet")
rl_teil_von, created = InstitutionInstitutionRelation.objects.get_or_create(name="untergeordnet", name_reverse="übergeordnet")
rl_pers_hst, created = PersonInstitutionRelation.objects.get_or_create(name="am Hofstaat")
amt_dummy, created = Institution.objects.get_or_create(name="AMT DUMMY", kind=inst_type_beh)
col_unsicher, created = Collection.objects.get_or_create(name="UNSICHER")


def get_or_create_amt(row, df_aemter_index, lst_hofst):
    beh_name = re.match(r"^(.+)[/\\(]", row["Amt/Behörde"])
    if beh_name:
        amt = beh_name.group(1).strip()
    else:
        amt = row["Amt/Behörde"].strip()
    try:
        row_target = df_aemter_index[amt]
    except:
        raise ValueError(f"couldnt find matching Amt entry for {amt}")
    if isinstance(row.iloc[11], str):
        if row.iloc[11] in lst_hofst.keys():
            name_hst_1 = lst_hofst[row.iloc[11]][0]
        else:
            name_hst_1 = row.iloc[11]
    else:
        name_hst_1 = "Dummy Hofstaat"
    test_unsicher = False
    if name_hst_1 == "UNSICHER - Collection, manuelle Entscheidung":
        name_hst_1 = "UNSICHER"
        test_unsicher = True
    name_hst_1 = name_hst_1.replace('Hofstaat', '').strip()
    inst_type, c3 = InstitutionType.objects.get_or_create(name=row_target.iloc[7])
    amt_ent, created = Institution.objects.get_or_create(name=f'{row_target["APIS-Vereinheitlichung"]} ({name_hst_1})', kind=inst_type)
    amt_ent.collection.add(col_unsicher)
    amt_super = False
    created2 = False
    if created:
        if row_target.iloc[8] in lst_hofst.keys():
            name_hst_2 = lst_hofst[row_target.iloc[8]][0]
        else:
            name_hst_2 = row_target.iloc[8]
        amt_super, created2 = Institution.objects.get_or_create(name=name_hst_2, kind=inst_type)
        InstitutionInstitution.objects.create(related_institutionA_id=amt_ent.pk, related_institutionB_id=amt_super.pk, relation_type=rl_teil_von)
    return amt_ent, amt_super, created2

def process_names(name):
    s1 = re.search(r"([a-zA-Z][a-z]+)/((.+)/)", name)
    if s1:
        return s1.group(1), f"{s1.group(1)}{s1.group(2)}"
    else:
        return name, None

map_text_type = {}
def create_text_entries(row, pers, df, src_base, columns=(5,18)):
    for idx in range(5,18):
        if isinstance(row.iloc[idx], str):
            c_name = df.columns[idx]
            if c_name not in map_text_type.keys():
                tt, created = TextType.objects.get_or_create(name=c_name, entity="Person")
                map_text_type[c_name] = tt
            src_base_1 = deepcopy(src_base)
            src_base_1["pubinfo"] += f"/noriginal text from: {c_name} column"
            st = Source.objects.create(**src_base_1)
            t_fin = Text.objects.create(text=row.iloc[idx], source=st, kind=map_text_type[c_name])
            pers.text.add(t_fin)

col, created = Collection.objects.get_or_create(name="Import HZAB full 10-3-21")