from apis_core.apis_entities.models import Institution
from apis_core.apis_metainfo.models import Collection
from apis_core.apis_vocabularies.models import InstitutionType, LabelType, \
    InstitutionInstitutionRelation, PersonInstitutionRelation

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
lt_titel_datiert, created = LabelType.objects.get_or_create(name="Titel datiert")

def create_all():
    inst_type_hst, created = InstitutionType.objects.get_or_create(name="Hofstaat")
    inst_type_beh, created = InstitutionType.objects.get_or_create(name="Amt/Behörde")
    lt_vorname_alternativ, created = LabelType.objects.get_or_create(name="alternativer Vorname")
    lt_nachname_alternativ, created = LabelType.objects.get_or_create(name="alternativer Nachname")
    lt_nachname_verheiratet, created = LabelType.objects.get_or_create(name="Nachname verheiratet")
    lt_nachname_alternativ_verheiratet, created = LabelType.objects.get_or_create(
        name="Nachname alternativ vergeiratet")
    rl_teil_von, created = InstitutionInstitutionRelation.objects.get_or_create(name="untergeordnet",
                                                                                name_reverse="übergeordnet")
    rl_pers_hst, created = PersonInstitutionRelation.objects.get_or_create(name="am Hofstaat")
    amt_dummy, created = Institution.objects.get_or_create(name="AMT DUMMY", kind=inst_type_beh)
    col_unsicher, created = Collection.objects.get_or_create(name="UNSICHER")
    lt_titel_datiert, created = LabelType.objects.get_or_create(name="Titel datiert")
