from apis_core.apis_entities.models import Person

from apis_core.apis_metainfo.models import Collection


def create_ignorelist(id_R=None, id_K=None, id_S=None):
    """
    todo: This most likely won't work when we have imports from additional sources. Or? Has to be thought through.
    todo: Problems might occur if a person's name is changed, resulting in a position change of the entry.
    todo: Unresolved: how to deal with the existing entries that are not part of the ignorelist? These should be deleted upon re-import!
    """

    CR = Collection.objects.get(name="MRomberg (HSV)")
    CK = Collection.objects.get(name="MKaiser (HSV)")
    CS = Collection.objects.get(name="CStandhartinger (HSV)")

    perR = list(Person.objects.all().filter(collection=CR))
    perK = list(Person.objects.all().filter(collection=CK))
    perS = list(Person.objects.all().filter(colelction=CS))

    perS.sort(key=lambda x: x.name)
    perK.sort(key=lambda x: x.name)
    perR.sort(key=lambda x: x.name)

    def get_stop_index(collection, stopvalue):
            for idx, per in enumerate(collection):
                if per.id == stopvalue:
                    return idx

    K_index = get_stop_index(perK, id_K)
    R_index = get_stop_index(perR, id_R)
    S_index = get_stop_index(perS, id_S)

    do_not_touch = perK[:K_index+1]+perR[:R_index+1]+perS[:S_index+1]

    do_not_id = [per.id for per in do_not_touch] #This could be converted to a list of person objects that should be removed before re-import
    do_not_touch_orig_id = [per.source.orig_id for per in do_not_touch]

    return do_not_touch_orig_id



