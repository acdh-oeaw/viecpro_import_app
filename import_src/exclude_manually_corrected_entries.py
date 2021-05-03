from apis_core.apis_entities.models import Person

def create_checklist(id_R=None, id_K=None, id_S=None):
    """
    todo: This most likely won't work when we have imports from additional sources.
    """
    K = []
    R = []
    S = []
    for person in Person.objects.all():
        for collection in person.collection.values_list():
            if collection[0] == 4:
                K.append(person)
            elif collection[0] == 5:
                R.append(person)
            elif collection[0] == 6:
                S.append(person)

    S.sort(key=lambda x: x.name)
    K.sort(key=lambda x: x.name)
    R.sort(key=lambda x: x.name)

    def get_stop_index(collection, stopvalue):
            for idx, per in enumerate(collection):
                if per.id == stopvalue:
                    return idx

    K_index = get_stop_index(K, id_K)
    R_index = get_stop_index(R, id_R)
    S_index = get_stop_index(S, id_S)

    dont_touch = K[:K_index+1]+R[:R_index+1]+S[:S_index+1]

    dont_id = [per.id for per in dont_touch]
    dont_orig_id = [per.source.orig_id for per in dont_touch]

    return dont_orig_id



