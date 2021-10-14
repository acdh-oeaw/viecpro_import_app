import os
import pathlib
import random
from shutil import copy2
import shutil

import django


path = pathlib.Path(__file__).parent

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_settings.apis_backup")
django.setup()

from django.template.loader import render_to_string
from apis_core.apis_entities.models import Person, Institution
from apis_core.apis_metainfo.models import Collection




def get_person_html(data):
    template = data["template"]
    context = data["context"]
    per = context["object"]
    orig_id = per.source.orig_id
    context["relations"] = per.get_related_relation_instances()
    context["titles"] = per.title.all()
    context["labels"] = per.label_set.all()
    #context["places"] = per.place.all()
    content = render_to_string(template, context)

    print(per)
    print(context)


    path = f"./rendered_files/output_html_{orig_id}.html"

    with open(path, "w") as file:
        file.write(content)


def create_inst_html(path=path):

    for inst in Institution.objects.all():
        context = {}
        template = "inst_template.html"
        context["object"] = inst
        inst_name = inst.name
        context["relationsA"] = inst.related_institutionA.all()
        context["relationsB"] = inst.related_institutionB.all()
        for key in context.keys():
            print(context[key])
        if "  " in inst_name:
            print(f"double space in inst name: {inst_name}")
        print("\n")
        content = render_to_string(template, context)

        path = f"./rendered_files/institutions/{inst_name}.html"

        with open(path, "w") as file:
            file.write(content)


def get_prev(idx, objects):
    if idx != 0:
        return objects[idx-1].source.orig_id
    else:
        return objects[0].source.orig_id

def get_next(idx, objects):
    if idx+1 != len(objects):
        return objects[idx + 1].source.orig_id
    else:
        return objects[idx].source.orig_id



def run_render():
    # todo: create option to set output path
    # todo: create option for css to use and automate the copy of the css to export folder correctly
    # todo: BUG: for some Institutions, not all related Institutions are written. Needs fix

    print("working directory", os.getcwd())

    data = {"template": "./file_template.html", "context": {"object": "this will work"}}
    os.chdir(path)
    print("working directory", os.getcwd())

    shutil.rmtree("./rendered_files/")
    os.mkdir("./rendered_files")
    os.mkdir("./rendered_files/institutions")

    objects = Person.objects.all()
    #c = Collection.objects.filter(name__startswith="Access Import")[0]
    #per = [p for p in Person.objects.filter(source__orig_id__in=[1999,5598, 114, 4585]) if c in p.collection.all()]
    random.seed(23948123)

    def get_random_sample():
        sample = random.sample(list(Person.objects.all()), 1000)

        return sample

    # objects = get_random_sample()
    objects = list(objects)
    #objects = per
    #objects.sort(key=lambda x: x.source.orig_id)

    create_inst_html()

    for idx, person in enumerate(objects):
        data = {"template": "file_template.html", "context": {"object": person, "previous": get_prev(idx, objects), "next": get_next(idx, objects)}}
        get_person_html(data)

    copy2("render_file.css", "./rendered_files/render_file.css")


if __name__ == "__main__":
    run_render()