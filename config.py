import os, django

class cfg:

    django_settings = None
    collection = None
    df_aemter = None
    df_hofstaat = None
    df_abbreviations = None
    df = None
    list_abbreviations = None
    lst_hofst = None
    df_aemter_index = None
    nlp = None
    logfile = None
    create_all = True
    annotations = None

    def set_settings(value):
        cfg.django_settings = value
        os.environ["DJANGO_SETTINGS_MODULE"] = value
        django.setup()

