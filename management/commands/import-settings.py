from django.core.management.base import BaseCommand, CommandError
import sys
import pathlib

path = pathlib.Path(__file__).absolute().parent.parent.parent
sys.path.append(str(path))

class Command(BaseCommand):
    help = "this is a teststring for the help"

    def handle(self, *args, **options):
        #todo: write commands for username, password, etc. to update settings.yaml
        pass

