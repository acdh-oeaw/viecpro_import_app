from django.core.management.base import BaseCommand, CommandError
import sys
import pathlib

path = pathlib.Path(__file__).absolute().parent.parent.parent
sys.path.append(str(path))

from main import run_import

class Command(BaseCommand):
    help = "this is a help teststring"

    def handle(self, *args, **options):
        self.stdout.write("This is working")
        run_import()
