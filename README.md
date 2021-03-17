# VieCPro Import App

Django Application to automate and test the data import for VieCPro.

## Installation
*WARNING*: **Only run the import on a test-database as the import is not completely up-to-date.**

- Clone Repository.

- Symlink in Project and add App to Installed-Apps in django-settings-module. 

- Customize the settings_template.yml in viecpro_import_app/settings and rename it to settings.yml before running the import. 
Sharepoint Password and Username need to be set, the rest is not yet implemented.


Import is run via the 'manage.py run_import' - command.

