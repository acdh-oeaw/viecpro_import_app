#!/usr/bin/fish

#python manage.py reset_db --noinput && python manage.py migrate && deactivate && cd  ~/gregor/arbeit/acdh/apis_final/ && source venv/bin/activate.fish && cd apis-webpage-base && python manage.py migrate && deactivate && cd ~/gregor/arbeit/acdh/viecpro_import_for_upload/ && source venv/bin/activate.fish && cd viecpro_import_app && python manage.py createsuperuser

python manage.py reset_db --noinput
python manage.py migrate
deactivate
cd  ~/gregor/arbeit/acdh/apis_final/
source venv/bin/activate.fish
cd apis-webpage-base
python manage.py migrate
deactivate
cd ~/gregor/arbeit/acdh/viecpro_import_for_upload/
source venv/bin/activate.fish
cd viecpro_import_app
python manage.py createsuperuser

echo "Database Reset Completed Successfully"