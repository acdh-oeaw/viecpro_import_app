# VieCPro Import App

Project to automate and test the data import for VieCPro.


## STATUS 

- command line tool implemented wit plac (for starting the import)
- component-tests implemented for RenameFunctions, date_apis-extension and
    CreateChunks
- testset is stored as a random selection of the original .xlsx, the random seed
    is stored in the filename to be reproducable
- tests are run via:

~~~bash 
python manage.py test tests
~~~

## TODO

- setup dev-branch 
- setup dev-server
- setup CI/CD
- expand testset with more and better selected cases
- update pipeline, insert entity ruler
- refactor file/module structure (split functions into modules within directories)
- write end-to-end test for import-pipeline
- implement EntityRuler for Hofstaate and Ämter, to improve performance
