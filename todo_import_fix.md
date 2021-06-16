titel 

-- TITEL verstehe ich nicht? Wo sollten die angezeigt werden? Bei Abensberg Traun ist der Titel sehr wohl hinterlegt, wenn man den Eintrag auf bearbeiten ansieht. Was ist da falsch / was verstehe ich nicht?

sollten erkennt werden aber müsste am speichern liegen 


institution amt wird nicht erkannt. 

ämter und hofstaate werden nicht richtig geordnet.




ämter nur erstes amt, rest dummy

hofstaat bie allen den aus sder spalte 


Funktionspalten EntityRuler reinnehmen.  

Großbuchstaben Probleme,, überhaupt einfache ämter nicht gesehen .


daten besser parsen 


warum erkennt er nicht, wenn nur eine funktion angegeben ist.

debugging von den pipeline components.


# Lösungen / todo

## funktion amt hofstaat - regeln: 

wenn amt und oder hofstaat in funktionsspalten chunk: vorrang.

wenn nicht: 

ämterspalte: 

anzahl ämter == anzahl chunks: 
zuordnen 

wenn nicht: 

erstes amt für erste funktion, rest amt dummy

hofstaatsspalte: 

anzahl hofstaate == anzahl chunks: 
zuordnen 

wenn nicht: 

erster Hofstaat allen funktionen zuordnen


## Titel: 

Titel werden nicht geschrieben

## Daten: 

Daten besser parsen. 

- check: chunks date preprocessing
- check: helper hsv dates (wird es für alle fälle angewendet) 


Debugging: 

- Wo werden Daten überall verarbeitet, wie ist das Zusammenspiel
- 

## Funktionen 

- erste Funktion wird oft nicht erkannt (warum ?) 
- Funktion mit Großbuchstaben wird nicht erkannt 

---> zwei Dinge also: 

Debugging - Warum werden die Funktionen nicht erkannt? Fallen sie im Prozess irgendwo wieder heraus? 

Solution A: Entity Ruler wieder einschalten. Neue, umfassendere Funktionsliste erstellen. 

Funktionsliste erstellen: Aus den existierenden Relationen in der Datenbank (der korrigierten Collection)

Funktionsliste aus erseten Funktionen. 

Händisch reinnehmen im Notebook, mit dem FileSet. 


# todo: 

- zuerst: refactor von ämter - matching nach den regeln. 





Frage: im original script: 

gab es in chunk prcess amt drei möglichkeiten: 

~~~python 
1
if isinstance(r_A, str) and idx_chunk == 0:

-->
try:
    inst2, inst3, created = get_or_create_amt(
        row, df_aemter_index, lst_hofst
    )
    if created:
        InstitutionInstitution.objects.create(
            related_institutionA=inst3,
            related_institutionB=inst,  # inst comes again
            relation_type=rl_teil_von,
        )
except Exception as e:
    inst2 = amt_dummy
    inst3 = False
    print(f"Exception in Amt function: {e}")

2
elif c_A is not None:
-->         inst2, created = Institution.objects.get_or_create(name=amt_name)


3
else:
--> inst2, created = Institution.objects.get_or_create(
            name=f"Dummy Amt ({nm_hst})"
        )
~~~

Mich interessieren hier die Arten, das Amt zu schreiben. 

bei 1. passiert folgendes in get_or_create_amt: 

~~~python 
beh_name = re.match(r"^(.+)[/\\(]", row["Amt/Behörde"])
    if beh_name:
        amt = beh_name.group(1).strip()
    else:
        amt = row["Amt/Behörde"].strip()
    try:
        row_target = df_aemter_index[amt]
    except:
        raise ValueError(f"couldnt find matching Amt entry for {amt}")
    if isinstance(row.iloc[11], str):
        if row.iloc[11] in lst_hofst.keys():
            name_hst_1 = lst_hofst[row.iloc[11]][0]
        else:
            name_hst_1 = row.iloc[11]
    else:
        name_hst_1 = "Dummy Hofstaat"
    test_unsicher = False
    if name_hst_1 == "UNSICHER - Collection, manuelle Entscheidung":
        name_hst_1 = "UNSICHER"
        test_unsicher = True
    name_hst_1 = name_hst_1.replace('Hofstaat', '').strip()
    inst_type, c3 = InstitutionType.objects.get_or_create(name=row_target.iloc[7])
    amt_ent, created = Institution.objects.get_or_create(name=f'{row_target["APIS-Vereinheitlichung"]} ({name_hst_1})', kind=inst_type)
    amt_ent.collection.add(col_unsicher)
    amt_super = False
    created2 = False
    if created:
        if row_target.iloc[8] in lst_hofst.keys():
            name_hst_2 = lst_hofst[row_target.iloc[8]][0]
        else:
            name_hst_2 = row_target.iloc[8]
        amt_super, created2 = Institution.objects.get_or_create(name=name_hst_2, kind=inst_type)
        InstitutionInstitution.objects.create(related_institutionA_id=amt_ent.pk, related_institutionB_id=amt_super.pk, relation_type=rl_teil_von)
    return amt_ent, amt_super, created2

~~~

das wurde alos nur für das erste amt gemacht. siehe oben 1. idx chunk musste 0 sein.

dann wurde diese meta_relation geamcht 
get or create amt hat returned: 
inst2, inst3, created

udn dann wurde eine InstitutionInstitutionRelation geabut. zwischen inst3 und inst (!)

## Einfache Funktionen nicht gefunden: 

nicht gefunden: 
also gar keine Funktion / Relation
Mayblir, Fabian 

~~~
aus Quelle:
Funktion
    Expeditor
Amt/Behörde
    BHR
Hofstaat
    L
~~~
Auszug aus dem Log aber: 

~~~
chunk_process_amt_NEW >>>  Return value of inst2 = BHR (L1 (Ks.))
chunk_create_relations >>> create realtions called for c_F ['Expeditor']
~~~


These: kann das an der get_or_create_amt relation liegen? 


gleiches Problem, aber mit erster Funktion: 

~~~
--------- Start of row | 422 | -------------- 
replacer >>> r_fun: replaced ); with: ) ;
person_process_field_vorname >>> r_vor
person_process_field_familienname >>> Binder, von
person_process_field_familienname >>> Init test: 'von in fam' INPUT: Binder, von END INPUT, ROW:422
NLP COMPONENT >>> UseExistingAnnotations.py >>> Entered call
NLP COMPONENT >>> UseExistingAnnotations.py >>> doc._excel_row was not in annotations.keys
NLP COMPONENT >>> CreateChunks.py >>> this was finally written, chunk(2) = [{'DATUM': ['1669-00-00', 'bis 1704<1704-06-30>'], 'HOFSTAAT': None, 'FUNKTION': ['Reichshofrat'], 'AMT': None}, {'DATUM': ['1701-00-00'], 'HOFSTAAT': None, 'FUNKTION': ['Envoye'], 'AMT': None}]
NLP COMPONENT >>> CreateChunks.py >>> 	0: chunk: {'DATUM': ['1669-00-00', 'bis 1704<1704-06-30>'], 'HOFSTAAT': None, 'FUNKTION': ['Reichshofrat'], 'AMT': None}
NLP COMPONENT >>> CreateChunks.py >>> 	1: chunk: {'DATUM': ['1701-00-00'], 'HOFSTAAT': None, 'FUNKTION': ['Envoye'], 'AMT': None}
process_chunks >>> len_doc_chunks: 2, len Ämter-Spalte: 2
helper_hsv_match_hofstaate >>> r_H = L
helper_hsv_match_hofstaate >>> chunk is -- > {'DATUM': ['1669-00-00', 'bis 1704<1704-06-30>'], 'HOFSTAAT': 'L', 'FUNKTION': ['Reichshofrat'], 'AMT': None}
helper_hsv_match_hofstaate >>> chunk is -- > {'DATUM': ['1701-00-00'], 'HOFSTAAT': 'L', 'FUNKTION': ['Envoye'], 'AMT': None}
helper_hsv_match_amt_with_funct >>> r_A = RHR / Ritter- , Gelehrtenstand, AC; Abgesandte
helper_hsv_match_amt_with_funct >>> r_A equals len(Chunks) -> c[amt] = RHR 
helper_hsv_match_amt_with_funct >>> r_A equals len(Chunks) -> c[amt] =  Abgesandte
process_chunks >>> cD ['1669-00-00', 'bis 1704<1704-06-30>'], cF ['Reichshofrat'], cH L, CA RHR 
helper_hsv_post_process_dates >>> old: 1669-00-00, new: 1669-00-00<1669-06-30>
helper_hsv_post_process_dates >>> inner date: <1704-06-30>
helper_hsv_post_process_dates >>> new_i_date: <1704-06-30>
helper_hsv_post_process_dates >>> old: bis 1704<1704-06-30>, new: bis 1704<1704-06-30>
chunk_process_datum >>> rel, chunk sdw: 1669-00-00<1669-06-30>
chunk_process_datum >>> rel, chunk edw: 1669-00-00<1669-06-30>
chunk_process_datum >>>  this is the full relation: {'related_person': <Person: Binder, von, Johann Friedrich>, 'start_date_written': '1669-00-00<1669-06-30>', 'end_date_written': 'bis 1704<1704-06-30>'}
chunk_get_nm_hst >>> c_H = L
chunk_create_institution >>> nm_hst is not Dummy Hofstaat: L1 (Ks.)
chunk_process_amt_NEW >>> c_A in if c_A true: RHR 
chunk_process_amt_NEW >>> c_A was: RHR  and amt after matching with amt index is: RHR
chunk_process_amt_NEW >>>  Return value of inst2 = RHR (L1 (Ks.))
chunk_create_relations >>> create realtions called for c_F ['Reichshofrat']
process_chunks >>> cD ['1701-00-00'], cF ['Envoye'], cH L, CA  Abgesandte
helper_hsv_post_process_dates >>> old: 1701-00-00, new: 1701-00-00<1701-06-30>
chunk_process_datum >>> rel, chunk sdw: 1701-00-00<1701-06-30>
chunk_process_datum >>>  this is the full relation: {'related_person': <Person: Binder, von, Johann Friedrich>, 'start_date_written': '1701-00-00<1701-06-30>'}
chunk_get_nm_hst >>> c_H = L
chunk_create_institution >>> nm_hst is not Dummy Hofstaat: L1 (Ks.)
chunk_process_amt_NEW >>> c_A in if c_A true:  Abgesandte
chunk_process_amt_NEW >>> c_A was:  Abgesandte and amt after matching with amt index is: Abgesandte
chunk_process_amt_NEW >>> exception caught and handled here. Multiple Institutions for Abgesandte (L1 (Ks.))
chunk_process_amt_NEW >>>  Return value of inst2 = Abgesandte (L1 (Ks.))
chunk_create_relations >>> create realtions called for c_F ['Envoye']
~~~

--> nur Envoye in der DB


todo: 

note: 

i am catching all ämter that werent in df_aemte_index, which means they weren’t in df_amter kürzel access 

i log them to separate file 

todo: 

some names like abensberg now have a „ „ before coma between frist and last name

implement a decision logger, that logs to separate directory, 

with metainfo on import data, 


- improve date parsing

- resolve issue with InstitutionInstitution Relation 

- list other name combinations in last names like , de , von , d', von und zu (not yet listed!)

- implement a pipeline component before ner, that resolves missing whitespaces between words!