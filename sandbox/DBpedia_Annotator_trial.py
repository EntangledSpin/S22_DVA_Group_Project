
import spacy_dbpedia_spotlight
from SPARQLWrapper import SPARQLWrapper, JSON
import json
from db_core.database import Database


db = Database()
nlp = spacy_dbpedia_spotlight.create('en')

##create list of keywords
keywords = db.execute_sql(sql = '''
        SELECT results FROM datalake.keyword_extraction_results
        WHERE episode_uri_id = '68Eyz1s96eGvjbFATMOJyN' AND expirement_uuid = '11ecfe61-2777-4e0e-9d21-a480749d5003'
''', return_list=True)

keywords = json.loads(keywords[0])['keywords']

#convert keyword list into a 'document'
keywords_doc = ", ".join(keywords)

print(keywords_doc)

#create nlp document
doc = nlp(keywords_doc)

#annotate keyword document
annotations = [(ent.text, ent.kb_id_, ent._.dbpedia_raw_result['@similarityScore']) for ent in doc.ents]

print(annotations)


#get rdf:types of annotation resource
sparql_query = 'SELECT DISTINCT * WHERE { <REPLACE> a ?subject FILTER strstarts(str(?subject), str(yago:Wiki))}'

results = []
for i in annotations:
    print(i[1])
    sparql = SPARQLWrapper('http://dbpedia.org/sparql', returnFormat=JSON)

    sparql.setQuery(sparql_query.replace('REPLACE', str(i[1])))

    sparql.setReturnFormat(JSON)

    results.append(sparql.query().convert())

print(results)


