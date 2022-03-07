import spacy_dbpedia_spotlight
from SPARQLWrapper import SPARQLWrapper, JSON
import json
from db_core.database import Database
from collections import defaultdict


def annotator(keywords: 'list', confidence=0.5, verbose=False):
    # create nlp pipe
    nlp = spacy_dbpedia_spotlight.create('en')
    nlp.get_pipe('dbpedia_spotlight').confidence = confidence

    # convert keyword list into a 'document'
    keywords_doc = " ".join(keywords)

    # create nlp document
    doc = nlp(keywords_doc)

    # annotate keyword document
    annotations = [(ent.text, ent.kb_id_, ent._.dbpedia_raw_result['@similarityScore']) for ent in doc.ents]

    if verbose:
        print(keywords_doc)
        print(annotations)

    # get rdf:types of annotation resource
    sparql_query = 'SELECT DISTINCT * WHERE { <REPLACE> a ?subject FILTER strstarts(str(?subject), str(yago:Wiki))}'

    results = defaultdict(list)
    for i in annotations:
        if i[0] in results.keys():
            continue
        sparql = SPARQLWrapper('http://dbpedia.org/sparql', returnFormat=JSON)
        i_query = sparql_query.replace('REPLACE', str(i[1]))
        sparql.setQuery(i_query)

        sparql.setReturnFormat(JSON)

        subjects = sparql.query().convert()
        for j in subjects['results']['bindings']:
            results[i[0]].append(j['subject']['value'][37:])
            if verbose:
                print(j)

    return results


if __name__ == '__main__':
    db = Database()

    # create list of keywords
    keyword_list = db.execute_sql(sql='''
            SELECT results FROM datalake.keyword_extraction_results
            WHERE episode_uri_id = '3aapVuXCEtmrR89oa8m2BS' AND expirement_uuid = 'aadb5075-3047-4a64-9538-a62d90947851'
    ''', return_list=True)

    keyword_list = json.loads(keyword_list[0])['keywords']

    print(annotator(keyword_list, confidence=0.5, verbose=True))
