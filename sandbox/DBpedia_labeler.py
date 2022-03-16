import spacy_dbpedia_spotlight
from SPARQLWrapper import SPARQLWrapper, JSON
import json
import re
from db_core.database import Database
from collections import defaultdict


def annotator(keywords: 'list', confidence=0.5, verbose=False, split_camel_case=False):
    # create nlp pipe
    nlp = spacy_dbpedia_spotlight.create('en')
    nlp.get_pipe('dbpedia_spotlight').overwrite_ents = False
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

    results = []
    done = []
    for i in annotations:
        if i[0] in done:
            continue
        sparql = SPARQLWrapper('http://dbpedia.org/sparql', returnFormat=JSON)
        i_query = sparql_query.replace('REPLACE', str(i[1]))
        sparql.setQuery(i_query)

        sparql.setReturnFormat(JSON)

        subjects = sparql.query().convert()

        if split_camel_case:
            for j in subjects['results']['bindings']:
                        word = j['subject']['value'][37:].replace(',', '')
                        camel_case = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z,])(?=[A-Z][a-z])|$)', j['subject']['value'][37:])
                        camel_list = [c.group(0) for c in camel_case]
                        for l in camel_list:
                            results.append(l)

        else:
            for j in subjects['results']['bindings']:
                results.append(j['subject']['value'][37:])

            if verbose:
                print(j)
        done.append(i[0])

    return results


if __name__ == '__main__':
    db = Database()
    test_dict = defaultdict(list)

    experiment = 'cdecfed9-6fc8-4d97-ba0f-b53f58e19df2'  # enter experiment ID from keyword_extraction_results

    episode_list = db.execute_sql('''
           select distinct episode_uri_id from datalake.keyword_extraction_results
            where expirement_uuid = '{fexperiment}'
             limit 10;
    '''.format(fexperiment = experiment), return_list=True)  # returns list of episodes with selected experiment id

    for episode in episode_list:
        query = '''SELECT results FROM datalake.keyword_extraction_results
                    WHERE expirement_uuid = '{fexperiment}' AND episode_uri_id = '{fepisode}' 
                    '''.format(fexperiment = experiment, fepisode = episode)

        keyword_list = db.execute_sql(query, return_list=True)  # calls keyword lists for episode

        keywords = json.loads(keyword_list[0])['keywords']  # loads list of keywords

        test_dict[episode] = annotator(keywords, confidence=0.5, verbose=False, split_camel_case=True)  # annotates keyword list, generates yago types, and adds to dict

    print(dict(test_dict))
