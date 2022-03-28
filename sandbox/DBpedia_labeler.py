import spacy_dbpedia_spotlight
from SPARQLWrapper import SPARQLWrapper, JSON
import re
from collections import defaultdict


def annotator(keywords: 'list', confidence: object = 0.5, verbose=False, split_camel_case=False):
    """
    Returns list of WikiCat Topics from DBPedia for a given list of keywords.

        Parameters:
            keywords (list): A list of keywords
            confidence (float): a float between 0 and 1 to define the confidence threshold for annotation (default 0.5)
            verbose (bool): prints values of intermediate steps for debugging purposes (default False)
            split_camel_case (bool): Splits WikiCat's into individual words based on CamelCasing if True (default False)

        Returns:
            results (list): A list of WikiCats (split into individual words if split_camel_case=True)
    """
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
                        camel_list = [c.group(0).lower() for c in camel_case]
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
    import json
    import ast
    from db_core.database import Database

    db = Database()
    test_dict = defaultdict(list)

    show_list = db.execute_sql('''
           select show_id from datalake.sample_shows_and_keywords;
    ''', return_list=True)  # returns list of episodes with selected experiment id

    for show in show_list:
        query = '''SELECT keyword_counts FROM datalake.sample_shows_and_keywords
                    WHERE  show_id = '{fshow}' 
                    '''.format(fshow=show)

        keyword_list = db.execute_sql(query, return_list=True)  # calls keyword lists for episode

        keywords_list = list(ast.literal_eval(keyword_list[0]).keys())  # loads list of keywords

        test_dict[show] = annotator(keywords_list, confidence=0.5, verbose=False, split_camel_case=True)  # annotates keyword list, generates yago types, and adds to dict

    print(dict(test_dict))
