import requests
import os
import pickle
import time

data_path = "/Users/polybahn/Desktop/data/hetrec2011-movielens-2k-v2"

movie_path = os.path.join(data_path, 'movies.dat')
imdb2intri = dict()
imdb2name = dict()


def build_reverse_dict(dictionary):
    return dict([(v, k) for k, v in dictionary.items()])


# read in movie name, intrinsic_id, imdb_id
with open(movie_path, 'r', encoding='cp1252') as input_f:
    next(input_f)
    for line in input_f:
        intrinsic_id, movie_name, imdb_id = line.split('\t')[:3]

        imdb2name[imdb_id] = movie_name
        if imdb_id not in imdb2intri:
            imdb2intri[imdb_id] = list()
        imdb2intri[imdb_id].append(intrinsic_id)

name2imdb = build_reverse_dict(imdb2name)


def get_dbpedia_entity(query_class, query, max_hit):
    url = "http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=" + query_class \
          + "&QueryString=" + query \
          + "&MaxHits=" + str(max_hit)

    headers = {'Accept': 'application/json'}
    resp = requests.get(url=url, headers=headers)
    print(resp.status_code)
    if not resp.ok:
        return None
    data = resp.json()['results']
    if not data:
        return None
    entity = data[0]
    entity_uri = entity['uri']
    entity_label = entity['label']
    entity_description = entity['description']
    return [entity_uri, entity_label, entity_description]


def get_triplets(entity_uri):
    resp = requests.get(url="http://dbpedia.org/sparql/?default-graph-uri=http://dbpedia.org&query=DESCRIBE+<"
                          + entity_uri
                          +">&format=text/csv")
    if not resp.ok:
        return None
    triplets = resp.content.decode("utf-8").split('\n')[1:]
    triplets = [[i.strip('"') for i in triple.split(',')] for triple in triplets]
    filtered_triples = [triple for triple in triplets if triple[0] == entity_uri and triple[-1].startswith("http://dbpedia.org/")]
    return filtered_triples




abstract_dict = dict()
triplets_kg = list()
for imdb_id, name in imdb2name.items():
    if imdb_id in abstract_dict:
        continue
    entity_info = get_dbpedia_entity(query_class='film', query=name.strip(), max_hit=1)
    if not entity_info:
        continue
    abstract_dict[imdb_id] = entity_info
    # get triplets of this movie
    entity_uri = entity_info[0]
    print(entity_uri)
    triplets = get_triplets(entity_uri)
    print(triplets)
    time.sleep(0.1)
    if triplets:
        triplets_kg += triplets


# save data
def save(array, f_name):
    if not os.path.exists('data'):
        os.mkdir('data')
    with open('data/' + f_name, 'wb') as f:
        pickle.dump(array, f)


save(imdb2intri, 'imbd2intri.pkl')
save(imdb2name, 'imdb2name.pkl')
save(abstract_dict, 'abstract_imdb.pkl')
save(triplets_kg, 'movie_kg.pkl')


