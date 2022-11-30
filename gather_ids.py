
import requests
from catalog_parse import gather, STORAGE

fil = 'ids.pkl'
rows = 100

def get_next(start):
    global rows
    # curl -sL
    # 'https://catalog.data.gov/api/action/package_search?rows=10&start=9'
    # | jq '.result.results[].id'
    # | jq '.result.results[].tags[].display_name'
    r = requests.get('https://catalog.data.gov/api/action/package_search?rows=%s&start=%s' % (rows, start))
    if "The request could not be satisfied." in r.text:
        return None
    return r.json()['result']['results']

def parse_dataset(result):
    r_id = result['id']
    r_tags = [i['display_name'] for i in result['tags']]
    r_org = result['organization']['id']

    return r_id, r_tags, r_org

def get_id(tid, results_list):
    global STORAGE
    if results_list is not None:
        for a, i in enumerate(results_list):
            i_s, i_t, i_o = parse_dataset(i)
            if i_s not in STORAGE.save:
                STORAGE.save[i_s] = {
                    'tags': i_t,
                    'org': i_o
                }


if __name__ == "__main__":
    try:
        STORAGE.restore(fil)
    except:
        pass

    gather(get_next, get_id, fil)

    STORAGE.backup(fil)
