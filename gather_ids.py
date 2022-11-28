
import requests
from catalog_parse import get_total_count


fil = 'ids.pkl'
all_threads = []
rows = 100

def get_next(start):
    global rows
    # curl -sL
    # 'https://catalog.data.gov/api/action/package_search?rows=10&start=9'
    # | jq '.result.results[].id'
    # | jq '.result.results[].tags[].display_name'
    r = requests.get('https://catalog.data.gov/api/action/package_search?rows=%s&start=%s' % (rows, start)).json()
    if "The request could not be satisfied." in r.text:
        return None
    return r['result']['results']

def parse_dataset(result):
    r_id = result['id']
    r_tags = [i['display_name'] for i in result['tags']]

    return r_id, r_tags

def get_id(results_list):
    global STORAGE
    if results_list is not None:
        for a, i in enumerate(results_list):
            i_s, i_t = parse_dataset(i)
            if i_s not in STORAGE.save:
                STORAGE.save[i_s] = i_t

if __name__ == "__main__":
    total = get_total_count()
    try:
        STORAGE.restore(fil)
    except:
        pass

    while current*batch < total:
        if concurrent < 5:
            t = Work(current, get_next, get_id, fil)
            all_threads.append(t)
            t.start()
            concurrent += 1
            current += 1
        for i in all_threads:
            i.join()

    STORAGE.backup(fil)
