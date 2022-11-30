
import requests
import nlp.processing.storage as nps
from catalog_parse import gather_todo, STORAGE

sta = 'status.pkl'
REFERENCE = nps.LocalStorage()
REFERENCE.restore('keywords.pkl')


def get_next(did):
    # curl -sL
    # 'https://catalog.data.gov/api/action/package_search?rows=10&start=9'
    # | jq '.result.results[].id'
    # | jq '.result.results[].tags[].display_name'
    r = requests.get('https://catalog.data.gov/dataset/%s' % (did))
    if "The request could not be satisfied." in r.text:
        return None
    return r.status_code

def save_status(tid, scode):
    global STORAGE
    if scode is not None:
        STORAGE.save[tid] = scode

def check_solr(start):
    r = requests.get('https://catalog.data.gov/api/action/package_show?id=%s' % (start))
    if "The request could not be satisfied." in r.text:
        return None
    s = r.status_code
    return s


if __name__ == "__main__":
    try:
        STORAGE.restore(sta)
    except:
        pass

    todo = list(REFERENCE.save.keys())
    gather_todo(todo, get_next, save_status, sta)

    STATUS.backup(sta)
