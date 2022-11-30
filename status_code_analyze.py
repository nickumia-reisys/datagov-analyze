
import requests
import nlp.processing.storage as nps
from catalog_500 import check_solr

REFERENCE = nps.LocalStorage()
REFERENCE.restore('test.pkl')
API = nps.LocalStorage()
API.restore('api.pkl')
del API.save[None]

status_code = {}

for k,v in REFERENCE.save.items():
    if v in status_code.keys():
        status_code[v] += 1
    else:
        status_code[v] = 1
    # if v == 404:
    #     i = check_solr(k)
    #     print(k, i)
    #     if i in API.save.keys():
    #         API.save[i].append(k)
    #     else:
    #         API.save[i] = [k]

API.backup('api.pkl')
print(status_code)
print(API.save.keys())
