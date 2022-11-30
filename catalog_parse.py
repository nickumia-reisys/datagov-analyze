import requests
import nlp.processing.storage as nps
import threading
from threading import Lock
import sys

STORAGE = nps.LocalStorage()
STORAGE.save = {}
concurrent = 0
batch = 100
processed = 0
all_threads = []
blocked = 0
current = 0

def get_total_count():
    # Returns the total number of datasets on catalog.data.gov
    return requests.get('https://catalog.data.gov/api/action/package_search').json()['result']['count']

class Work(threading.Thread):
    def __init__(self, threadID, gnext, lfunc, fil):
        threading.Thread.__init__(self)
        self.name = threadID
        self.tid = threadID
        self.lfunc = lfunc
        self.gnext = gnext
        self.fil = fil
        self.lock = Lock()

    def run(self):
        global concurrent, processed, blocked
        print("Starting...", self.name)
        results = self.gnext(self.tid)
        if results is None:
            blocked += 1
            return
        self.lock.acquire()
        self.lfunc(self.tid, results)
        concurrent -= 1
        processed += 1
        self.lock.release()
        if processed % 100 == 0:
            STORAGE.backup(self.fil)
        print("Completed...", self.name)

def gather(get_next, l_work, fil):
    global current, concurrent, all_threads
    total = get_total_count()
    while current*batch < total:
        if blocked > 100:
            sys.exit()
        if concurrent < 5:
            t = Work(current, get_next, l_work, fil)
            all_threads.append(t)
            t.start()
            concurrent += 1
            current += 1
        for i in all_threads:
            i.join()

def gather_todo(todo, get_next, l_work, fil):
    global concurrent, all_threads
    while todo != []:
        print(processed)
        if blocked > 15:
            print('Blocked by Cloudfront')
            sys.exit(1)
        if concurrent < 20:
            a = todo.pop()
            if a not in STORAGE.save.keys():
                t = Work(a, get_next, l_work, fil)
                all_threads.append(t)
                t.start()
                concurrent += 1
        for i in all_threads:
            i.join()
