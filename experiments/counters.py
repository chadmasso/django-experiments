from django.conf import settings
from pymongo import Connection

MONGO_HOST = getattr(settings, 'EXPERIMENTS_MONGO_HOST', 'localhost')
MONGO_PORT = getattr(settings, 'EXPERIMENTS_MONGO_PORT', 27017)
MONGO_EXPERIMENTS_DB = getattr(settings, 'EXPERIMENTS_MONGO_DB', 'experiments')

MONGO_URI = Connection(MONGO_HOST, MONGO_PORT)
db = MONGO_URI[MONGO_EXPERIMENTS_DB]

COUNTER_CACHE_KEY = 'experiments:participants:%s'
COUNTER_FREQ_CACHE_KEY = 'experiments:freq:%s'

def increment(key, participant_identifier):
    try:
        cache_key = COUNTER_CACHE_KEY % key
        freq_cache_key = COUNTER_FREQ_CACHE_KEY % key
        new_value = r.hincrby(cache_key, participant_identifier, 1)

        new_doc = db.counts.find_and_modify({'_id': cache_key}, {"$inc" : {participant_identifier: 1 }}, upsert = True, new = True)
        new_value = new_doc.get(participant_identifier)
        # Maintain histogram of per-user counts
        if new_value > 1:
            ident = str(new_value - 1)
            db.counts.update({'_id': freq_cache_key}, {"$inc" : {ident: -1}}, upsert = True)
        db.counts.update({'_id': freq_cache_key}, {"$inc" : {new_value: 1}}, upsert = True)
    except Exception:
        # Handle failures gracefully
        pass

def get(key):
    try:
        cache_key = COUNTER_CACHE_KEY % key
        return (len(db.counts.find_one({'_id': cache_key}).keys()) - 1)
    except Exception:
        # Handle failures gracefully
        return 0


def get_frequencies(key):
    try:
        freq_cache_key = COUNTER_FREQ_CACHE_KEY % key
        # In some cases when there are concurrent updates going on, there can
        # briefly be a negative result for some frequency count. We discard these
        # as they shouldn't really affect the result, and they are about to become
        # zero anyway.
        return dict((int(k),int(v)) for (k,v) in db.counts.find_one({'_id': freq_cache_key}).items() if int(v) > 0)

    except Exception:
        # Handle Redis failures gracefully
        return tuple()

def reset(key):
    try:
        cache_key = COUNTER_CACHE_KEY % key
        db.counts.remove({'_id': cache_key})
        freq_cache_key = COUNTER_FREQ_CACHE_KEY % key
        db.counts.remove({'_id': freq_cache_key})
        return True
    except Exception:
        # Handle Redis failures gracefully
        return False

def reset_pattern(key):
    #similar to above, but can pass pattern as arg instead
    try:
        cache_key = COUNTER_CACHE_KEY % key
        db.counts.remove({'_id': {'$regex': cache_key})
        freq_cache_key = COUNTER_FREQ_CACHE_KEY % key
        db.counts.remove({'_id': {'$regex': freq_cache_key})
        return True
    except Exception:
        # Handle Redis failures gracefully
        return False
