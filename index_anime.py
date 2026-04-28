import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

es = Elasticsearch('http://localhost:9200')
df = pd.read_csv('data/anime.csv')
df = df.where(pd.notna(df), None)

timestamp = datetime.utcnow().isoformat()

def to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def gen():
    for _, row in df.iterrows():
        doc = row.to_dict()
        doc['@timestamp'] = timestamp
        doc['Score'] = to_float(doc.get('Score'))
        yield {'_index': 'anime', '_id': row.get('MAL_ID'), '_source': doc}

helpers.bulk(es, gen())
print('Done:', es.count(index='anime'))
print('hehe')