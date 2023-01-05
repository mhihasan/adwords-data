INDEX_MAPPING = {
    'settings': {
        'number_of_replicas': 1,
        'number_of_shards': 10,
        "index": {
            "sort.field": "volume",
            "sort.order": "desc"
        }
    },
    'mappings': {
        'dynamic': False,
        'properties': {
            'keyword': {'type': 'text'},
            'volume': {'type': 'long'},
            'cpc': {'type': 'float'},
            'competition': {'type': 'float'},
            'spell_type': {'type': 'keyword'},
            'history': {'type': 'object'},
            'categories': {'type': 'keyobjectword'},
        }
    }
}
