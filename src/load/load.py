#!/usr/bin/env python3

"""
Load JSON records into opensearch.

"""


import argparse
import json

from opensearchpy import OpenSearch


host = 'opensearch:9200'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='input file (must be one JSON object per line')
    args = parser.parse_args()

    client = OpenSearch(hosts=[host],
                        use_ssl = False,
                        )
    index_name = 'test-index'
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1,
                }
            }
        }
    # response = client.indices.create(index_name, body=index_body)

    with open(args.filename, 'r') as fin:
        id = 1
        for line in fin:
            document = json.loads(line.strip())
            response = client.index(
                index=index_name,
                body=document,
                id=id,
                refresh=True,
                )
            print(response)
            id += 1
                


if __name__ == '__main__':
    main()
