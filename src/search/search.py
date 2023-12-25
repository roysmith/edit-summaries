#!/usr/bin/env python3

import argparse

from opensearchpy import OpenSearch
from pprint import pprint


host = 'opensearch:9200'
auth = ('admin', 'admin')


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--info',
                       action='store_true',
                       help='print sever info')
    group.add_argument('--dump',
                       action='store_true',
                       help='dump data')
    args = parser.parse_args()

    server = OpenSearch(hosts = [host],
                        use_ssl = False,
                        # http_auth = auth,
                        )

    if args.info:
        print(server.info())
    elif args.dump:
        index_name = 'test-index'
        query = {
            'query': {
                'match_all': {
                }
            }
        }
        response = server.search(body=query, index=index_name)
        pprint(response)

    
if __name__ == '__main__':
    main()
