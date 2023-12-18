#!/usr/bin/env python3

from opensearchpy import OpenSearch


def main():
    es = OpenSearch(['http://elasticsearch.svc.tools.eqiad1.wikimedia.cloud:80'])
    print(es.info())

    
if __name__ == '__main__':
    main()
