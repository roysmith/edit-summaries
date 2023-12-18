#!/usr/bin/env python3

from opensearchpy import OpenSearch


def main():
    es = OpenSearch(['http://localhost:9200'])
    print(es.info())

    
if __name__ == '__main__':
    main()
