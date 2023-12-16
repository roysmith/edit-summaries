#!/usr/bin/env python3

from elasticsearch import Elasticsearch


def main():
    es = Elasticsearch(['elasticsearch.svc.tools.eqiad1.wikimedia.cloud:80'])
    print(es.info())

    
if __name__ == '__main__':
    main()
