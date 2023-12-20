#!/usr/bin/env python3

from opensearchpy import OpenSearch

host = 'opensearch:9200'
auth = ('admin', 'admin')

def main():
    server = OpenSearch(hosts = [host],
                        http_auth = auth,
                        use_ssl = False,
                        #verify_certs = True,
                        )
    print(server.info())

    
if __name__ == '__main__':
    main()
