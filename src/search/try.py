#!/usr/bin/env python3

from opensearchpy import OpenSearch

host = 'opensearch:9200'
auth = ('admin', 'admin')

def main():
    server = OpenSearch(hosts = [host],
                        use_ssl = False,
                        # http_auth = auth,
                        #verify_certs = True,
                        )
    print(server.info())

    
if __name__ == '__main__':
    main()
