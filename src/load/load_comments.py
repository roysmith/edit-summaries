#!/usr/bin/env python3

"""
Extract edit comments and load into opensearch.

"""


import argparse
import bz2
from collections import namedtuple
from configparser import ConfigParser
from itertools import islice
import json
import os
from pathlib import Path
from pprint import pprint

import opensearchpy


from load.schema import field_names, unescape_tnr, escape_tnr, build_row

EscapedRow = namedtuple("EscapedRow", field_names)


def main():
    args = parse_command_line()
    config = read_config()
    auth = (config['auth']['user'], config['auth']['password'])
    client = opensearchpy.OpenSearch(hosts=[f'{args.host}:{args.port}'],
                                     http_auth=auth,
                                     use_ssl=True,
                                     verify_certs=False,
                                     ssl_show_warn=False,
                                     )
    index_name = args.index_name

    if args.unsafe_drop_index:
        print('dropping index')
        try:
            client.indices.delete(index=index_name)
        except opensearchpy.exceptions.NotFoundError:
            print('index not found, ignoring')

    if args.filename:
        try:
            indexer = BulkIndexer(client, index_name, args.batch_size)
            with bz2.open(args.filename, 'rt') as fin:
                for document in islice(get_documents(fin), args.max_count):
                    if not args.dry_run:
                        indexer.index(document)
                    if args.verbose:
                        print(f'{document=}')
                indexer.flush()
        except opensearchpy.exceptions.OpenSearchException as ex:
            pprint(ex.errors)

def get_documents(fin):
    """Iterate over documents which should be inserted into the index.

    """
    for line in fin:
        row = EscapedRow(*line.split('\t'))
        if row.event_entity != 'revision':
            continue
        if row.event_user_is_bot_by_string:
            continue
        comment = unescape_tnr(row.event_comment_escaped)
        if not comment:
            continue
        human_comment = get_human_text(comment)
        if not human_comment:
            continue

        yield {'id': row.revision_id,
               'ts': row.event_timestamp,
               'co': human_comment,
               'un': unescape_tnr(row.event_user_text_escaped),
               }
        

def get_human_text(text):
    if text == '*':
        return ''
    if text.startswith('[[WP:AES|\u2190]]'):
        return ''
    if text.startswith('/*') and '*/' in text:
        return text[text.find('*/') + 2:]
    return text


def parse_command_line():
    """Parse the command line.  Return the parsed args.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v',
                        default=False,
                        action=argparse.BooleanOptionalAction,
                        help='enable verbose logging')
    parser.add_argument('--host',
                        required=True,
                        help='hostname of opensearch server to connect to')
    parser.add_argument('--port',
                        type=int,
                        default=9200,
                        help='port number of opensearch server to connect to')
    parser.add_argument('--max-count',
                        type=int,
                        help='maximum number of records to load')
    parser.add_argument('--dry-run',
                        default=True,
                        action=argparse.BooleanOptionalAction,
                        help='Process documents but do not insert into index')
    parser.add_argument('--batch-size',
                        type=int,
                        default=1000,
                        help='Number of insertions per bulk operation')
    parser.add_argument('--unsafe-drop-index',
                       action='store_true',
                       help=
                        '''delete index (and all the data contained in it!).  If specified
                        with --filename, index is dropped first, then new data loaded.''')
    parser.add_argument('-f',
                       '--filename',
                       help='input file (must be .tsv.bz2)')
    parser.add_argument('--index-name',
                        default='edit-comment',
                        help='index name')
    return parser.parse_args()


def read_config():
    config_path = Path(os.environ['OPENSEARCH_CONFIG'])
    mode = config_path.stat().st_mode
    if mode & 0o44:
        raise RuntimeError(f'Config file ({config_path}) has unsafe mode {oct(mode)}: may not be group or world readable')

    config = ConfigParser()
    config.read(config_path)
    return config


class BulkIndexer:
    def __init__(self, client, index, batch_size):
        self.client = client
        self.index_name = index
        self.batch_size = batch_size
        self.actions = []
        self.doc_count = 0
        self.insert_count = 0


    def index(self, doc):
        self.actions.append({'_index': self.index_name,
                             '_source': doc,
                             })
        if len(self.actions) >= self.batch_size:
            self.flush()


    def flush(self):
        if self.actions:
            ok, _ = opensearchpy.helpers.bulk(self.client, self.actions)
            self.doc_count += len(self.actions)
            self.insert_count += ok
            self.actions = []
            print(f'{self.doc_count} docs, {self.insert_count} inserted')


if __name__ == '__main__':
    main()
