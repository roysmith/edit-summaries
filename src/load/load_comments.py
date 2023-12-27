#!/usr/bin/env python3

"""
Extract edit comments and load into opensearch.

"""


import argparse
import bz2
from collections import namedtuple
from itertools import islice
import json
from pathlib import Path

from opensearchpy import OpenSearch

from load.schema import field_names, unescape_tnr, escape_tnr, build_row

EscapedRow = namedtuple("EscapedRow", field_names)


host = 'opensearch:9200'

def main():
    args = parse_command_line()
    client = OpenSearch(hosts=[host], use_ssl=False)
    index_name = 'test-index'

    with bz2.open(args.filename, 'rt') as fin:
        for document in islice(get_documents(fin), args.max_count):
            if not args.dry_run:
                response = client.index(
                    index=index_name,
                    body=document,
                    id=id,
                    refresh=True,
                )
            if args.verbose:
                print(f'{document=}')


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
    parser.add_argument('filename',
                        help='input file (must be .tsv.bz2)')
    parser.add_argument('--verbose', '-v',
                        default=True,
                        action=argparse.BooleanOptionalAction,
                        help='enable verbose logging')
    parser.add_argument('--max-count',
                        type=int,
                        help='maximum number of records to load')
    parser.add_argument('--dry-run',
                        default=True,
                        action=argparse.BooleanOptionalAction,
                        help='Process documents but do not insert into index')
    return parser.parse_args()


if __name__ == '__main__':
    main()
