#!/usr/bin/env python3

"""
Extract edit comments.

"""


import argparse
import bz2
from collections import namedtuple
import json
from pathlib import Path

from load.schema import field_names, unescape_tnr, escape_tnr, build_row

EscapedRow = namedtuple("EscapedRow", field_names)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='input file (must be in tsv-bz2 format)')
    parser.add_argument('--tuple',
                        default=True,
                        action=argparse.BooleanOptionalAction,
                        help='Use the high-speed NamedTuple parsing strategy')
    args = parser.parse_args()

    if args.tuple:
        process_as_tuples(args.filename)
    else:
        process_as_rows(args.filename)


def process_as_tuples(filename):
    for row in get_tuples(filename):
        if row.event_entity == 'revision':
            comment = unescape_tnr(row.event_comment_escaped)
            if comment and not row.event_user_is_bot_by_string:
                human_comment = get_human_text(comment)
                if human_comment:
                    data = {'timestamp': row.event_timestamp,
                            'comment': human_comment,
                            'username': unescape_tnr(row.event_user_text_escaped),
                            }
                    print(json.dumps(data))


def get_tuples(filename):
    with bz2.open(filename, 'rt') as f:
        for line in f:
            yield EscapedRow(*line.split('\t'))


def process_as_rows(filename):
    for row in get_rows(filename):
            if row.event_entity == 'revision' and row.event_comment and not row.event_user_is_bot_by:
                summary = get_human_text(row.event_comment)
                if summary:
                    print(escape_tnr(summary))


def get_rows(filename):
    with bz2.open(filename, 'rt') as f:
        for line in f:
            yield build_row(line)


def get_human_text(text):
    if text == '*':
        return ''
    if text.startswith('[[WP:AES|\u2190]]'):
        return ''
    if text.startswith('/*') and '*/' in text:
        return text[text.find('*/') + 2:]
    return text


if __name__ == '__main__':
    main()
