#!/usr/bin/env python3

import bz2
from collections import namedtuple
from pathlib import Path

import pandas as pd

from schema import DATA_SCHEMA

DataRow = namedtuple('DataRow', DATA_SCHEMA)

def main():
    DUMP_DIR = "/public/dumps/public/other/mediawiki_history"
    DATE = "2023-11"
    WIKI = "enwiki"
    # for schema and how to interpret fields, see: 
    # https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Mediawiki_history_dumps#Technical_Documentation

    wiki_dir = Path(DUMP_DIR) / DATE / WIKI
    filenames = sorted(wiki_dir.glob('*.tsv.bz2'))
    filenames = filenames[-1:]
    for filename in filenames:
        for row in get_rows(filename):
            if row.event_entity == 'revision':
                summary = get_human_text(row.event_comment_escaped)
                if summary:
                    print(summary)
        break

def get_rows(file_name):
    with bz2.open(file_name, 'rt') as f:
        for line in f:
            yield DataRow(*line.split('\t'))

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
