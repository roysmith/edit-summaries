#!/usr/bin/env python3

"""
Extract edit comments.

"""


import bz2
from pathlib import Path

import schema


DUMP_DIR = "/public/dumps/public/other/mediawiki_history"
DATE = "2023-11"
WIKI = "enwiki"


def main():
    wiki_dir = Path(DUMP_DIR) / DATE / WIKI
    filenames = sorted(wiki_dir.glob('*.tsv.bz2'))
    filenames = filenames[-1:]
    for filename in filenames:
        for row in get_rows(filename):
            if row.event_entity == 'revision' and row.event_comment and not row.event_user_is_bot_by:
                summary = get_human_text(row.event_comment)
                if summary:
                    print(summary)


def get_rows(file_name):
    with bz2.open(file_name, 'rt') as f:
        for line in f:
            yield schema.build_row(line)


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
