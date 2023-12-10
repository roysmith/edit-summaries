#!/usr/bin/env python3

from argparse import ArgumentParser
import bz2
from configparser import ConfigParser
import glob
import logging
from xml.dom import pulldom


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = ArgumentParser()
    parser.add_argument('--config', '-c', default='config.ini',
                        help='config file (default %(default)s)')
    parser.add_argument('--file', '-f',
                        help='input file (over-rides config')
    args = parser.parse_args()

    config = ConfigParser()
    config.read(args.config)

    if args.file:
        logging.info('Processing command-line file "%s"', args.file)
        process_path(args.file)
    else:
        pattern = config.get('dumps', 'path_glob')
        logging.info('Processing directory "%s"', pattern)
        for path in glob.iglob(pattern):
            process_path(path)


def process_path(path):
    logging.info('Processing file "%s"', path)
    if path.endswith('.bz2'):
        with bz2.open(path) as stream:
            process_stream(stream)
    else:
        with open(path) as stream:
            process_stream(stream)


def process_stream(stream):
    doc = pulldom.parse(stream)
    for event, node in doc:
        if event == pulldom.START_ELEMENT and node.tagName == 'page':
            doc.expandNode(node)

            for revision in node.getElementsByTagName('revision'):
                id = revision.getElementsByTagName('id')[0].childNodes[0].nodeValue

                contributor = revision.getElementsByTagName('contributor')[0]
                usernames = contributor.getElementsByTagName('username')
                if usernames:
                    user_node = usernames[0]
                else:
                    user_node = contributor.getElementsByTagName('ip')[0]
                user = user_node.childNodes[0].nodeValue
                    
                comment_nodes = revision.getElementsByTagName('comment')
                if comment_nodes:
                    comment = comment_nodes[0].childNodes[0].nodeValue
                else:
                    comment = ''

                logging.debug('%s: [%s], "%s"', id, user, comment)







            # title = node.getElementsByTagName('title')[0].childNodes[0].nodeValue
            # if not ns == '0':
            #     continue

            # cdataNodes = node.getElementsByTagName('text')[0].childNodes
            # content = ' '.join(node.nodeValue for node in cdataNodes).lower()

    
if __name__ == '__main__':
    main()
