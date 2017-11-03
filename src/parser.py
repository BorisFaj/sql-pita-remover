#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import nltk
import re


class Parser:
    def __init__(self, grammar):
        self.__grammar = self._read_grammar_(grammar)
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s %(asctime)s %(message)s')
        self.logger = logging.getLogger('hive_parser')

    @staticmethod
    def _read_grammar_(path):
        f = open(path, 'r')
        return nltk.CFG.fromstring(' '.join(f.readlines()))


    @staticmethod
    def __skip_to_node__(target, current):
        if type(target) is nltk.Tree:
            return target

        return current

    @staticmethod
    def __init_query__(d, i):
        d.setdefault(i, {})
        d[i].setdefault('columns', {'alias': {}, 'names': []})
        d[i].setdefault('tables', {'alias': {}, 'names': []})
        return d

    def parse_query(self, query):
        sent = query.replace(',', ' , ').replace('.', ' . ').replace('(', ' ( ').replace(')', ' ) ')
        sent = [chunk for chunk in re.sub(' +', ' ', sent).split(' ') if chunk]
        parser = nltk.ChartParser(self.__grammar)

        return parser.parse(sent)

    def get_table_name(self, tree, root, queries, i, skip_to=None):
        tables = queries[i]['tables']
        for node in tree:
            if type(node) is nltk.Tree:
                if node.label() not in ['TABLE_NAMES', root]:
                    _skip, _ = self.get_table_name(node, root, queries, i, skip_to)
                    skip_to = self.__skip_to_node__(skip_to, _skip)
                elif node.label() == root:
                    skip_to = self.__skip_to_node__(skip_to, tree)
                    _subqueries_num = tables.get('__subqueries__', 0) + 1
                    tables['__subqueries__'] = _subqueries_num
                    tables['alias'].setdefault(tree[-1].leaves()[1], {'subquery': i + _subqueries_num})
                elif tree.label() != 'TABLE_ALIAS':
                    tables['names'] += [''.join(node.leaves())]
                elif tree.label() == 'TABLE_ALIAS':
                    if tables['names']:  # si no es una subquery
                        tables['alias'].setdefault(node.leaves()[0], tables['names'][-1])

        return skip_to, tables

    def get_column_names(self, tree, columns):
        for node in tree:
            if type(node) is nltk.Tree:
                if node.label() != 'COLUMN_NAMES':
                    self.get_column_names(node, columns)
                elif tree.label() != 'COLUMN_ALIAS':
                    columns['names'] += [''.join(tree.leaves())]
                elif tree.label() == 'COLUMN_ALIAS':
                    columns['alias'].setdefault(node.leaves()[0], columns['names'][-1])

        return columns

    def get_nodes(self, parent, queries={'max': 0}, i=0, root='SELECT_SENTENCE'):
        queries.update(self.__init_query__(queries, i))
        name_nodes = []

        for node in parent:
            next_node = None
            if type(node) is nltk.Tree:
                if node.label() == root:
                    queries['max'] += 1
                    i = queries['max']
                elif node.label() == 'COLUMN_EXPRESSION':
                    name_nodes.append(node)
                    self.get_column_names(node, queries[i]['columns'])
                elif parent.label() == 'TABLE_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
                    name_nodes.append(node)
                    next_node, _ = self.get_table_name(node, root, queries, i)

                if not node.label() == 'COLUMN_EXPRESSION':
                    _nodes, _queries = self.get_nodes(self.__skip_to_node__(next_node, node), queries, i)
                    name_nodes += _nodes
                    queries.update(_queries)

        return name_nodes, queries
