#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import nltk
import re
import os
import json


class Parser:
    def __init__(self, conf_path, conf_file):
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s %(asctime)s %(message)s')
        self.logger = logging.getLogger('hive_parser')
        self._config = self.load_json(os.path.join(conf_path, conf_file))
        self.__grammar = self._read_grammar_(os.path.join(conf_path, self._config['grammar_file']))
        self.__mapping = self._load_mapping_files(os.path.join(conf_path, self._config['mapping_dir']))
        self.__subqueries = []
        self.__reverse_tree = []

    @staticmethod
    def _read_grammar_(path):
        """Lee las reglas de produccion de la gramatica contenidas en el fichero indicado.

        Parameters
        ----------
        path: str
            Ruta al fichero que contiene las reglas de produccion de la gramatica.

        Returns
        -------
        nltk.grammar.CFG
            Objeto nltk que contiene la gramatica libre de contexto leida.
        """
        f = open(path, 'r')
        return nltk.CFG.fromstring(' '.join(f.readlines()))

    @staticmethod
    def __skip_to_node__(target, current):
        """Devuelve el nodo target si este no es null, en otro caso devuelve el current.

        Parameters
        ----------
        target: nltk.Tree
            Nodo evaluado.
        current: nltk.Tree
            Nodo actual.

        Returns
        -------
        nltk.Tree
            Devuelve el nodo target si este no es null, en otro caso devuelve el current.
        """
        if type(target) is nltk.Tree:
            return target

        return current

    @staticmethod
    def __init_query__(d, i):
        """Inicializa una query con el formato especifico utilizado en la clase.

        Parameters
        ----------
        d: dict
            Diccionario que se va a inicializar.
        i: int
            Indice actual.

        Returns
        -------
        dict
            Diccionario inicializado.
        """
        d.setdefault('max', 0)
        d.setdefault(i, {})
        d[i].setdefault('columns', {'alias': {}, 'names': []})
        d[i].setdefault('tables', {'alias': {}, 'names': []})
        return d

    def load_json(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError('No se encuentra el fichero especificado: {}'.format(path))

        js = open(path)
        return json.load(js)

    def _load_mapping_files(self, path):
        if not os.path.isdir(path):
            raise FileNotFoundError('No se encuentra la ruta especificada: {}'.format(path))

        return {f: self.load_json(os.path.join(path, f)) for f in os.listdir(path)}

    def parse_query(self, query):
        """Parsea una query en texto plano para transformarla en una sentencia de la gramatica.
            1. Se ponen espacio a cada lado de los signos de puntuacion, parentesis, etc.
            2. Si quedan varios espacios seguidos, se simplifican a un espacio.
            3. Se separan las palabras para convertirlas elementos de una lista.

        Parameters
        ----------
        query: str
            Query que se va a transformar.

        Returns
        -------
        generator(nltk.tree.Tree)
            Devuelve los arboles de sintaxis que representan la query parseada.
        """
        sent = query.replace(',', ' , ').replace('.', ' . ').replace('(', ' ( ').replace(')', ' ) ')
        sent = [chunk for chunk in re.sub(' +', ' ', sent).split(' ') if chunk]
        parser = nltk.ChartParser(self.__grammar)

        return parser.parse(sent)

    def __update_subqueries__(self, i):
        """Cuando se lee un nodo que representa una subquery, este se almacena en una cola a la espera de saber
        el indice de esa subquery. Cuando la siguiente subquery empieza a procesarse, coge el ultimo elemento de la
        cola y le asigna su indice.

        Parameters
        ----------
        i: int
            Indice de la subquery.
        """
        # cuando se procesa la query raiz de todas, no habra subqueries
        # tampoco cuando sea la segunda parte de un union
        if not len(self.__subqueries):
            self.logger.debug('No hay subqueries para asignar la numero {}'.format(i))
            return

        _node = self.__subqueries.pop()
        _node.update({'subquery': i})

    def get_table_name(self, tree, root, queries, i, skip_to=None):
        """Extrae los nombres de las tablas involucradas en este nivel de la query asi como sus alias a partir de un
        nodo TABLE_EXPRESSION. Las posibles subqueries no se procesan todavia, quedan a la espera de que la funcion
        que llama a get_table_name itere sobre ellas.

        Parameters
        ----------
        tree: nltk.Tree
            Nodo raiz.
        root: str
            Nombre del nodo que contiene una subquery.
        queries: dict
            Diccionario con la informacion de las queries procesadas hasta ahora.
        i: int
            Indice actual.
        skip_to: nltk.Tree
            Toma el valor del ultimo nodo procesado para que la funcion que llama a get_table_name pueda saltar
        directamente a ese nodo y no tenga que volver a procesarlo. Si el nodo contiene subqueries, se deuvelve
        la raiz de la primera subquery.

        Returns
        -------
        nltk.Tree
            Toma el valor del ultimo nodo procesado para que la funcion que llama a get_table_name pueda saltar
        directamente a ese nodo y no tenga que volver a procesarlo. Si el nodo contiene subqueries, se deuvelve
        la raiz de la primera subquery.
        """
        tables = queries[i]['tables']
        for node in tree:
            if type(node) is nltk.Tree:
                if node.label() not in ['TABLE_NAMES', root]:
                    _skip = self.get_table_name(node, root, queries, i, skip_to)
                    skip_to = self.__skip_to_node__(skip_to, _skip)
                elif node.label() == root:
                    # Se evalua si es el primer nodo root encontrado
                    skip_to = self.__skip_to_node__(skip_to, tree)
                    # Se extrae el alias (sin AS) y se actualiza el diccionario
                    _table_name = tree[-1].leaves()[1]
                    tables['alias'].setdefault(_table_name, {'subquery': 0})
                    # Se mete a la cola de subqueries
                    self.__subqueries.append(tables['alias'][_table_name])
                elif tree.label() != 'TABLE_ALIAS':
                    tables['names'] += [''.join(node.leaves())]
                    self.__reverse_tree.append((tree, i))
                elif tree.label() == 'TABLE_ALIAS':
                    if tables['names']:  # si no es una subquery
                        tables['alias'].setdefault(node.leaves()[0], tables['names'][-1])
                        self.__reverse_tree.append((tree, i))

        return skip_to

    def get_column_names(self, tree, columns):
        """Obtiene los nombres de todas las columnas involucradas en un nodo COLUMN_EXPRESSION, asi como sus alias.

        Parameters
        ----------
        tree: nltk.Tree
            Nodo raiz.
        columns: list(str)
            Lista de columnas.

        Returns
        -------
        list(str)
            Lista actualizada de columnas.
        """
        for node in tree:
            if type(node) is nltk.Tree:
                if node.label() != 'COLUMN_NAMES':
                    self.get_column_names(node, columns)
                elif tree.label() != 'COLUMN_ALIAS':
                    columns['names'] += [''.join(tree.leaves()).replace('DISTINCT', '')]
                elif tree.label() == 'COLUMN_ALIAS':
                    columns['alias'].setdefault(node.leaves()[0], columns['names'][-1])

        return columns

    def get_nodes(self, parent, queries, i=0, root='SELECT_SENTENCE'):
        queries.update(self.__init_query__(queries, i + 1))

        for node in parent:
            next_node = None
            if type(node) is nltk.Tree:
                if node.label() == root:
                    queries['max'] += 1
                    i = queries['max']
                    self.__update_subqueries__(i)
                elif node.label() == 'COLUMN_EXPRESSION':
                    self.__reverse_tree.append((node, i))
                    self.get_column_names(node, queries[i]['columns'])
                elif parent.label() == 'FROM_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
                    self.__reverse_tree.append((node, i))
                    self.get_column_names(node, queries[i]['columns'])
                elif parent.label() == 'TABLE_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
                    next_node = self.get_table_name(node, root, queries, i)

                if not node.label() == 'COLUMN_EXPRESSION':
                    self.get_nodes(self.__skip_to_node__(next_node, node), queries, i)

        return queries

    def get_reverse_tree(self):
        return self.__reverse_tree
