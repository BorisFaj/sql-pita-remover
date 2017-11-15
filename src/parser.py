#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import nltk
import re
import os
import json
import sqlparse
from operator import itemgetter
from copy import copy


class UnreferencedTableError(Exception):
    def __init__(self, tables):
        super(UnreferencedTableError, self).__init__('Se ha solicitado el nombre de una tabla no referenciada '
                                                     'explicitamente en la query pero la query hace referencia a mas '
                                                     'de una tabla: {}'.format(tables))


class Parser:
    def __init__(self, conf_path, conf_file, log_level=logging.INFO):
        logging.basicConfig(level=log_level, format='%(levelname)s %(name)s %(asctime)s %(message)s')
        self.logger = logging.getLogger('hive_parser')
        self._config = self.load_json(os.path.join(conf_path, conf_file))
        self._conf_path = conf_path
        self.__mapping = self._load_mapping_files(os.path.join(conf_path, self._config['mapping_dir']))
        #self.__mapping = self.__init_debug_mapping()  # ToDo: utilizar los ficheros de mapping en lugar de esto
        self.__queries_elements = []
        self.__reverse_tree = []
        self.__queries = {}
        self.__comments = []
        self.__words = []
        self._terminals = None
        self._queries = self.clean_queries(self.load_queries())
        self.__grammar = self._read_grammar_(os.path.join(conf_path, self._config['grammar_file']))

    def get_grammar(self):
        return self.__grammar

    @staticmethod
    def read_query(query):
        with open(query, 'r') as file:
            return [line.replace('\n', ' ').replace('\t', ' ') for line in file.readlines()]

    def load_queries(self):
        return (self.read_query(os.path.join(self._config['input_path'], query))
                for query in os.listdir(self._config['input_path']))

    @staticmethod
    def str_to__terminal(s):
        if '.' in s:
            return ["'" + t + "'" for t in s.split('.')]
        else:
            s = "'" + s + "'"
            return s, s

    def _read_grammar_(self, path, new_terminals=None):
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
        grammar_file = ' '.join(f.readlines())
        self._terminals = [t.upper().strip().replace("'", "") for t in self.find_between(grammar_file, "'", "'")]
        if new_terminals:
            new_tables, new_columns = zip(*[self.str_to__terminal(t) for t in new_terminals])
            tables = '\nTABLE_NAMES -> ' + '|'.join(new_tables)
            columns = '\nCOLUMN_NAMES ->' + '|'.join(new_columns)

            return nltk.CFG.fromstring(grammar_file + tables + columns)

        else:
            return nltk.CFG.fromstring(grammar_file)

    @staticmethod
    def __init_debug_mapping():
        """Me creo estos mappings durante el desarrollo para las pruebas."""
        t3_mapping = {
            "old_name": "T3",
            "new_name": "nueva_t3",
            "fields": {
                "A": "nuevo_a_t3",
                "B": "nuevo_b_t3"
            }
        }

        t2_mapping = {
            "old_name": "T2",
            "new_name": "nueva_t2",
            "fields": {
                "A": "nuevo_a_t2",
                "B": "nuevo_b_t2",
                "C": "nuevo_c_t2",
                "P": "nuevo_p_t2",
                "WHERE_COLUMN": "nueva_where"
            }
        }

        t1_mapping = {
            "old_name": "T1",
            "new_name": "nueva_t1",
            "fields": {
                "A": "nuevo_a_t1",
                "B": "nuevo_b_t1",
                "C": "nuevo_c_t1",
                "D": "nuevo_d_t1",
                "E": "nuevo_e_t1",
                "P": "nuevo_p_t1",
                "WHERE_COLUMN": "nueva_where"
            }
        }

        t4_mapping = {
            "old_name": "T4",
            "new_name": "nueva_t4",
            "fields": {
                "A": "nuevo_a_t4",
                "B": "nuevo_b_t4",
                "C": "nuevo_c_t4",
                "D": "nuevo_d_t4",
                "E": "nuevo_e_t4",
                "F": "nuevo_f_t4",
                "G": "nuevo_g_t4"
            }
        }

        t5_mapping = {
            "old_name": "T5",
            "new_name": "nueva_t5",
            "fields": {
                "A": "nuevo_a_t5",
                "B": "nuevo_b_t5",
                "C": "nuevo_c_t5",
                "D": "nuevo_d_t5",
                "E": "nuevo_e_t5",
                "F": "nuevo_f_t5",
                "G": "nuevo_g_t5"
            }
        }

        t6_mapping = {
            "old_name": "T6",
            "new_name": "nueva_t6",
            "fields": {
                "A": "nuevo_a_t6",
                "B": "nuevo_b_t6"
            }
        }

        mapping = {'T1': t1_mapping, 'T2': t2_mapping, 'T3': t3_mapping, 'T5': t5_mapping,
                   'T6': t6_mapping, 'T4': t4_mapping}
        mapping['tables'] = {mapping[table]['old_name']: mapping[table]['new_name'] for table in mapping.keys()}
        return mapping

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

    def _init_query(self, i):
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
        self.__queries.setdefault('max', 0)
        self.__queries.setdefault(i, {})
        self.__queries[i].setdefault('columns', {'alias': {}, 'names': []})
        self.__queries[i].setdefault('tables', {'alias': {}, 'names': []})
        return self.__queries

    def load_json(self, path):
        """Cargar un fichero json en un diccionario.

        Parameters
        ----------
        path: str
            Ruta al fichero json.

        Returns
        -------
        dict
            Diccionario que representa el json especificado.
        """
        if not os.path.exists(path):
            raise FileNotFoundError('No se encuentra el fichero especificado: {}'.format(path))

        js = open(path)
        return json.load(js)

    def _load_mapping_files(self, path):
        """Carga los ficheros de mapping contenidos en el directorio especificado en un diccionario.

        Parameters
        ----------
        path: str
            Directorio que contiene los json de mapping.

        Returns
        -------
        dict
            Diccionario que representa todos los ficheros de mapping.
        """
        if not os.path.isdir(path):
            raise FileNotFoundError('No se encuentra la ruta especificada: {}'.format(path))

        return {f.replace('.json', ''): self.load_json(os.path.join(path, f)) for f in os.listdir(path)}

    def parse_query(self, query, trace=0):
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
        sent = [chunk.upper() for chunk in re.sub(' +', ' ', sent).split(' ') if chunk]
        new_terminals = set(filter(lambda x: x not in self._terminals, sent))
        self.__grammar = self._read_grammar_(os.path.join(self._conf_path, self._config['grammar_file']), new_terminals)
        parser = nltk.ChartParser(self.__grammar, trace=trace)

        return parser.parse(sent)

    def _update_subqueries(self, i):
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
        if not len(self.__queries_elements):
            self.logger.debug('No hay subqueries para asignar la numero {}'.format(i))
            return

        _node = self.__queries_elements.pop()
        _node.update({'subquery': i})

    def _process_table_name(self, parent, node, root, i, skip_to):
        """Extrae los nombres de las tablas involucradas en este nivel de la query asi como sus alias a partir de un
        nodo TABLE_EXPRESSION. Las posibles subqueries no se procesan todavia, quedan a la espera de que se itere sobre
        ellas.

        Parameters
        ----------
        parent: nltk.Tree
            Nodo progenitor del que se procesa.
        node: nltk.Tree
            Nodo que se procesa.
        root: str
            Etiqueta del nodo que contiene una subquery.
        i: int
            Indice de la query actual.
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
        tables = self.__queries[i]['tables']
        if node.label() not in ['TABLE_NAMES', root]:
            # Si no es un nodo de tabla, sigue iterando
            _skip = self.iter_table_node(node, root, i, skip_to)
            skip_to = self.__skip_to_node__(skip_to, _skip)
        elif node.label() == root:
            # Si es el primer nodo root encontrado
            skip_to = self.__skip_to_node__(skip_to, parent)
            # Se extrae el alias (sin AS) y se actualiza el diccionario
            _alias_node = parent[-1].leaves()
            _table_name = _alias_node[0] if len(_alias_node) == 1 else _alias_node[1]  # Si no lleva 'AS' guarda el primero
            tables['alias'].setdefault(_table_name, {'subquery': 0})
            # Se mete a la cola de subqueries
            self.__queries_elements.append(tables['alias'][_table_name])
        elif parent.label() != 'TABLE_ALIAS':
            # Si es una referencia directa a una tabla
            _table_name = ''.join(node.leaves())
            if _table_name not in tables['names']:
                tables['names'] += [_table_name]
            self.__reverse_tree.append((parent, i))
        elif parent.label() == 'TABLE_ALIAS' and tables['names']:
            # Si es una referencia a un alias y este no es de una subquery
            tables['alias'].setdefault(node.leaves()[0], tables['names'][-1])
            self.logger.debug('Se mete nodo alias: {}'.format(parent))
            self.__reverse_tree.append((parent, i))

        return skip_to

    def _merge_schema(self, node):
        """Si el nodo es una referencia a una tabla y esta lleva referencia a su esquema, se fusiona el nombre de la
        tabla y el esquema en un mismo nodo.
        Esto se hace porque la gramatica no contempla la referencia al esquema para proporcionar mayor flexibilidad
        en el preprocesamiento. De esta manera, en la query se admite cualquier nombre en tablas y columnas. Si
        tuviera que diferenciar el esquema, tendria tambien que diferenciar en el preprocesamiento cuando se trata
        de una clausula FROM.

        Parameters
        ----------
        node: nltk.Tree
            Nodo que se comprueba.
        """
        if node.label() != 'TABLE_REFERENCE':
            return False

        if len(node) < 3:
            return False

        if node[0].label() == 'TABLE_NAMES' and node[2].label() == 'TABLE_NAMES':
            _schema = node[0].leaves()
            table = copy(node[2])
            node[0][0] = '.'.join(_schema + table.leaves())
            node.remove(node[1])  # el punto
            node.remove(table)
            return True

        return False

    def iter_table_node(self, tree, root, i, skip_to=None):
        """Itera sobre los nodos de una referencia a tabla y los va procesando.

        Parameters
        ----------
        tree: nltk.Tree
            Nodo raiz.
        root: str
            Etiqueta del nodo que contiene una subquery.
        i: int
            Indice de la query actual.
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
        self._merge_schema(tree)
        skip_to = [self._process_table_name(tree, node, root, i, skip_to) for node in self.get_subtrees(tree)]

        return skip_to[0] if skip_to else None

    def _process_column_node(self, parent, node, columns):
        """Extrae los nombres de las columnas involucradas en los nodos que se procesan.

        Parameters
        ----------
        parent: nltk.Tree
            Nodo progenitor del que se procesa.
        node: nltk.Tree
            Nodo que se procesa.
        columns: list(str)
            Lista de columnas extraidas hasta ahora.

        Returns
        -------
        list(str)
            Lista de columnas extraidas hasta ahora.
        """
        if node.label() != 'COLUMN_NAMES':
            # Si no es un nodo de columna, sigue iterando
            self.iter_column_node(node, columns)
        elif parent.label() != 'COLUMN_ALIAS':
            # Si es una referencia directa a una columna
            columns['names'] += [''.join(parent.leaves()).replace('DISTINCT', '')]
        elif parent.label() == 'COLUMN_ALIAS':
            # Si es un alias
            columns['alias'].setdefault(node.leaves()[0], columns['names'][-1])

        return columns

    def iter_column_node(self, tree, columns):
        """Itera sobre los nodos que hacen referencia a columnas y los va procesando.

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
        columns = [self._process_column_node(tree, node, columns) for node in self.get_subtrees(tree)]

        return columns

    def rename_non_select(self, node):
        """Renombra las referencias a tablas en nodos distintos a select. Como no tienen dependencias de
        subqueries ni alias, se pueden renombrar directamente.

        Parameters
        ----------
        node: nltk.Tree
            Nodo que se va a renombrar.
        """
        table_node = [child for child in self.get_subtrees(node) if child.label() == 'TABLE_REFERENCE'][0]
        self._merge_schema(table_node)
        table_name = table_node[0][0]
        if table_name in self.__mapping:
            table_node[0][0] = self.__mapping[table_name]['new_name']

    def _process_node(self, node, parent, root, i):
        """Realiza el procesamiento de un nodo. Extrae los nombres de columnas y tablas y sus alias correspondientes.
        Si un alias hace referencia a un subquery, se almacena el indice de la subquery. Al mismo tiempo se van
        insertando los nodos procesados que contienen nombres de tablas o columnas en una lista (self.__reverse_tree).

        Parameters
        ----------
        node: nltk.Tree
            Nodo a procesar.
        parent: nltl.Tree
            Nodo progenitor del que se esta procesando.
        root: str
            Etiqueta del nodo raiz de una consulta.
        i: int
            Indice de la query procesada.
        """
        next_node = None
        if node.label() == root:
            # Si es el nodo raiz, actualiza indices de queries y subqueries
            self.__queries['max'] += 1
            i = self.__queries['max']
            self._update_subqueries(i)
        elif node.label() == 'COLUMN_EXPRESSION':
            # Si es un nodo columna, se mete a la lista de procesados y se explora el trozo de query
            self.__reverse_tree.append((node, i))
            self.iter_column_node(node, self.__queries[i]['columns'])
        elif parent.label() == 'FROM_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
            # Si es un nodo from, se mete a la lista de procesados y se explora el trozo de query
            self.__reverse_tree.append((node, i))
            self.iter_column_node(node, self.__queries[i]['columns'])
        elif parent.label() == 'TABLE_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
            # Si es un nodo tabla, se explora el trozo de query y se obtiene el nodo de la primera subquery que
            # contiene, en caso de que contenga alguna
            next_node = self.iter_table_node(node, root, i)
        elif node.label() in ['INSERT_EXPRESSION', 'CREATE_EXPRESSION']:
            # Si es la parte del insert o create table, se renombra directamente
            self.rename_non_select(node)

        if not node.label() == 'COLUMN_EXPRESSION':
            # Si el nodo es de columnas, ya esta procesado y no es necesario profundizar
            self.process_tree(self.__skip_to_node__(next_node, node), i, root)

    def process_tree(self, tree, i=0, root='SELECT_SENTENCE'):
        """Recorre el AST y va actualizando el diccionario de queries (self.__queries).

        Parameters
        ----------
        tree: nltk.Tree
            Nodo raiz sobre el que se itera.
        i: int
            Indice de la query procesada.
        root: str
            Etiqueta del nodo raiz de una consulta select.
        """
        self.__queries.update(self._init_query(i + 1))
        [self._process_node(node, tree, root, i) for node in self.get_subtrees(tree)]

    def get_reverse_tree(self):
        """Devuelve la lista de nodos extraidos en la funcion get_nodes."""
        if not self.__reverse_tree:
            raise AssertionError('La query todavia no ha sido procesada. Por favor, ejecuta la funcion process_tree()'
                                 'antes de volver a intentarlo.')

        return sorted(self.__reverse_tree, key=itemgetter(1), reverse=True)

    def get_queries(self):
        """Devuelve el diccionario de queries extraido en la funcion get_nodes."""
        return self.__queries

    def is_table_alias(self, name, i):
        """Comprueba que el nombre de la tabla proporcionada es un alias.

        Parameters
        ----------
        name: str
            Nombre de la tabla.
        i: int
            Indice de la query en la que se le hace referencia.

        Returns
        -------
        Boolean
            True si es un alias, False en caso contrario.
        """
        return self.__queries[i]['tables']['alias'].get(name, False)

    def is_subquery(self, name, i):
        """Comprueba que el nombre de la tabla proporcionada es una subquery.

        Parameters
        ----------
        name: str
            Nombre de la tabla.
        i: int
            Indice de la query en la que se le hace referencia.

        Returns
        -------
        Boolean
            True si es una subquery, False en caso contrario.
        """
        return 'subquery' in self.__queries[i]['tables']['alias'].get(name, {})

    @staticmethod
    def get_subtrees(tree):
        """Obtiene los subarboles contenidos en un arbol, ignorando las hojas.

        Parameters
        ----------
        tree: nltk.Tree
            Arbol a consultar.

        Returns
        -------
        filter
            Lista filtrada de subarboles.
        """
        return filter(lambda child: isinstance(child, nltk.Tree), tree)

    @staticmethod
    def equal_columns(a, b):
        """Comprueba que las columnas a y b son la misma, independientemente de su tabla.

        Parameters
        ----------
        a: str
            Nombre de la columna a.
        b: str
            Nombre de la columna b.

        Returns
        -------
        Boolean
            True si las columnas son iguales aun que pertenezcan a distintas tablas, False en caso contrario.
        """
        return str(a).split('.')[-1].upper() == str(b).split('.')[-1].upper()

    def get_unreferenced_table(self, i):
        """Obtiene la tabla en una query en la que las columnas no llevan referencias a tablas. Solo es posible
        deducirlo si la query hace referencia a una sola tabla.

        Parameters
        ----------
        i: int
            Indice de la query actual.

        Returns
        -------
        str
            Nombre de la tabla referenciada en la query indicada.
        """
        tables = self.__queries[i]['tables']['names']
        if len(tables) > 1:
            raise UnreferencedTableError(tables)

        return tables[0]

    @staticmethod
    def is_referenced_column(column):
        """Comprueba si la columna indicada lleva alguna referencia a tabla.

        Parameters
        ----------
        column: str
            Nombre de columna tal y como aparece en la query.

        Returns
        -------
        boolean
        """
        return len(column.split('.')) > 1

    def is_column_alias(self, column, i):
        """Comprueba si la columna indicada hace referencia a un alias.

        Parameters
        ----------
        column: str
            Nombre a comprobar.
        i: int
            Indice de la query actual.

        Returns
        -------
        boolean
        """
        return column in self.__queries[i]['columns']['alias']

    def get_referenced_names(self, names, i):
        """Obtiene, a partir de una referencia a columna, la tabla y la columna. Si no lleva referencia explicita a
        la tabla, la query solo puede consultar una tabla, en otro caso hay un error semantico y no se puede saber a
        quien se hace referencia.

        Parameters
        ----------
        names: str
            Referencia a la tabla.
        i: int
            Indice de la query procesada.

        Returns
        -------
        (str, str)
            Nombre de tabla, nombre de columna.
        """
        if self.is_referenced_column(names):
            return names.split('.')
        else:
            return self.get_unreferenced_table(i), names

    def get_reference_in_subquery(self, current_column, target_i):
        """Encuentra el nombre que tiene una columna dentro de una subquery determinada.

        Parameters
        ----------
        current_table: str
            Nombre de la tabla a la que hace referencia actualmente.
        current_column: str
            Nombre actual de la columna.
        target_i: int
            Indice de la subquery a la que se hace referencia.

        Returns
        -------
        (str, str)
            Nombre de la subtabla, nombre de la subcolumna.
        """
        if self.is_column_alias(current_column, target_i):
            # Si se trata de un alias, no lleva referencia de columna
            return '', current_column

        try:
            # Busca coincidencias en la subquery con el nombre de la columna actual
            new_reference = [e for e in self.__queries[target_i]['columns']['names'] if
                             self.equal_columns(current_column, e)]
            return self.get_referenced_names(new_reference[0], target_i)
        except IndexError:
            # La columna puede no estar en la subquery, pero pertenecer a la subtabla
            pass

        # Si no es un alias y va sin referencia a tabla, esta haciendo referencia a una columna de la subquery que no
        # aparece en la consulta pero que deberia existir. Esto solo se permite si la subquery consulta una sola tabla
        table = self.get_unreferenced_table(target_i)

        try:
            return table, self.__mapping[table]['fields'][current_column]
        except KeyError as err:
            self.logger.warning("{}. La referencia a la columna '{}' de la tabla '{}' no se encuentra en los ficheros "
                                "de mapping proporcionados. Se devuelve el nombre original".format(err, current_column, table))
            return table, current_column

    def find_sub_column(self, current_table, current_column, i):
        """Dada una columna que hace referencia a una subquery, devuelve el nombre de la columna dentro de la subquery.

        Parameters
        ----------
        current_table: str
            Nombre de la tabla a la que hace referencia actualmente.
        current_column: str
            Nombre actual de la columna.
        i: int
            Indice de la query que le hace referencia.

        Returns
        -------
        str
            Nombre de la subcolumna.
        """
        child_index = self.__queries[i]['tables']['alias'][current_table]['subquery']
        new_table, new_column = self.get_reference_in_subquery(current_column, child_index)

        if not new_table:  # era un alias
            return new_column

        return self.change_column_name(new_table, new_column, child_index)[1]

    def change_column_name(self, table_name, column_name, i):
        """Cambia el nombre de una columna por el proporcionado en los ficheros de mapping.

        Parameters
        ----------
        table_name: str
            Nombre de la tabla a la que hace referencia actualmente.
        column_name: str
            Nombre actual de la columna.
        i: int
            Indice de la query que le hace referencia.

        Returns
        -------
        str, str
            Nombres nuevos de la tabla y la columna.
        """
        if not self.is_subquery(table_name, i) and self.is_table_alias(table_name, i):
            # Si es un alias que no pertenece a una subquery
            real_name = self.__queries[i]['tables']['alias'][table_name]
            new_table = table_name
            new_column = self.__mapping[real_name]['fields'].get(column_name, column_name)
        elif self.is_subquery(table_name, i):
            # Si es una subquery
            new_table = table_name  # el nombre de tabla es un alias
            new_column = self.find_sub_column(table_name, column_name, i)
        else:
            # Si es una referencia normal
            new_table = self.__mapping[table_name].get('new_name', table_name)
            new_column = self.__mapping[table_name]['fields'].get(column_name, column_name)

        return new_table, new_column

    @staticmethod
    def is_referenced_column_node(parent, node):
        """Comprueba si el nodo pertenece a una columna con referencia a su tabla.

        Parameters
        ----------
        parent: nltk.Tree
            Nodo progenitor del nodo que se comprueba.
        node: nltk.Tree
            Nodo que se comprueba.

        Returns
        -------
        Boolean
        """
        return parent.label() == 'COLUMN_REFERENCE' \
               and node.label() == 'TABLE_NAMES' \
               and parent[1].label() == 'TABLE_NAMES'

    @staticmethod
    def is_unreferenced_column_node(parent, node):
        """Comprueba si el nodo pertenece a una columna sin referencias a su tabla.

        Parameters
        ----------
        parent: nltk.Tree
            Nodo progenitor del nodo que se comprueba.
        node: nltk.Tree
            Nodo que se comprueba.

        Returns
        -------
        Boolean
        """
        return parent.label() == 'COLUMN_REFERENCE' \
               and len(parent) > 1 \
               and parent[1].label() == 'COLUMN_NAMES' \
               and node.label() == 'COLUMN_NAMES'

    @staticmethod
    def is_table(parent, node):
        """Comprueba si el nodo pertenece a una tabla.

        Parameters
        ----------
        parent: nltk.Tree
            Nodo progenitor del nodo que se comprueba.
        node: nltk.Tree
            Nodo que se comprueba.

        Returns
        -------
        Boolean
        """
        return parent.label() == 'TABLE_REFERENCE' and node.label() == 'TABLE_NAMES'

    def _rename_orphan_column(self, node, i):
        """Renombra una columna que no lleva referencia a ninguna tabla.

        Parameters
        ----------
        node: nltk.Tree
            Nodo que contiene la referencia a la columna.
        i: int
            Indice de la query actual.

        Returns
        -------
        (str, str)
            Nombre nuevo de la tabla, nombre nuevo de la columna
        """
        tables = self.__queries[i]['tables']['names']
        table_name = None
        new_column = None
        if len(tables) > 1:
            raise UnreferencedTableError(tables)
        elif not tables:
            # Si tiene las tablas vacias, hace referencia a un alias
            table_name = next(iter(self.__queries[i]['tables']['alias']))
            table_name, new_column = self.change_column_name(table_name, node[1][0], i)
        elif not self.is_column_alias(node[1][0], i):
            # Si no es un alias que haga referencia a la propia tabla, en una clausula where por ejemplo, sino que es
            # una referencia a una columna sin referencia a tabla
            table_name = tables[0]
            new_column = self.__mapping[table_name]['fields'][node[1][0]]

        return table_name, new_column

    def _process_names(self, node, child, i):
        """Procesa un nodo del AST. En caso de que este contenga un nombre de tabla o columna, lo renombra, en otro
        caso, continua recorriendo en profundidad el nodo.

        Parameters
        ----------
        node: nltk.Tree
            Nodo progenitor del que se procesa.
        child: nltk.Tree
            Nodo a procesar.
        i: int
            Indice de la query que se esta procesando.
        """
        if self.is_referenced_column_node(node, child):
            # Columna con referencia a su tabla. El hijo de indice 1 es la tabla y el 3 la columna
            node[1][0], node[3][0] = self.change_column_name(node[1][0], node[3][0], i)
        elif self.is_unreferenced_column_node(node, child):
            # Columna sin referencia a su tabla. Solo se acepta si hay solamente 1 tabla
            _, _new_column = self._rename_orphan_column(node, i)
            node[1][0] = _new_column if _new_column else node[1][0]
        elif self.is_table(node, child):
            # Tabla
            child[0] = self.__mapping[child[0]].get('new_name', child[0])
        else:
            self.rename_nodes(child, i)

    def rename_nodes(self, node, i):
        """Renombra las tablas y las columnas de acuerdo a los ficheros de mapping. Los cambios tienen lugar en el
        propio arbol recibido por parametro, ya que es un objeto mutable.

        Parameters
        ----------
        node: nltk.Tree
            Nodo procesado.
        i: int
            Indice de la query que se esta procesando.
        """
        [self._process_names(node, child, i) for child in self.get_subtrees(node)]

    def rename_tree(self):
        """Lleva a cabo el renombramiento del AST anteriormente procesado."""
        [self.rename_nodes(e[0], e[1]) for e in self.get_reverse_tree()]

    def remove_comment(self, line):
        m = re.search(r'--.*', line)
        if m:
            comment = m.group(0)
            self.__comments.append(comment.strip())
            return line.replace(comment, '')
        else:
            return line

    @staticmethod
    def find_numbers(line):
        line = re.sub(r' +', ' ', line).strip()
        all_numbers = [int(e.strip()) for e in re.findall(r'^[0-9]+ | [0-9]+ | [0-9]+$', line)]
        return (' ' + str(e) + ' ' for e in all_numbers)

    def find_between(self, string, start, end):
        substrings = []
        start_index = string.find(start)
        if start_index > -1:
            start_offset = start_index + len(start)
            end_index = string[start_offset:].find(end)
            if end_index > -1:
                end_index += start_offset
                substrings.append(start + string[start_offset:end_index] + end)
                end_offset = end_index + len(end)
                return substrings + self.find_between(string[end_offset:], start, end)

        return []

    def clean_line(self, line):
        line = self.remove_comment(line)
        line = (line
                .replace(',', ' , ')
                .replace(';', ' ; ')
                .replace('(', ' ( ')
                .replace(')', ' ) ')
                .replace('=', ' = ')
                )
        literals = self.find_between(line, start="${", end="}")
        literals += self.find_between(line, start="'", end="'")
        literals += self.find_numbers(line)
        if literals:
            line = str(' ' + str(line) + ' ')
            return self.tokenize_literals(line, literals).upper()
        else:
            return line.upper()

    def replace_literal(self, m, replacements):
        self.__words.append(m.group(0))
        return replacements[re.escape(m.group(0))]

    def tokenize_literals(self, line, literals, token=' #WORD# '):
        replacements = {re.escape(k): token for k in iter(literals)}
        pattern = re.compile("|".join(replacements.keys()))
        return pattern.sub(lambda m: self.replace_literal(m, replacements), line)

    def untokenize(self, line, token='#WORD#'):
        replacements = {re.escape(token): k for k in self.get_words()}
        pattern = re.compile("|".join(replacements.keys()))
        return pattern.sub(lambda m: replacements[re.escape(m.group(0))], line)

    def clean_queries(self, queries):
        self.__words = []
        self.__comments = []
        return [' '.join(map(self.clean_line, query)).split(';') for query in queries]

    def get_words(self):
        return self.__words

    def rebuild_query(self, query, pretty=True):
        query = (self.untokenize(query)
                 .replace(' , ', ', ')
                 .replace(' ; ', ';')
                 .replace(' ( ', ' (')
                 .replace(' ) ', ') ')
                 #.replace(' = ', '=')
                 .replace(' . ', '.')
                 )

        if pretty:
            return sqlparse.format(query, reindent=True, keyword_case='upper')
        else:
            return query
