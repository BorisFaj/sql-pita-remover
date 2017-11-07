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
        #self.__mapping = self._load_mapping_files(os.path.join(conf_path, self._config['mapping_dir']))
        self.__mapping = self.__init_debug_mapping()  # ToDo: utilizar los ficheros de mapping en lugar de esto
        self.__subqueries = []
        self.__reverse_tree = []
        self.__queries = {}

    @staticmethod
    def __init_debug_mapping():
        """Me creo estos mappings durante el desarrollo para las pruebas."""
        t3_mapping = {
            "old_name": "t3",
            "new_name": "nueva_t3",
            "fields": {
                "a": "nuevo_a_t3",
                "b": "nuevo_b_t3"
            }
        }

        t2_mapping = {
            "old_name": "t2",
            "new_name": "nueva_t2",
            "fields": {
                "a": "nuevo_a_t2",
                "b": "nuevo_b_t2",
                "p": "nuevo_p_t2",
                "where_column": "nueva_where"
            }
        }

        t1_mapping = {
            "old_name": "t1",
            "new_name": "nueva_t1",
            "fields": {
                "a": "nuevo_a_t1",
                "b": "nuevo_b_t1",
                "c": "nuevo_c_t1",
                "d": "nuevo_d_t1",
                "e": "nuevo_e_t1",
                "p": "nuevo_p_t1",
                "where_column": "nueva_where"
            }
        }

        t4_mapping = {
            "old_name": "t4",
            "new_name": "nueva_t4",
            "fields": {
                "a": "nuevo_a_t4",
                "b": "nuevo_b_t4",
                "c": "nuevo_c_t4",
                "d": "nuevo_d_t4",
                "e": "nuevo_e_t4",
                "f": "nuevo_f_t4",
                "g": "nuevo_g_t4"
            }
        }

        t5_mapping = {
            "old_name": "t5",
            "new_name": "nueva_t5",
            "fields": {
                "a": "nuevo_a_t5",
                "b": "nuevo_b_t5",
                "c": "nuevo_c_t5",
                "d": "nuevo_d_t5",
                "e": "nuevo_e_t5",
                "f": "nuevo_f_t5",
                "g": "nuevo_g_t5"
            }
        }

        t6_mapping = {
            "old_name": "t6",
            "new_name": "nueva_t6",
            "fields": {
                "a": "nuevo_a_t6",
                "b": "nuevo_b_t6"
            }
        }

        mapping = {'t1': t1_mapping, 't2': t2_mapping, 't3': t3_mapping, 't5': t5_mapping,
                   't6': t6_mapping, 't4': t4_mapping}
        mapping['tables'] = {mapping[table]['old_name']: mapping[table]['new_name'] for table in mapping.keys()}
        return mapping

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

    def __init_query__(self, i):
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

    def get_table_name(self, tree, root, i, skip_to=None):
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
        tables = self.__queries[i]['tables']
        for node in tree:
            if type(node) is nltk.Tree:
                if node.label() not in ['TABLE_NAMES', root]:
                    _skip = self.get_table_name(node, root, i, skip_to)
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

    def get_nodes(self, parent, i=0, root='SELECT_SENTENCE'):
        """Recorre el AST y va actualizando el diccionario de queries (self.__queries). Extrae los nombres de columnas y
        tablas y sus alias correspondientes. Si un alias hace referencia a un subquery, se almacena el indice de la
        subquery. Al mismo tiempo se van insertando los nodos procesados que contienen nombres de tablas o columnas
        en una lista (self.__reverse_tree).

        Parameters
        ----------
        parent: nltk.Tree
            Nodo raiz sobre el que se itera.
        i: int
            Indice de la query procesada.
        root: str
            Etiqueta del nodo raiz de una consulta select.
        """
        self.__queries.update(self.__init_query__(i + 1))

        for node in parent:
            next_node = None
            if type(node) is nltk.Tree:
                if node.label() == root:
                    self.__queries['max'] += 1
                    i = self.__queries['max']
                    self.__update_subqueries__(i)
                elif node.label() == 'COLUMN_EXPRESSION':
                    self.__reverse_tree.append((node, i))
                    self.get_column_names(node, self.__queries[i]['columns'])
                elif parent.label() == 'FROM_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
                    self.__reverse_tree.append((node, i))
                    self.get_column_names(node, self.__queries[i]['columns'])
                elif parent.label() == 'TABLE_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
                    next_node = self.get_table_name(node, root, i)

                if not node.label() == 'COLUMN_EXPRESSION':
                    self.get_nodes(self.__skip_to_node__(next_node, node), i)

    def get_reverse_tree(self):
        """Devuelve la lista de nodos extraidos en la funcion get_nodes."""
        return self.__reverse_tree

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

    def get_column_in_query(self, table, column, i):
        """Encuentra el nombre que tiene una columna dentro de una subquery determinada.

        Parameters
        ----------
        table: str
            Nombre de la tabla a la que hace referencia actualmente.
        column: str
            Nombre actual de la columna.
        i: int
            Indice de la query que le hace referencia.

        Returns
        -------
        str
            Nombre nuevo de la columna.
        """
        if column in self.__queries[i]['columns']['alias']:
            return '', column

        try:
            new_reference = [e for e in self.__queries[i]['columns']['names'] if self.equal_columns(column, e)][0]
        except IndexError:  # La columna puede no estar en la query pero pertenece a la tabla
            return table, self.__mapping[table]['fields'][column]

        return new_reference.split('.') if '.' in new_reference else ('', new_reference)

    def find_sub_column(self, table, column, i):
        """Cambia el nombre de una "subcolumna", es decir, una columna que hace referencia a una subquery.

        Parameters
        ----------
        table: str
            Nombre de la tabla a la que hace referencia actualmente.
        column: str
            Nombre actual de la columna.
        i: int
            Indice de la query que le hace referencia.

        Returns
        -------
        str
            Nombre nuevo de la columna.
        """
        child_index = self.__queries[i]['tables']['alias'][table]['subquery']
        new_table, new_column = self.get_column_in_query(table, column, child_index)

        if not new_table:  # era el alias
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
        #     print('Tabla: {} - Column: {} - i: {}'.format(table_name, column_name, i))
        if not self.is_subquery(table_name, i) and self.is_table_alias(table_name, i):
            # Si es un alias que no pertenece a una subquery
            real_name = self.__queries[i]['tables']['alias'][table_name]
            new_table = table_name
            new_column = self.__mapping[real_name]['fields'][column_name]
        elif self.is_subquery(table_name, i):
            # Si es una subquery
            new_table = table_name  # el nombre de tabla es un alias
            new_column = self.find_sub_column(table_name, column_name, i)
        else:
            # Si es una referencia normal
            new_table = self.__mapping[table_name]['new_name']
            new_column = self.__mapping[table_name]['fields'].get(column_name, column_name)

        return new_table, new_column

    def change_name(self, node, i):
        """Cambia los nombres de tablas y columnas por los proporcionados en los ficheros de mapping.
        Los cambios tienen lugar en el propio arbol recibido por parametro, ya que es un objeto mutable.

        Parameters
        ----------
        node: nltk.Tree
            Nodo procesado.
        i: int
            Indice de la query que se esta procesando.
        """
        for child in filter(lambda child: isinstance(child, nltk.Tree), node):
            #         print('node: {} - child: {}'.format(node.label(), child.label()))
            if node.label() == 'COLUMN_REFERENCE' and child.label() == 'TABLE_NAMES' and node[1].label() == 'TABLE_NAMES':
                # Columna con referencia a su tabla
                table_name = node[1][0]
                column_name = node[3][0]
                print('Columna con referencia. Tabla: {} - Columna: {}'.format(table_name, column_name))
                node[1][0], node[3][0] = self.change_column_name(table_name, column_name, i)
            elif node.label() == 'COLUMN_REFERENCE' and len(node) > 1 and node[
                1].label() == 'COLUMN_NAMES' and child.label() == 'COLUMN_NAMES':
                # Columna sin referencia a su tabla. Solo se acepta si hay solamente 1 tabla
                if len(self.__queries[i]['tables']['names']) > 1:
                    raise ValueError('Le falta referencia a la tabla o sobran tablas en el from: {}'.format(node))

                if not self.__queries[i]['tables']['names']:
                    # Si tiene las tablas vacias, hace referencia a un alias
                    table_name = next(iter(self.__queries[i]['tables']['alias']))
                    column_name = node[1][0]
                    print('Las mando')
                    _, node[1][0] = self.change_column_name(table_name, column_name, i)
                elif node[1][0] not in self.__queries[i]['columns']['alias']:
                    # Comprueba que no es un alias de la propia tabla en una clausula where por ejemplo
                    print('columna: {}'.format(node[1][0]))
                    _table_name = self.__queries[i]['tables']['names'][0]
                    print('tabla: {}'.format(_table_name))
                    node[1][0] = self.__mapping[_table_name]['fields'][node[1][0]]
            elif node.label() == 'TABLE_REFERENCE' and child.label() == 'TABLE_NAMES':
                # Tabla
                child[0] = self.__mapping[child[0]]['new_name']
            else:
                self.change_name(child, i)