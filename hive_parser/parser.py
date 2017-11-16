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
    def __init__(self, conf, log_level=logging.INFO):
        logging.basicConfig(level=log_level, format='%(levelname)s %(name)s %(asctime)s %(message)s')
        self._logger = logging.getLogger('hive_parser')
        self._config = self.load_json(conf)
        self.__mapping = self.load_mapping_files(self._config['mapping_dir']) if 'mapping_dir' in self._config else None
        self.__queries_elements = []
        self.__reverse_tree = []
        self.__queries = {}
        self.__comments = []
        self.__words = []
        self._terminals = None
        self.tree = None
        self.__grammar = self._read_grammar_(self._config['grammar_file'])

    def get_grammar(self):
        """Devuelve la gramatica utilizada."""
        return self.__grammar

    def get_words(self):
        """Devuelve las palabras que se han eliminado de la query: variables hive, literales y constantes."""
        return self.__words

    def get_comments(self):
        """Devuelve los comentarios que se han eliminado de la query."""
        return self.__comments

    @staticmethod
    def _read_query_file(path):
        """Lee una query en un fichero.

        Parameters
        ----------
        path: str
            Ruta del fichero.

        Returns
        -------
        generator
            Devuelve cada vez una linea del fichero.
        """
        with open(path, 'r') as file:
            yield [line.replace('\n', ' ').replace('\t', ' ') for line in file.readlines()]

    def load_queries(self, path=None):
        """Lee las queries situadas en la ruta especificada. Si no se especifica ruta, se busca en el fichero de
        configuracion.

        Returns
        -------
        path: str
            Ruta al directorio de las queries. Si no se especifica, se lee del fichero de configuracion.

        Returns
        -------
        dict
            Diccionario cuya clave es el nombre del fichero (solo el nombre, no la ruta completa) y el valor es las
            queries almacenadas en este.
        """
        if not path:
            path = self._config.get('input_path', None)

        if not os.path.exists(path):
            self._logger.error("La carpeta especificada para leer los ficheros de queries '{}' no existe. Por favor, "
                               "comprueba que existe y que tiene los permisos adecuados".format(path))
            raise NotADirectoryError

        return {file: self._read_query_file(os.path.join(path, file)) for file in os.listdir(path)}

    def save_renamed(self, queries, path=None):
        """Renombra las queries de acuerdo a los ficheros de mapping y las guarda en la ruta especificada. El nombre de
        cada fichero es el mismo que en la entrada.

        Parameters
        ----------
        queries: dict
            Diccionario generado por la funcion load_queries. La clave es el nombre del fichero y el valor es un
            generador de las queries almacenadas en este.
        path: str
            Ruta al directorio donde se van a almacenar las queries renombradas. Si no se especifica, se lee del fichero
            de configuracion.
        """
        if not path:
            path = self._config['output_path']

        if not os.path.exists(path):
            self._logger.error("La carpeta de salida para guardar los ficheros renombrados '{}' no existe. Por favor, "
                               "comprueba que existe y que tiene los permisos adecuados".format(path))
            raise NotADirectoryError

        self.__comments = []
        compacted_queries = self.__process_file(queries)

        [[self.__parse_and_save(query, file, path) for query in queries if query] for file, queries in compacted_queries]
        self._logger.info('Todas las queries han sido correctamente renombradas y almacenadas en la ruta {}'.format(path))

    def __process_file(self, queries):
        """Preprocesa un fichero de query. Se eliminan los comentarios, se juntan todas las lineas en un solo string
        y se separan las queries por cada punto y coma.

        Parameters
        ----------
        queries: dict
            Diccionario generado por la funcion load_queries. La clave es el nombre del fichero y el valor es un
            generador de las queries almacenadas en este.

        Returns
        -------
        generator
            Devuelve cada vez uno de los ficheros preprocesados.
        """
        for file in queries.keys():
            yield file, ' '.join(map(self._remove_comment, next(line for line in queries[file]))).split(';')

    def __parse_and_save(self, query, file_name, path):
        """Parsea y almacena una query. La query que entra viene de iterar sobre un generador, el proposito de esta
        funcion es poder realizar el procesamiento sin tener que almacenar todas las queries en memoria en ningun
        momento.

        Parameters
        ----------
        query: str
            Query con los comentarios eliminados.
        file_name: str
            Nombre del fichero de salida.
        path: str
            Ruta de los ficheros de salida.

        Returns
        -------

        """
        self._logger.debug(query)
        self.__queries_elements = []
        self.__reverse_tree = []
        self.__queries = {}
        parsed = self.parse_query(query)
        _out_path = os.path.join(path, file_name)
        self.save_query(parsed.rename_tree().rebuild_query(), _out_path)

    @staticmethod
    def save_query(query, file):
        """Agrega la query al fichero de texto especificado.

        Parameters
        ----------
        query: str
            Query.
        file: str
            Fichero de destino.
        """
        with open(file, 'a') as f:
            f.writelines(query + '\n;\n\n')

    @staticmethod
    def __str_to_terminals(s):
        """Pretende separar un terminal extraido de la query en la tupla (tabla, columna). En la practica, sin una
        estructura gramatical a priori, es imposible saber si la entrada se refiere a (tabla, columna) o (esquema, tabla).
        De la misma forma, si la cadena no lleva punto, es imposible saber a priori si se refiere a un esquema, una tabla,
        o una columna.

        Parameters
        ----------
        s: str
            Terminal a separar.

        Returns
        -------
        str, str
            tabla, columna.
        """
        if '.' in s:
            return ["'" + t + "'" for t in s.split('.')]
        else:
            s = "'" + s + "'"
            return s, s

    def _read_grammar_(self, path=None, new_terminals=None):
        """Lee las reglas de produccion de la gramatica contenidas en el fichero indicado. Si se especifica una lista
        de nuevos simbolos terminales, esta se agrega a las reglas de produccion que contienen los nombres de tablas y
        columnas.

        Parameters
        ----------
        path: str
            Ruta al fichero que contiene las reglas de produccion de la gramatica. Si no se especifica esta variable,
            la lee del fichero de configuracion.
        new_terminals: list(str)
            Lista de nuevos simbolos terminales, extraidos de la query.

        Returns
        -------
        nltk.grammar.CFG
            Objeto nltk que contiene la gramatica libre de contexto leida.
        """
        if not path:
            path = self._config['grammar_file']

        f = open(path, 'r')
        grammar_file = ' '.join(f.readlines())
        self._terminals = [t.upper().strip().replace("'", "") for t in self._find_between(grammar_file, "'", "'")]
        if new_terminals:
            new_tables, new_columns = zip(*[self.__str_to_terminals(t) for t in new_terminals])
            tables = '\nTABLE_NAMES -> ' + '|'.join(new_tables)
            columns = '\nCOLUMN_NAMES ->' + '|'.join(new_columns)

            return nltk.CFG.fromstring(grammar_file + tables + columns)

        else:
            return nltk.CFG.fromstring(grammar_file)

    @staticmethod
    def __skip_to_node(target, current):
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
            self._logger.error('No se encuentra el fichero json especificado: {}'.format(path))
            raise FileNotFoundError

        js = open(path)
        return json.load(js)

    def load_mapping_files(self, path):
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
            self._logger.error('No se encuentra la ruta especificada: {}'.format(path))
            raise FileNotFoundError

        return {f.replace('.json', ''): self.load_json(os.path.join(path, f)) for f in os.listdir(path)}

    def parse_query(self, query, trace=0):
        """Parsea una query en texto plano para transformarla en una sentencia de la gramatica.
            1. Se preprocesa la query: se cambian las variables hive, literales y constantes por el simbolo #WORD#
            2. Se ponen espacio a cada lado de los signos de puntuacion, parentesis, etc.
            3. Si quedan varios espacios seguidos, se simplifican a un espacio.
            4. Se tokenizan las palabras para convertirlas elementos de una lista.

        Parameters
        ----------
        query: str
            Query que se va a transformar.
        trace: int
            Define el nivel de traza que genera el objeto nltk.ChartParser. Si es 0 no genera traza.

        Returns
        -------
        Parser
            Devuelve un objeto parser que contiene el arbol generado en la variable Parser.tree.
        """
        self.__words = []
        clean_query = self._clean_line(query)
        sent = clean_query.replace(',', ' , ').replace('.', ' . ').replace('(', ' ( ').replace(')', ' ) ')
        sent = [chunk.upper() for chunk in re.sub(' +', ' ', sent).split(' ') if chunk]
        new_terminals = set(filter(lambda x: x not in self._terminals, sent))
        self.__grammar = self._read_grammar_(self._config['grammar_file'], new_terminals)
        parser = nltk.ChartParser(self.__grammar, trace=trace)

        self.tree = next(parser.parse(sent), None)
        return self

    def __update_subqueries(self, i):
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
            self._logger.debug('No hay subqueries para asignar la numero {}'.format(i))
            return

        _node = self.__queries_elements.pop()
        _node.update({'subquery': i})

    def __process_table_name(self, parent, node, root, i, skip_to):
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
            _skip = self.__iter_table_node(node, root, i, skip_to)
            skip_to = self.__skip_to_node(skip_to, _skip)
        elif node.label() == root:
            # Si es el primer nodo root encontrado
            skip_to = self.__skip_to_node(skip_to, parent)
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
            self._logger.debug('Se mete nodo alias: {}'.format(parent))
            self.__reverse_tree.append((parent, i))

        return skip_to

    @staticmethod
    def __merge_schema(node):
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

        # Los hijos deberian ser: TABLE_NAMES POINT TABLE_NAMES
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

    def __iter_table_node(self, tree, root, i, skip_to=None):
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
        self.__merge_schema(tree)
        skip_to = [self.__process_table_name(tree, node, root, i, skip_to) for node in self.get_subtrees(tree)]

        return skip_to[0] if skip_to else None

    def __process_column_node(self, parent, node, columns):
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
            self.__iter_column_node(node, columns)
        elif parent.label() != 'COLUMN_ALIAS':
            # Si es una referencia directa a una columna
            columns['names'] += [''.join(parent.leaves()).replace('DISTINCT', '')]
        elif parent.label() == 'COLUMN_ALIAS':
            # Si es un alias
            columns['alias'].setdefault(node.leaves()[0], columns['names'][-1])

        return columns

    def __iter_column_node(self, tree, columns):
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
        columns = [self.__process_column_node(tree, node, columns) for node in self.get_subtrees(tree)]

        return columns

    def __rename_non_select(self, node):
        """Renombra las referencias a tablas en nodos distintos a select. Como no tienen dependencias de
        subqueries ni alias, se pueden renombrar directamente.

        Parameters
        ----------
        node: nltk.Tree
            Nodo que se va a renombrar.
        """
        table_node = [child for child in self.get_subtrees(node) if child.label() == 'TABLE_REFERENCE'][0]
        self.__merge_schema(table_node)
        table_name = table_node[0][0]
        if table_name in self.__mapping:
            table_node[0][0] = self.__mapping[table_name]['new_name']

    def __process_node(self, node, parent, root, i):
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
            self.__update_subqueries(i)
        elif node.label() == 'COLUMN_EXPRESSION':
            # Si es un nodo columna, se mete a la lista de procesados y se explora el trozo de query
            self.__reverse_tree.append((node, i))
            self.__iter_column_node(node, self.__queries[i]['columns'])
        elif parent.label() == 'FROM_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
            # Si es un nodo from, se mete a la lista de procesados y se explora el trozo de query
            self.__reverse_tree.append((node, i))
            self.__iter_column_node(node, self.__queries[i]['columns'])
        elif parent.label() == 'TABLE_EXPRESSION' and node.label() != 'TABLE_EXPRESSION':
            # Si es un nodo tabla, se explora el trozo de query y se obtiene el nodo de la primera subquery que
            # contiene, en caso de que contenga alguna
            next_node = self.__iter_table_node(node, root, i)
        elif node.label() in ['INSERT_EXPRESSION', 'CREATE_EXPRESSION']:
            # Si es la parte del insert o create table, se renombra directamente
            self.__rename_non_select(node)

        if not node.label() == 'COLUMN_EXPRESSION':
            # Si el nodo es de columnas, ya esta procesado y no es necesario profundizar
            self._process_tree(self.__skip_to_node(next_node, node), i, root)

    def _process_tree(self, tree, i=0, root='SELECT_SENTENCE'):
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
        [self.__process_node(node, tree, root, i) for node in self.get_subtrees(tree)]

    def _get_reverse_tree(self):
        """Devuelve la lista de nodos extraidos en la funcion get_nodes."""
        if not self.__reverse_tree:
            self._logger.error('La query todavia no ha sido procesada. Por favor, ejecuta la funcion process_tree() '
                               'antes de volver a intentarlo.')
            raise AssertionError

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
        if not self.__queries:
            self._logger.error('El arbol no ha sido procesado todavia. Por favor ejecuta la funcion rename_tree() para '
                               'procesar el arbol.')
            raise LookupError
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
        if not self.__queries:
            self._logger.error('El arbol no ha sido procesado todavia. Por favor ejecuta la funcion rename_tree() para '
                               'procesar el arbol.')
            raise LookupError
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
    def _equal_columns(a, b):
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

    def _get_unreferenced_table(self, i):
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
        if not self.__queries:
            self._logger.error('El arbol no ha sido procesado todavia. Por favor ejecuta la funcion rename_tree() para '
                               'procesar el arbol.')
            raise LookupError

        return column in self.__queries[i]['columns']['alias']

    def _get_referenced_names(self, names, i):
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
            return self._get_unreferenced_table(i), names

    def __get_reference_in_subquery(self, current_column, target_i):
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
                             self._equal_columns(current_column, e)]
            return self._get_referenced_names(new_reference[0], target_i)
        except IndexError:
            # La columna puede no estar en la subquery, pero pertenecer a la subtabla
            pass

        # Si no es un alias y va sin referencia a tabla, esta haciendo referencia a una columna de la subquery que no
        # aparece en la consulta pero que deberia existir. Esto solo se permite si la subquery consulta una sola tabla
        table = self._get_unreferenced_table(target_i)

        try:
            return table, self.__mapping[table]['fields'][current_column]
        except KeyError as err:
            self._logger.warning("{}. La referencia a la columna '{}' de la tabla '{}' no se encuentra en los ficheros "
                                "de mapping proporcionados. Se devuelve el nombre original".format(err, current_column, table))
            return table, current_column

    def __find_sub_column(self, current_table, current_column, i):
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
        new_table, new_column = self.__get_reference_in_subquery(current_column, child_index)

        if not new_table:  # era un alias
            return new_column

        return self.__change_column_name(new_table, new_column, child_index)[1]

    def __change_column_name(self, table_name, column_name, i):
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
            new_column = self.__find_sub_column(table_name, column_name, i)
        else:
            # Si es una referencia normal
            new_table = self.__mapping[table_name].get('new_name', table_name)
            new_column = self.__mapping[table_name]['fields'].get(column_name, column_name)

        return new_table, new_column

    @staticmethod
    def _is_referenced_column_node(parent, node):
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
    def _is_unreferenced_column_node(parent, node):
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
    def _is_table(parent, node):
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
            table_name, new_column = self.__change_column_name(table_name, node[1][0], i)
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
        if self._is_referenced_column_node(node, child):
            # Columna con referencia a su tabla. El hijo de indice 1 es la tabla y el 3 la columna
            node[1][0], node[3][0] = self.__change_column_name(node[1][0], node[3][0], i)
        elif self._is_unreferenced_column_node(node, child):
            # Columna sin referencia a su tabla. Solo se acepta si hay solamente 1 tabla
            _, _new_column = self._rename_orphan_column(node, i)
            node[1][0] = _new_column if _new_column else node[1][0]
        elif self._is_table(node, child):
            # Tabla
            child[0] = self.__mapping[child[0]].get('new_name', child[0])
        else:
            self._rename_children(child, i)

    def _rename_children(self, node, i):
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
        """Procesa el AST y lleva a cabo el renombramiento conforme a los ficheros de mapping."""
        if not self.tree:
            self._logger.error('El arbol no ha sido generado todavia. Por tanto, todavia no hay ninguna query que '
                               'renombrar. Por favor ejecuta la funcion parse_query() para generar el arbol')
            raise LookupError

        if not self.__mapping:
            self._logger.error('No se han cargado los ficheros de mapping. Por favor ejecuta la funcion '
                               'load_mapping_files() para cargarlos antes de proceder al renombramiento.')
            raise LookupError

        self.__queries_elements = []
        self.__reverse_tree = []
        self.__queries = {}
        self._process_tree(self.tree)
        [self._rename_children(e[0], e[1]) for e in self._get_reverse_tree()]

        return self

    def _remove_comment(self, line):
        """Elimina el comentario de una linea y lo almacena"""
        m = re.search(r'--.*', line)
        if m:
            comment = m.group(0)
            self.__comments.append(comment.strip())
            return line.replace(comment, '')
        else:
            return line

    @staticmethod
    def _find_numbers(line):
        """"Devuelve la lista de los numeros contenidos en la linea."""
        line = re.sub(r' +', ' ', line).strip()
        all_numbers = [int(e.strip()) for e in re.findall(r'^[0-9]+ | [0-9]+ | [0-9]+$', line)]
        return (' ' + str(e) + ' ' for e in all_numbers)

    def _find_between(self, string, start, end):
        substrings = []
        start_index = string.find(start)
        if start_index > -1:
            start_offset = start_index + len(start)
            end_index = string[start_offset:].find(end)
            if end_index > -1:
                end_index += start_offset
                substrings.append(start + string[start_offset:end_index] + end)
                end_offset = end_index + len(end)
                return substrings + self._find_between(string[end_offset:], start, end)

        return []

    def _clean_line(self, line):
        """Limpia una linea. Busca referencias a variables hive, literales y constantes; las remplaza por #WORD# y se
        almacena todo para posteriormente reconstruir la query con la forma original."""
        if not line:
            return line

        line = (line
                .replace(',', ' , ')
                .replace(';', ' ; ')
                .replace('(', ' ( ')
                .replace(')', ' ) ')
                .replace('=', ' = ')
                )
        variables = self._find_between(line, start="${", end="}")
        variables += self._find_between(line, start="'", end="'")
        variables += self._find_numbers(line)
        if variables:
            line = str(' ' + str(line) + ' ')
            return self._tokenize_vars(line, variables).upper()
        else:
            return line.upper()

    def __replace_var(self, variable, replacements):
        """Remplaza la variables por la que corresponda, segun el diccionario 'replacements'.

        Parameters
        ----------
        variable: str
            Variable a sustituir.
        replacements: dict
            Diccionario con los remplazamientos a realizar.

        Returns
        -------
        str
            Variable sustituida.
        """
        self.__words.append(variable.group(0))
        return replacements[re.escape(variable.group(0))]

    def _tokenize_vars(self, line, variables, token=' #WORD# '):
        """Tokeniza las variables en una linea sustituyendolos por el especificado por parametro.

        Parameters
        ----------
        line: str
            Linea.
        variables: list(str)
            Lista de variables.
        token: str
            Token que va a sustituir a las variables especificadas.

        Returns
        -------
        str
            Linea con las variables tokenizadas.
        """
        replacements = {re.escape(k): token for k in iter(variables)}
        pattern = re.compile("|".join(replacements.keys()))
        return pattern.sub(lambda m: self.__replace_var(m, replacements), line)

    def _untokenize(self, line, token='#WORD#'):
        """Vuelve a poner las variables de una linea, sustituyendo a su token correspondiente."""
        if not self.__words:
            return line

        replacements = {re.escape(token): k for k in self.__words}
        pattern = re.compile("|".join(replacements.keys()))
        return pattern.sub(lambda m: replacements[re.escape(m.group(0))], line)

    def rebuild_query(self, pretty=True):
        """Reconstruye la query representada por el arbol procesado. Se vuelven a poner las variables anteriormente
        tokenizadas, se eliminan espacios utilizados para parsear y se vuelven a poner los comentarios eliminados al
        principio de la query.

        Parameters
        ----------
        pretty: boolean
            Formatear la query a una forma humanamente amigable.

        Returns
        -------
        str
            Query reconstruida.
        """
        if not self.tree:
            self._logger.error('El arbol no ha sido generado todavia. Por tanto, todavia no hay ninguna query que '
                               'reconstruir. Por favor ejecuta la funcion parse_query() para generar el arbol')
            raise LookupError

        if not self.__queries:
            self._logger.error('El arbol no ha sido procesado todavia. Por favor ejecuta la funcion rename_tree() para '
                               'procesar el arbol.')
            raise LookupError

        query = ' '.join(self.tree.leaves())
        query = (self._untokenize(query)
                 .replace(' , ', ', ')
                 .replace(' ; ', ';')
                 .replace(' ( ', ' (')
                 .replace(' ) ', ') ')
                 #.replace(' = ', '=')
                 .replace(' . ', '.')
                 )

        if pretty:
            return '\n'.join(self.__comments) + '\n' + sqlparse.format(query, reindent=True, keyword_case='upper')
        else:
            return query
