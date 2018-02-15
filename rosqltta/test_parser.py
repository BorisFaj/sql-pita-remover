import os
import nltk
import copy
import re
from unittest import TestCase
from rosqltta.parser import Parser, UnreferencedTableError


class TestParser(TestCase):
    def setUp(self):
        self.hv = Parser('../conf/config.conf')

    def test_get_grammar(self):
        self.hv.get_grammar()

    def test_get_words(self):
        self.hv.get_words()

    def test_get_comments(self):
        self.hv.get_comments()

    @staticmethod
    def _create_test_file(path):
        file = open(path, 'w')
        file.write('Prueba de un fichero\n')
        file.write('Primera linea\t\n')
        file.close()

    def test_read_query_file(self):
        file_path = os.path.join('.', '.test_file')
        self._create_test_file(file_path)
        read_file = next(self.hv._read_query_file(file_path))

        self.assertEqual(first=read_file[0], second='Prueba de un fichero ')
        self.assertEqual(first=read_file[1], second='Primera linea  ')
        os.remove(file_path)

    def test_load_queries(self):
        test_dir = '.test_files'
        if not os.path.exists(test_dir):
            try:
                self.hv.load_queries(test_dir)
            except:
                self.assertRaises(NotADirectoryError)

            os.mkdir(test_dir)

        file_path_1 = os.path.join('.', test_dir, '.test_file1')
        file_path_2 = os.path.join('.', test_dir, '.test_file2')

        self._create_test_file(path=file_path_1)
        self._create_test_file(path=file_path_2)

        queries = self.hv.load_queries(test_dir)

        self.assertEqual(first=next(queries['.test_file1'], None)[0], second='Prueba de un fichero ')
        self.assertEqual(first=next(queries['.test_file2'], None)[1], second='Primera linea  ')

        os.remove(file_path_1)
        os.remove(file_path_2)
        os.rmdir(test_dir)

    def test_save_renamed(self):
        pass

    def test_process_file(self):
        pass

    def test_parse_and_save(self):
        pass

    def test_save_query(self):
        pass

    def test_str_to_terminals(self):
        column_name = 'a'
        table_column_name = 't.a'

        table, column = self.hv._Parser__str_to_terminals(column_name)
        self.assertEqual(table, column, "'" + column_name + "'")

        table, column = self.hv._Parser__str_to_terminals(table_column_name)
        self.assertEqual(table, "'t'")
        self.assertEqual(column, "'a'")

    def test__read_grammar_(self):
        pass

    def test_skip_to_node(self):
        current = nltk.Tree(3, [4])
        target = 'node_label'
        node = self.hv._Parser__skip_to_node(target, current)
        self.assertEqual(current, node)

        target = nltk.Tree(3, [44])
        node = self.hv._Parser__skip_to_node(target, current)
        self.assertEqual(target, node)

    def test_init_query(self):
        query = self.hv._init_query(0)

        self.assertTrue('max' in query)
        self.assertTrue(0 in query)
        self.assertTrue('columns' in query[0])
        self.assertTrue('alias' in query[0]['columns'])
        self.assertTrue('names' in query[0]['columns'])
        self.assertTrue('tables' in query[0])
        self.assertTrue('alias' in query[0]['tables'])
        self.assertTrue('names' in query[0]['tables'])

    def test_load_json(self):
        if not os.path.exists('path_falso'):
            try:
                self.hv.load_json('path_falso')
            except:
                self.assertRaises(FileNotFoundError)

    def test_load_mapping_files(self):
        if not os.path.exists('path_falso'):
            try:
                self.hv.load_mapping_files('path_falso')
            except:
                self.assertRaises(FileNotFoundError)

    def test_parse_query(self):
        hv_parser = copy.copy(self.hv)
        new_tree = hv_parser.parse_query('SELECT a FROM ${hivevar:tabla} WHERE (a = 1 AND b=2) OR c = 4')
        self.assertTrue(new_tree._Parser__grammar)
        self.assertTrue(new_tree.tree)

    def test_update_subqueries(self):
        i = 0
        hv_parser = copy.copy(self.hv).parse_query('SELECT t1.a FROM (SELECT a FROM t2) AS t1')
        # Si la lista esta vacia devuelve node
        self.assertFalse(hv_parser._Parser__update_subqueries(i))

        # Creo un elemento sintetico como los de queries_elements para comprobar que lo cambia
        element = [{'_test_aux_': 2}]
        hv_parser._Parser__queries_elements.append(element[0])
        hv_parser._Parser__update_subqueries(i)

        self.assertTrue('_test_aux_' in element[0])
        self.assertTrue('subquery' in element[0])
        self.assertEqual(element[0]['subquery'], i)

    def test_process_table_name(self):
        pass

    def test_merge_schema(self):
        # Funcionamiento normal
        tree = nltk.Tree('TABLE_REFERENCE',
                         [nltk.Tree('TABLE_NAMES', ['esquema']),
                          nltk.Tree('POINT', ['.']),
                          nltk.Tree('TABLE_NAMES', ['tabla'])
                          ])

        self.assertTrue(self.hv._Parser__merge_schema(tree))
        self.assertEqual(tree[0].label(), 'TABLE_NAMES')
        self.assertEqual(tree[0].leaves()[0], 'esquema.tabla')

        # En caso de que el nodo no lleve referencia al esquema
        tree = nltk.Tree('TABLE_REFERENCE',
                         [nltk.Tree('TABLE_NAMES', ['tabla'])
                          ])

        self.assertFalse(self.hv._Parser__merge_schema(tree))
        self.assertEqual(tree[0].label(), 'TABLE_NAMES')  # Compruebo que no me modifica nada
        self.assertEqual(tree[0].leaves()[0], 'tabla')

        # En caso de que ni sea un nodo de tabla
        tree = nltk.Tree('SELECT_EXPRESSION',
                         [nltk.Tree('TABLE_NAMES', ['tabla'])
                          ])

        self.assertFalse(self.hv._Parser__merge_schema(tree))
        self.assertEqual(tree[0].label(), 'TABLE_NAMES')  # Compruebo que no me modifica nada
        self.assertEqual(tree[0].leaves()[0], 'tabla')

    def test_iter_table_node(self):
        pass

    def test_process_column_node(self):
        # Referencia directa a columna
        node = nltk.Tree('COLUMN_REFERENCE',
                         [nltk.Tree('COLUMN_NAMES', ['tabla'])
                          ])

        columns = self.hv._init_query(0)[0]['columns']
        columns = self.hv._Parser__process_column_node(node, node[0], columns)
        self.assertTrue(columns['names'][0] == 'tabla')

        # Alias
        node = nltk.Tree('COLUMN_ALIAS', ['AS', nltk.Tree('COLUMN_NAMES', ['tabla_alias'])])

        columns = self.hv._init_query(0)[0]['columns']
        columns = self.hv._Parser__process_column_node(node, node[1], columns)
        self.assertTrue('tabla_alias' in columns['alias'])
        self.assertTrue(columns['alias']['tabla_alias'] == 'tabla')
        self.assertTrue(columns['names'][0] == 'tabla')

        # Con iteracion, pasandole el padre
        node = nltk.Tree('COLUMN_EXPRESSION',
                         [nltk.Tree('COLUMN_REFERENCE', [nltk.Tree('COLUMN_NAMES', ['tabla'])]),
                          nltk.Tree('COLUMN_ALIAS', ['AS', nltk.Tree('COLUMN_NAMES', ['tabla_alias'])])
                          ])

        columns = self.hv._init_query(0)[0]['columns']
        columns = self.hv._Parser__process_column_node(node, node[1], columns)
        self.assertTrue('tabla_alias' in columns['alias'])
        self.assertTrue(columns['alias']['tabla_alias'] == 'tabla')
        self.assertTrue(columns['names'][0] == 'tabla')

    def test_iter_column_node(self):
        node = nltk.Tree('COLUMN_EXPRESSION',
                         [nltk.Tree('COLUMN_REFERENCE', [nltk.Tree('COLUMN_NAMES', ['tabla'])]),
                          nltk.Tree('COLUMN_ALIAS', ['AS', nltk.Tree('COLUMN_NAMES', ['tabla_alias'])])
                          ])

        queries = self.hv._init_query(0)
        self.hv._Parser__iter_column_node(node, queries[0]['columns'])
        columns = queries[0]['columns']

        self.assertTrue('tabla_alias' in columns['alias'])
        self.assertTrue(columns['alias']['tabla_alias'] == 'tabla')
        self.assertTrue(columns['names'][0] == 'tabla')

    def test___rename_non_select(self):
        node = nltk.Tree('CREATE_EXPRESSION',
                         ['CREATE',
                          'TABLE',
                          nltk.Tree('TABLE_REFERENCE', [nltk.Tree('TABLE_NAMES', ['tabla'])]),
                          'AS',
                          nltk.Tree('SELECT_SENTENCE', [nltk.Tree('COLUMN_NAMES', ['blablabla...'])])
                          ])

        hv_parser = copy.copy(self.hv)
        hv_parser._Parser__mapping = {'tabla': {'new_name': 'nombre_cambiado'}}
        hv_parser._Parser__rename_non_select(node)
        self.assertEqual(node[2][0][0], 'nombre_cambiado')

    def test_process_node(self):
        pass

    def test_process_tree(self):
        pass

    def test_get_reverse_tree(self):
        try:
            self.hv._get_reverse_tree()
        except:
            self.assertRaises(AssertionError)

        hv_parser = copy.copy(self.hv).parse_query('SELECT t1.a FROM t1')
        hv_parser._process_tree(hv_parser.tree)
        self.assertEquals(len(hv_parser._get_reverse_tree()), 7)

    def test_get_queries(self):
        self.hv.get_queries()

    def test_is_table_alias(self):
        try:
            self.hv.is_table_alias('tabla', 0)
        except:
            self.assertRaises(LookupError)

        hv_parser = copy.copy(self.hv)
        hv_parser._Parser__queries = {0: {'tables': {'alias': {'alias_t1': 'tabla1'}}}}
        self.assertEquals(hv_parser.is_table_alias('alias_t1', 0), 'tabla1')

    def test_is_subquery(self):
        try:
            self.hv.is_subquery('tabla', 0)
        except:
            self.assertRaises(LookupError)

        hv_parser = copy.copy(self.hv)
        hv_parser._Parser__queries = {0: {'tables': {'alias': {'alias_t1': 'tabla1'}}}}
        self.assertTrue(hv_parser.is_table_alias('alias_t1', 0))
        self.assertFalse(hv_parser.is_table_alias('otracosa', 0))

    def test_get_subtrees(self):
        node = nltk.Tree('CREATE_EXPRESSION',
                         ['CREATE',
                          'TABLE',
                          nltk.Tree('TABLE_REFERENCE', [nltk.Tree('TABLE_NAMES', ['tabla'])]),
                          'AS',
                          nltk.Tree('SELECT_SENTENCE', [nltk.Tree('COLUMN_NAMES', ['blablabla...'])])
                          ])

        sub_trees = list(self.hv.get_subtrees(node))

        self.assertEquals(len(sub_trees), 2)
        self.assertEquals(sub_trees[0].label(), 'TABLE_REFERENCE')
        self.assertEquals(sub_trees[1].label(), 'SELECT_SENTENCE')

    def test_equal_columns(self):
        self.assertTrue(self.hv._equal_columns('a', 'a'))
        self.assertTrue(self.hv._equal_columns('t1.a', 't2.a'))
        self.assertTrue(self.hv._equal_columns('t1.a', 'a'))
        self.assertTrue(self.hv._equal_columns('a', 't2.a'))

        self.assertFalse(self.hv._equal_columns('a', 'b'))
        self.assertFalse(self.hv._equal_columns('t1.a', 't2.b'))
        self.assertFalse(self.hv._equal_columns('t1.a', 'b'))
        self.assertFalse(self.hv._equal_columns('a', 't2.b'))

    def test_get_unreferenced_table(self):
        hv_parser = copy.copy(self.hv)
        hv_parser._Parser__queries = {0: {'tables': {'names': ['tabla1', 'tabla2']}}}
        try:
            hv_parser._get_unreferenced_table(0)
        except:
            self.assertRaises(UnreferencedTableError)

        hv_parser._Parser__queries = {0: {'tables': {'names': ['tabla1']}}}
        tabla = hv_parser._get_unreferenced_table(0)
        self.assertEquals(tabla, 'tabla1')

    def test_is_referenced_column(self):
        self.assertTrue(self.hv.is_referenced_column('t1.a'))
        self.assertFalse(self.hv.is_referenced_column('a'))

    def test_is_column_alias(self):
        try:
            self.hv._get_unreferenced_table(0)
        except:
            self.assertRaises(LookupError)

        hv_parser = copy.copy(self.hv)
        hv_parser._Parser__queries = {0: {'columns': {'alias': {'alias1': 'tabla1'}}}}

        self.assertTrue(hv_parser.is_column_alias('alias1', 0))
        self.assertFalse(hv_parser.is_column_alias('alias2', 0))

    def test_get_referenced_names(self):
        pass

    def test_get_reference_in_subquery(self):
        pass

    def test_find_sub_column(self):
        pass

    def test_change_column_name(self):
        pass

    def test_is_referenced_column_node(self):
        node = nltk.Tree('COLUMN_REFERENCE',
                         [nltk.Tree('TABLE_NAMES', ['tabla1']),
                          nltk.Tree('TABLE_NAMES', ['tabla2'])
                          ])

        self.assertTrue(self.hv._is_referenced_column_node(node, node[0]))

    def test_is_unreferenced_column_node(self):
        node = nltk.Tree('COLUMN_REFERENCE',
                         [nltk.Tree('COLUMN_NAMES', ['columna1']),
                          nltk.Tree('COLUMN_NAMES', ['columna2'])
                          ])

        self.assertTrue(self.hv._is_unreferenced_column_node(node, node[0]))

    def test_is_table(self):
        node = nltk.Tree('TABLE_REFERENCE',
                         [nltk.Tree('TABLE_NAMES', ['columna1']),
                          nltk.Tree('COLUMN_NAMES', ['columna2'])
                          ])

        self.assertTrue(self.hv._is_table(node, node[0]))
        self.assertFalse(self.hv._is_table(node, node[1]))

    def test__rename_orphan_column(self):
        pass

    def test__process_names(self):
        pass

    def test__rename_children(self):
        pass

    def test_rename_tree(self):
        pass

    def test_remove_comment(self):
        hv_parser = copy.copy(self.hv)
        line = hv_parser._remove_comment('query --comentario1')
        self.assertEquals(line, 'query ')
        line = hv_parser._remove_comment('--comentario2')
        self.assertEquals(line, '')
        comments = hv_parser.get_comments()
        self.assertEquals(comments[0], '--comentario1')
        self.assertEquals(comments[1], '--comentario2')

    def test_find_numbers(self):
        numbers = [' ' + str(e) + ' ' for e in self.hv._find_numbers('hola 14 como estas1 mil 1000')]
        self.assertEquals(len(numbers), 2)
        self.assertEquals(numbers[0], '  14  ')
        self.assertEquals(numbers[1], '  1000  ')

    def test_find_between(self):
        strings = self.hv._find_between("hola 'cadena1', estaba aqui siendo una 'cadena2'", "'", "'")
        self.assertEquals(len(strings), 2)
        self.assertEquals(strings[0], "'cadena1'")
        self.assertEquals(strings[1], "'cadena2'")

        vars = self.hv._find_between("hola ${hivevar:var1}, cosas {}", "${", "}")
        self.assertEquals(len(vars), 1)
        self.assertEquals(vars[0], "${hivevar:var1}")

    def test_clean_line(self):
        line = "SELECT 1, a FROM ${hivevar: db}.t1 WHERE a > '2013-06-01' AND b < 2"
        parsed = self.hv._clean_line(line)
        self.assertEqual(parsed, " SELECT #WORD# ,  A FROM  #WORD# .T1 WHERE A >  #WORD#  AND B < #WORD# ")

    def test_replace_var(self):
        hv_parser = copy.copy(self.hv)
        pattern = re.compile(r"a|b")
        new_line = pattern.sub(lambda m: hv_parser._Parser__replace_var(m, {'a': '_a_', 'b': '_b_'}), 'hola boris')
        self.assertEqual(new_line, 'hol_a_ _b_oris')
        words = hv_parser.get_words()
        self.assertEqual(words[0], 'a')
        self.assertEqual(words[1], 'b')

    def test_tokenize_vars(self):
        hv_parser = copy.copy(self.hv)
        line = " SELECT 1 ,  A FROM ${hivevar: db} .T1 WHERE A >  '2013-06-01'  AND B < 2 "
        clean_line = hv_parser._tokenize_vars(line, [" 1 ", " ${hivevar: db} ", " '2013-06-01' ", " 2 "])
        self.assertEquals(clean_line, " SELECT #WORD# ,  A FROM #WORD# .T1 WHERE A >  #WORD#  AND B < #WORD# ")
        words = hv_parser.get_words()
        self.assertEqual(words[0], " 1 ")
        self.assertEqual(words[1], " ${hivevar: db} ")
        self.assertEqual(words[2], " '2013-06-01' ")
        self.assertEqual(words[3], " 2 ")

    def test_untokenize(self):
        hv_parser = copy.copy(self.hv)
        line = " SELECT 1 ,  A FROM ${hivevar: db} .T1 WHERE A >  '2013-06-01'  AND B < 2 "
        clean_line = hv_parser._tokenize_vars(line, [" 1 ", " ${hivevar: db} ", " '2013-06-01' ", " 2 "])
        untokenized_line = hv_parser._untokenize(clean_line)
        self.assertEqual(re.sub(r' +', ' ', line), re.sub(r' +', ' ', untokenized_line))

    def test_rebuild_query(self):
        pass
