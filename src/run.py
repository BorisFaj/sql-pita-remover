#!/usr/bin/env python
# -*- coding: utf-8 -*-

import nltk
import re
from src.parser import Parser

query = 'SELECT t1.a,                              \
                t2.a t2_a,                         \
                t2.suma AS total,              \
                t3.c                               \
        FROM t1 alias_t1,                          \
            (SELECT a, tp_sub.b AS suma FROM  \
                (SELECT p FROM t2, t1) AS tp_sub       \
            ) AS t2,    \
            (SELECT b, a a_s FROM t4) AS sub_2          '


hv_parser = Parser('../conf/grammar')

print('query: {}'.format(re.sub(' +', ' ', query)))
tree = [e for e in hv_parser.parse_query(query)][0]
reverse_tree, di = hv_parser.get_nodes(tree)
print(di)
