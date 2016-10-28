# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

class Column(object):
    def __init__(self, name, type, kind='regular', order=None, position=-1):
        self.name = name
        self.type = type
        self.kind = kind
        self.order = order
        self.position = position

    def is_pk(self):
        return self.kind == 'partition_key'

    def is_clustering(self):
        return self.kind == 'clustering'

    def dump_cql(self):
        return '{} {}'.format(self.name, self.type.upper())

    def __eq__(self, other):
        if not isinstance(other, Column):
            return False
        return (self.name == other.name and
                self.type.lower() == other.type.lower() and
                self.kind == other.kind and
                self.order == other.order and
                self.position == other.position)

    def __ne__(self, other):
        return not self.__eq__(other)


class Table(object):
    def __init__(self, name, columns=[]):
        self.name = name
        self.columns = columns

    def primary_keys(self):
        pk = {}
        for c in self.columns:
            if c.is_pk():
                pk[c.position] = c.name
        r = []
        for i in sorted(pk):
            r.append(pk[i])
        return r

    def clustering_columns(self):
        cl = {}
        for c in self.columns:
            if c.is_clustering():
                cl[c.position] = c.name
        r = []
        for i in sorted(cl):
            r.append(cl[i])
        return r

    def get_column(self, name):
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def dump_cql(self):
        cql = ['CREATE TABLE {} ('.format(self.name)]
        cql.append('\t' + ',\n\t'.join([x.dump_cql() for x in self.columns]) + ",")
        pk = self.primary_keys()
        pks = ('({})' if len(pk) > 1 else '{}').format(', '.join(pk))
        cc = self.clustering_columns()
        clustering = ', ' + ', '.join(cc) if cc else ''
        cql.append('\tPRIMARY KEY ({}{})'.format(pks, clustering))
        cql.append(')')
        return '\n'.join(cql)

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False
        if len(self.columns) != len(other.columns):
            return False
        for col in self.columns:
            other_col = other.get_column(col.name)
            if not other_col:
                return False
            if col != other_col:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def drop_cql(self):
        return 'DROP TABLE "{}"'.format(self.name)

    def drop_column_cql(self, column):
        return 'ALTER TABLE "{}" DROP "{}"'.format(self.name, column.name)

    def add_column_cql(self, column):
        return 'ALTER TABLE "{}" ADD "{}" {}'.format(self.name, column.name, column.type)

    def alter_column_type_cql(self, name, type):
        return 'ALTER TABLE "{}" ALTER "{}" TYPE {}'.format(self.name, name, type)


class Keyspace(object):
    def __init__(self, name, tables=[]):
        self.name = name
        self.tables = tables

    def get_table(self, name):
        for t in self.tables:
            if t.name == name:
                return t
        return None