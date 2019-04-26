#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Project : linezone_opff_model_1
# @Time    : 2019-02-23 15:38
# @Author  : redpoppet
# @Site    : localhost
# @File    : wraps.py.py
# @Software: PyCharm


def singleton(cls, *args, **kw):
    instance = {}

    def _singleton():
        if cls not in instance:
            instance[cls] = cls(*args, **kw)
        return instance[cls]

    return _singleton


@singleton
class A(object):
    def __init__(self):
        self.a = 3

    def add(self, a, b):
        return a + b


if __name__ == '__main__':
    x = A().add(1, 2)
    v = A()
    print(type(v))
    print(v.a)
    print(A().a)
    print(x)
