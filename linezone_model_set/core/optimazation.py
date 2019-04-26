#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Project : linezone_opff_model_1
# @Time    : 2019-03-04 09:42
# @Author  : redpoppet
# @Site    : localhost
# @File    : optimazation.py
# @Software: PyCharm

# import gurobipy
import pyscipopt


class Optimazation(object):
    def __init__(self, executor='scip'):
        self.executor = executor
        if executor == 'scip':
            self.m = pyscipopt.Model()
        elif executor == 'gurobi':
            self.m = gurobipy.Model()

    def setParam(self, *args, **kwargs):
        if self.executor == 'scip':
            self.m.setRealParam(*args, **kwargs)
            return True

        elif self.executor == 'gurobi':
            if 'limits/gap' in kwargs:
                kwargs['MIPGap'] = kwargs['limits/gap']
                kwargs.pop('limits/gap')

            if 'limits/time' in kwargs:
                kwargs['TimeLimit'] = kwargs['limits/time']
                kwargs.pop('limits/time')
            self.m.setParam(*args, **kwargs)
            return True

    def addVar(self, *args, **kwargs):
        if self.executor == 'scip':
            return self.m.addVar(*args, **kwargs)
        elif self.executor == 'gurobi':
            if 'lb' in kwargs:
                print('ddddd')
                if kwargs['lb'] is None:
                    kwargs['lb'] = -gurobipy.GRB.INFINITY
            else:
                kwargs['lb'] = 0
            if 'vtype' in kwargs:
                print('eeeeee')
                if kwargs['vtype'] == 'I':
                    print('match 1')
                    kwargs['vtype'] = gurobipy.GRB.INTEGER
                elif kwargs['vtype'] == 'C':
                    kwargs['vtype'] = gurobipy.GRB.CONTINUOUS
                elif kwargs['vtype'] == 'B':
                    print('match 2')
                    print()
                    kwargs['vtype'] = gurobipy.GRB.BINARY
                else:
                    print(kwargs['vtype'])
            print(kwargs)
            return self.m.addVar(*args, **kwargs)

    def update(self):
        if self.executor == 'scip':
            pass
            # raise NotImplementException()
        elif self.executor == 'gurobi':
            self.m.update()
            return True

    def setObjective(self, *args, **kwargs):
        if self.executor == 'scip':
            self.m.setObjective(*args, **kwargs)
            return True

        elif self.executor == 'gurobi':
            if 'clear' in kwargs:
                kwargs.pop('clear')
                self.m.setObjective(*args, **kwargs)
            else:
                self.m.setObjective(*args, **kwargs)

    def setMinimize(self, *args, **kwargs):
        if self.executor == 'scip':
            self.m.setMinimize(*args, **kwargs)
            return True
        elif self.executor == 'gurobi':
            pass

    def addCons(self, *args, **kwargs):
        if self.executor == 'scip':
            self.m.addCons(*args, **kwargs)

            return True
        elif self.executor == 'gurobi':
            self.m.addConstr(*args, **kwargs)
            return True

    def optimize(self):
        if self.executor == 'scip':
            self.m.optimize()
            return True
        elif self.executor == 'gurobi':
            self.m.optimize()
            return True

    def getStatus(self):
        if self.executor == 'scip':
            return self.m.getStatus()
        elif self.executor == 'gurobi':
            # raise NotImplementException()
            pass

    def getGap(self):
        if self.executor == 'scip':
            return self.m.getGap()

        elif self.executor == 'gurobi':
            pass
            # raise NotImplementException()

    def getAttr(self, *args, **kwargs):
        if self.executor == 'scip':
            pass
            # raise NotImplementException()
        elif self.executor == 'gurobi':
            return self.m.getAttr(*args, **kwargs)

    def getVal(self, *args, **kwargs):
        if self.executor == 'scip':
            return self.m.getVal(*args, **kwargs)
        elif self.executor == 'gurobi':
            pass
            # raise NotImplementException()

    def quicksum(self, *args, **kwargs):
        if self.executor == 'scip':
            return pyscipopt.quicksum(*args, **kwargs)
        elif self.executor == 'gurobi':
            return gurobipy.quicksum(*args, **kwargs)

    def max(self, *args, **kwargs):
        if self.executor == 'scip':
            return max(*args, **kwargs)
        elif self.executor == 'gurobi':
            return gurobipy.max_(*args, **kwargs)

    def LinExpr(self, *args, **kwargs):
        if self.executor == 'scip':
            pass
            # raise NotImplementException()
        elif self.executor == 'gurobi':
            return gurobipy.LinExpr(*args, **kwargs)

    def get_result(self, q):
        if self.executor == 'scip':
            m_status = self.getStatus()
            m_gap = self.getGap()
            # lz_logger.info('=' * 50 + ' ' + m_status + ' ' + '=' * 50)
            # lz_logger.info('-' * 30 + ' Gap: ' + str(m_gap * 100) + '% ' + '-' * 30)

            q_r = {}
            if m_status == 'optimal' or m_gap <= 0.05:
                for k in q:
                    q_r[k] = round(self.getVal(q[k]))
            return q_r
        else:
            return self.getAttr('x', q)
