# coding:utf-8

from functools import wraps
from inspect import signature

import pandas as pd
import numpy as np
from config.settings import config
from check.Factory import ValidatorFactory
from utils.logger import lz_logger


def typeassert(*ty_args, **ty_kwargs):
    def decorate(func):
        if not config.DEBUG:
            return func

        # Map function argument names to supplied types
        sig = signature(func)
        bound_types = sig.bind_partial(*ty_args, **ty_kwargs).arguments

        @wraps(func)
        def wrapper(*args, **kwargs):
            lz_logger.info('======{func_name} check starts===========\n'.format(func_name=func.__name__))
            bound_values = sig.bind(*args, **kwargs)
            # Enforce type assertions across supplied arguments
            for name, value in bound_values.arguments.items():
                if name in bound_types:
                    if not isinstance(value, bound_types[name]):
                        raise TypeError('Argument {} must be {}'.format(name, bound_types[name]))
                    else:
                        print('name', bound_types[name])
                        # print(type(bound_types[name]))
                        # print(bound_types[name]())
                        validator = ValidatorFactory(value).validator
                        validator.validate(name, func.__name__)

            lz_logger.info('========{func_name} check ends========\n'.format(func_name=func.__name__))
            return func(*args, **kwargs)

        return wrapper

    return decorate


@typeassert(x=pd.DataFrame)
def spam2(x):
    pass


@typeassert(x=dict)
def spam3(x):
    pass


@typeassert(x=pd.DataFrame, y=dict)
def spam4(x, y):
    pass


if __name__ == '__main__':
    # spam2(pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, np.nan]}))
    # spam3({"A": 1})
    spam4(pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}), {'A': 1})
