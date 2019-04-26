# coding:utf-8
import pandas as pd

from config.settings import config
from exceptions.lz_exception import EmptyDataFrameException, ExistNullDataFrameException
from utils.logger import lz_logger


# class DataFrameValidator(object):
#     def __init__(self):
#         pass
#
#     @staticmethod
#     def process(data):
#         return data
#
#     @classmethod
#     def validate(cls, data, param_index, func_name):
#
#         lz_logger.info(
#             ' {param_index} is checking in {func_name}\n'.format(param_index=param_index, func_name=func_name))
#
#         if isinstance(data, pd.DataFrame):
#
#             if data.empty:
#                 lz_logger.critical(
#                     ' the {param_index}st  parameter is emtpy DataFrame in {func_name}'.format(
#                         param_index=param_index, func_name=func_name))
#                 if config.DEBUG:
#                     raise EmptyDataFrameException(func_name, data=data)
#                 if config.RELEASE:
#                     data = cls.process(data)
#                     return data
#             else:
#                 lz_logger.critical(
#                     'the {param_index}st parameter is not an emtpy DataFrame in {func_name}'
#                         .format(param_index=param_index, func_name=func_name))
#
#             for column in data.columns:
#                 if data[column].isna().any():
#                     lz_logger.critical(' {column}  in  parameter is null in {func_name}'
#                                        .format(column=column, func_name=func_name))
#                     if config.DEBUG:
#                         raise ExistNullDataFrameException(func_name, data=data)
#                     if config.RELEASE:
#                         data = cls.process(data)
#                         return data
#
#             lz_logger.info('the dtypes of data is \n{dtypes}\n '.format(dtypes=data.dtypes))
#             lz_logger.info('the head 5 line of data is \n{head_value} \n'.format(head_value=data[0:5]))
#         else:
#             lz_logger.critical(' one parameter is emtpy DataFrame in {func_name}'.format(
#                 func_name=func_name))
#
#         return data
#
#
# class DictValidator(object):
#     def __init__(self):
#         pass
#
#     @staticmethod
#     def validate(data):
#         if isinstance(data, dict):
#             print('ok')
#         else:
#             print('no ok')
#
#     @staticmethod
#     def process(data):
#         return data
#
#
# class DateValidator(object):
#     def __init__(self):
#         pass
#
#     @staticmethod
#     def validate(data):
#         pass
#
#
# class DatetimeValidator(object):
#     pass


class AbstractType:
    def __init__(self, data):
        self.data = data

    @property
    def processed_data(self):
        return self.data


class DataFrameTypeCheck(AbstractType):
    def __init__(self, data):
        super(DataFrameTypeCheck, self).__init__(data)
        print('i am a DataFrame type check validator')

    @property
    def processed_data(self):
        # TODO 此处可以防止过滤数据的过程
        return self.data

    def validate(self, bound_types_name, func_name):

        lz_logger.info(
            ' the parameter {bound_types_name} is checking in {func_name}'.format(bound_types_name=bound_types_name,
                                                                                  func_name=func_name))

        # TODO 校验DataFrame是否整体为空
        if self.data.empty:
            lz_logger.critical(' the  parameter {bound_types_name} is emtpy DataFrame in {func_name} '.format(
                bound_types_name=bound_types_name, func_name=func_name))
            if config.DEBUG:
                # raise EmptyDataFrameException(func_name, data=self.data)
                pass
            else:
                return self.processed_data

        else:
            lz_logger.critical('the parameter {bound_types_name} is not an emtpy DataFrame in {func_name}'.format(
                bound_types_name=bound_types_name, func_name=func_name))

        flag = True
        # TODO 校验DataFrame的某一列是否为Null
        for column in self.data.columns:
            print(column)
            if self.data[column].isna().any():
                lz_logger.critical(
                    ' {column}  in  parameter is null in {func_name}'.format(column=column, func_name=func_name))
                if config.DEBUG:
                    flag = False
        if flag:
            # raise ExistNullDataFrameException(func_name, data=self.data)
            pass
        else:
            return self.processed_data

        # TODO 输出DataFrame的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=self.data.dtypes))
        lz_logger.info('the head 5 line of data is \n{head_value} \n'.format(head_value=self.data[0:5]))

        return self.data


class DictTypeCheck(AbstractType):
    def __init__(self, data):
        super(DictTypeCheck, self).__init__(data)
        print('i am a dict type check validator')

    @property
    def processed_data(self):
        # TODO 此处可以添加过滤数据的过程
        return self.data

    def validate(self, bound_types_name, func_name):
        lz_logger.info(
            'the parameter {bound_types_name} is checking in {func_name} \n '.format(
                bound_types_name=bound_types_name, func_name=func_name))

        # TODO 输出Dict的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=type(self.data)))
        lz_logger.info('the head 5 line of data is \n{head_value} \n'.format(head_value=self.data))


class DateTypeCheck(AbstractType):
    def __init__(self, data):
        super(DateTypeCheck, self).__init__(data)
        print('i am a date type check validator')

    @property
    def processed_data(self):
        # TODO 此处可以添加过滤数据的过程
        return self.data

    def validate(self, bound_types_name, func_name):
        lz_logger.info('the parameter {bound_types_name} is checking in {func_name} \n '.format(
            bound_types_name=bound_types_name, func_name=func_name))
        # TODO 输出Date的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=type(self.data)))
        lz_logger.info('the data is \n{head_value} \n'.format(head_value=self.data))


class DatetimeTypeCheck(AbstractType):
    def __init__(self, data):
        super(DatetimeTypeCheck, self).__init__(data)
        print('i am a datetime type check validator')

    @property
    def processed_data(self):
        # TODO 此处可以添加过滤数据的过程
        return self.data

    def validate(self, bound_types_name, func_name):
        lz_logger.info('the parameter {bound_types_name} is checking in {func_name} \n '.format(
            bound_types_name=bound_types_name, func_name=func_name))
        # TODO 输出Dict的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=type(self.data)))
        lz_logger.info('the data is \n{head_value} \n'.format(head_value=self.data))


class DigitalTypeCheck(AbstractType):
    def __init__(self, data):
        super(DigitalTypeCheck, self).__init__(data)
        print('i am a digital type check validator')

    @property
    def processed_data(self):
        return self.data

    def validate(self, bound_types_name, func_name):
        lz_logger.info('the parameter {bound_types_name} is checking in {func_name} \n '.format(
            bound_types_name=bound_types_name, func_name=func_name))
        # TODO 输出Dict的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=type(self.data)))
        lz_logger.info('the data is \n{head_value} \n'.format(head_value=self.data))


class StringsTypeCheck(AbstractType):
    def __init__(self, data):
        super(StringsTypeCheck, self).__init__(data)
        print('i am a digital type check validator')

    @property
    def processed_data(self):
        return self.data

    def validate(self, bound_types_name, func_name):
        lz_logger.info('the parameter {bound_types_name} is checking in {func_name} \n '.format(
            bound_types_name=bound_types_name, func_name=func_name))
        # TODO 输出Dict的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=type(self.data)))
        lz_logger.info('the data is \n{head_value} \n'.format(head_value=self.data))


class ListTypeCheck(AbstractType):
    def __init__(self, data):
        super(ListTypeCheck, self).__init__(data)
        print('i am a list type check validator')

    @property
    def processed_data(self):
        return self.data

    def validate(self, bound_types_name, func_name):
        lz_logger.info('the parameter {bound_types_name} is checking in {func_name} \n '.format(
            bound_types_name=bound_types_name, func_name=func_name))
        # TODO 输出Dict的类型和数据长度
        lz_logger.info('the type of data is \n{dtypes}\n '.format(dtypes=type(self.data)))
        lz_logger.info('the data is \n{head_value} \n'.format(head_value=self.data))
