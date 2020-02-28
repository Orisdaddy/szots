from tablestore import *
from functools import wraps
from exception import *
from utils.response import OTSResponse, Data
from utils.query import *
import json


def add_ots_error(func):
    '''收集客户端/服务端错误'''

    @wraps(func)
    def inner(*args, **kwargs):
        response = OTSResponse()
        try:
            response = func(*args, **kwargs)
        except OTSClientError as e:  # 客户端错误
            response.status = False
            response.error_type = 'client'
            response.error_message = e.get_error_message()

        except OTSServiceError as e:  # 服务器错误
            response.status = False
            response.error_type = 'server'
            response.error_message = e.get_error_message()
            response.error_code = e.get_error_code()
            response.request_id = e.get_request_id()

        except Exception as e:  # 代码内部错误
            response.status = False
            response.error_message = repr(e)
            response.error_type = 'inner'
            error_line = []
            trace = e.__traceback__
            while trace:
                error_line.append('%s line %s' % (trace.tb_frame.f_code.co_filename, trace.tb_lineno))
                trace = trace.tb_next
            response.error_line = error_line

        finally:  # 返回结果
            return response

    return inner


class OTS:
    '''
    阿里云表格存储

    pk={'pk1': 1}
    用法1:
    ots = OTS(endpoint, access_key_id, access_key_secret, instance_name)
    result = ots.table_name.get(pk).response
    result = ots.table_name.get(pk).delete()

    用法2:
    ots = OTS(endpoint, access_key_id, access_key_secret, instance_name)
    result = ots.get(pk,table_name=table_name).response
    ots.delete(pk,table_name=table_name)

    '''
    # 命令逻辑运算
    __conditions = {'AND': 1, 'OR': 2, 'NOT': 3}

    # 命令对应运算符 E:== NE:!= GT:> GTE:>= LT:< LTE:<=
    __condition_mark = {'E': 0, 'NE': 1, 'GT': 2, 'GTE': 3, 'LT': 4, 'LTE': 5}

    def __init__(self, endpoint, access_key_id, access_key_secret, instance_name, **kwargs):
        '''
        :param endpoint: "https:/(实例名).cn-hangzhou.ots.aliyuncs.com"   杭州公网
        :param access_key_id:      阿里ak
        :param access_key_secret:   阿里ak
        :param instance_name: 实例名字
        :param sort_mode:排序方式
        :param sort: 排序参数
        :param limit:
        '''
        self.__pk = None
        self.__table_name = kwargs.get('table_name')  # 实例化时 不要给table_name
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.instance_name = instance_name
        self.__client = OTSClient(
            end_point=endpoint,
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            instance_name=instance_name
        )
        self.ots_response = OTSResponse()
        self.ots_response.ots = self
        if not self.__table_name:
            for table in self.table_list:
                # ORM用法 ==> ots.table_name.put(pk,value)
                setattr(self, table, OTS(endpoint, access_key_id, access_key_secret, instance_name, table_name=table))

    def get_pk(self):
        describe_response = self.__client.describe_table(self.__table_name)
        return [i[0] for i in describe_response.table_meta.schema_of_primary_key]

    def get_table_name(self):
        return self.__table_name

    @property
    def table_list(self):
        '''所有表名'''
        return self.__client.list_table()

    @property
    def status(self):
        return self.ots_response.status

    @property
    def response(self):
        '''获取结果'''
        return self.ots_response

    @property
    def first(self):
        """获取第一条"""
        if isinstance(self.ots_response.data, list) and self.ots_response.data:
            self.ots_response.data = self.ots_response.data[0]
        return self.ots_response

    @property
    def all(self):
        """不分页获取全部"""
        table_name = self.all_param.get('table_name')
        index_name = self.all_param.get('index_name')
        index_type = self.all_param.get('index_type')
        columns = self.all_param.get('columns')

        if index_type == 'bool_query':
            bool_param = self.bool_param
            value = {}
        else:
            value = self.__value
            bool_param = {}
        next_token = self.ots_response.next_token.encode()
        ots = OTS(
            self.endpoint,
            self.access_key_id,
            self.access_key_secret,
            self.instance_name
        )
        while next_token:
            res = ots.index_search(
                index_type=index_type,
                table_name=table_name,
                index_name=index_name,
                limit=100,
                columns=columns,
                value=value,
                next_token=next_token,
                **bool_param
            ).response
            next_token = res.next_token
            if next_token:
                next_token = next_token
            else:
                next_token = None
            data = res.data
            self.ots_response.next_token = next_token
            self.ots_response.data.extend(data)
        self.ots_response.data = self.ots_response.data.__iter__()
        return self.ots_response

    @add_ots_error
    def put(self, pk, tid=None, **kwargs):
        """
        插入
        :param pk: 单条:{'pk01':'1'}  多条:[{'pk01':'1'}]
        :param tid: 事务id
        :param kwargs: table_name与属性
        :return:
        """
        table_name = kwargs.pop('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        if isinstance(pk, dict):
            primary_key = [(k, v) for k, v in pk.items()]
            data = [(key, value) for key, value in kwargs.items()]
            row = Row(primary_key, data)
            condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)

            self.__client.put_row(table_name, row, condition, transaction_id=tid)
            return self
        elif isinstance(pk, list):
            put_row_items = []
            for i in pk:
                primary_key = [(k, v) for k, v in i.items()]
                attribute_columns = [(k, v) for k, v in kwargs.items()]
                row = Row(primary_key, attribute_columns)
                condition = Condition(RowExistenceExpectation.IGNORE)
                item = PutRowItem(row, condition)
                put_row_items.append(item)

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, put_row_items))
            if tid:
                request.set_transaction_id(tid)

            result = self.__client.batch_write_row(request)

            return self.__update_response(result)

    @add_ots_error
    def data_put(self, pk, data, tid=None, **kwargs):
        """
        插入data
        :param pk: 单条:{'pk01':'1'}  多条:[{'pk01':'1'}]
        :param data: {'name':'john','name1':'john1'}
        :param tid: 事务id
        :param kwargs: table_name参数形式优先级较高
        :return:
        """
        table_name = kwargs.get('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        if isinstance(pk, dict):
            primary_key = [(k, v) for k, v in pk.items()]
            data = [(key, value) for key, value in data.items()]
            row = Row(primary_key, data)
            condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)

            self.__client.put_row(table_name, row, condition, transaction_id=tid)
            return self
        elif isinstance(pk, list):
            put_row_items = []
            for i in pk:
                primary_key = [(k, v) for k, v in i.items()]
                attribute_columns = [(k, v) for k, v in data.items()]
                row = Row(primary_key, attribute_columns)
                condition = Condition(RowExistenceExpectation.IGNORE)
                item = PutRowItem(row, condition)
                put_row_items.append(item)

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, put_row_items))
            if tid:
                request.set_transaction_id(tid)

            result = self.__client.batch_write_row(request)

            return self.__update_response(result)

    @add_ots_error
    def get(self, pk, cond_mark=None, cond=None, columns=[], **kwargs):
        """
        查询
        :param pk: 单条:{'pk01':'1'}  多条:[{'pk01':'1'}]
        :param cond_mark: 多条件之间的逻辑关系 or/and/not 默认AND  (多条)
        :param cond: {'GT':{'num':1},'LT':{'num':10},'NE':{'num':[3,5]}}  (多条)
                #命令对应运算符 E:== NE:!= GT:> GE:>= LT:< LE:<=
        :param columns:(可选获取字段) ['name','name1']   默认获取所有字段
        :param kwargs: table_name
        :return:
        """
        self.__pk = pk
        table_name = kwargs.get('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        if isinstance(pk, dict):
            primary_key = [(k, v) for k, v in pk.items()]
            columns_to_get = columns
            # go get
            consumed, return_row, next_token = self.__client.get_row(table_name, primary_key, columns_to_get)
            if return_row:
                row_result = return_row.attribute_columns
                data = Data()
                for row in row_result:
                    data[row[0]] = row[1]
                for k, v in pk.items():
                    data[k] = v
                self.ots_response.data = data
            return self
        elif isinstance(pk, list):
            # 主键与范围
            rows_to_get = []
            for v in pk:
                primary_key = [(pk, num) for pk, num in v.items()]
                rows_to_get.append(primary_key)

            # 条件逻辑运算
            try:
                condition_mark = cond_mark.upper()
            except:
                condition_mark = LogicalOperator.AND

            if condition_mark in self.__conditions:
                logical = self.__conditions[condition_mark]
            else:
                logical = LogicalOperator.AND

            # 生成条件
            condition = None
            if cond:
                condition = CompositeColumnCondition(logical)

                for mark, cond_v in cond.items():
                    for k, v in cond_v.items():
                        if isinstance(v, list) or isinstance(v, tuple):
                            for i in v:
                                condition.add_sub_condition(
                                    SingleColumnCondition(k, i, self.__condition_mark[mark.upper()]))
                        else:
                            condition.add_sub_condition(
                                SingleColumnCondition(k, v, self.__condition_mark[mark.upper()]))
                if len(condition.sub_conditions) == 1:
                    condition = condition.sub_conditions[0]

            request = BatchGetRowRequest()

            request.add(TableInBatchGetRowItem(table_name, rows_to_get, columns, condition,
                                               max_version=1))

            # try: 执行
            result = self.__client.batch_get_row(request)

            self.ots_response.data = {}
            self.ots_response.error_code = {}
            self.ots_response.error_message = {}
            # 处理结果
            for table, results in result.items.items():
                for result in results:
                    self.ots_response.data = []
                    if result.is_ok:
                        if result.row:
                            attr = result.row.attribute_columns
                            primary_key = result.row.primary_key
                            data = Data()
                            for i in primary_key:
                                data[i[0]] = i[1]
                            for i in attr:
                                data[i[0]] = i[1]
                            self.ots_response.data.append(data)
                        else:
                            self.ots_response.data.append({})
                    else:
                        self.ots_response.status = False
                        self.ots_response.error_code = result.error_code
                        self.ots_response.error_message = result.error_message
            return self

    @add_ots_error
    def update_delete(self, pk=None, put={}, delete={}, delete_all=[], cond={}, tid=None, **kwargs):
        """
        更新与删除值
        :param pk: 单条:{'pk01':'1'}  多条:[{'pk01':'1'}]
        :param put: {'name':'David','address':'Hongkong'}
        :param delete: {'address':'hongkong'}
        :param delete_all: ['mobile','age']
        :param cond: {'E':{'name':'john'}}   (多条)
        :param tid: 事务id
        :param kwargs: table_name
        :return:
        """
        if not pk:
            pk = self.__pk
            if not pk:
                raise FieldNotFoundError('Missing primary key')
            else:
                self.__pk = None
        table_name = kwargs.get('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        if isinstance(pk, dict):
            primary_key = [(k, v) for k, v in pk.items()]
            update_context = {}
            if put:
                update_context['PUT'] = [(k, v) for k, v in put.items()]
            if delete:
                update_context['DELETE'] = [(k, None, v) for k, v in delete.items()]
            if delete_all:
                update_context['DELETE_ALL'] = [(v) for v in delete_all]

            row = Row(primary_key, update_context)
            condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
            # go
            self.__client.update_row(table_name, row, condition, transaction_id=tid)
            return self
        elif isinstance(pk, list):
            update_row_items = []
            for i in pk:
                primary_key = [(k, v) for k, v in i.items()]
                attribute_columns = {}
                if put:
                    attribute_columns['put'] = [(k, v) for k, v in put.items()]
                if delete:
                    attribute_columns['delete'] = [(k, None, v) for k, v in delete.items()]
                if delete_all:
                    attribute_columns['delete_all'] = [(v) for v in delete_all]
                row = Row(primary_key, attribute_columns)
                condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
                for mark, c in cond.items():
                    condition = Condition(RowExistenceExpectation.EXPECT_EXIST,
                                          SingleColumnCondition(list(c.keys())[0], list(c.values())[0],
                                                                self.__condition_mark[mark.upper()]))
                item = UpdateRowItem(row, condition)
                update_row_items.append(item)

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, update_row_items))
            if tid:
                request.set_transaction_id(tid)

            result = self.__client.batch_write_row(request)

            return self.__update_response(result)

    @add_ots_error
    def update(self, pk, cond={}, tid=None, **kwargs):
        """
        更新
        :param pk: 单条:{'pk01':'1'}  多条:[{'pk01':'1'}]
        :param cond: {'E':{'name':'john'}}  (多条)
        :param tid: 事务id
        :param kwargs: 属性
        :return:
        """
        if not pk:
            pk = self.__pk
            if not pk:
                raise FieldNotFoundError('Missing primary key')
            else:
                self.__pk = None
        table_name = kwargs.pop('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        if isinstance(pk, dict):
            primary_key = [(k, v) for k, v in pk.items()]
            update_context = {'PUT': []}
            for k, v in kwargs.items():
                if isinstance(v, F):
                    value = int(self.get(pk=pk, columns=[v.field]).response['data'][v.field])
                    value = eval(f'value {v.operator} {v.num}')
                    update_context['PUT'].append((k, value))
                elif isinstance(v, (AP, EXT, ITEM)):
                    value = json.loads(self.get(pk=pk, columns=[v.field]).response['data'][v.field])
                    if isinstance(v, AP):
                        value.append(v.value)
                    elif isinstance(v, EXT):
                        value.extend(v.value_list)
                    else:
                        value[v.key] = v.value
                    update_context['PUT'].append((k, json.dumps(value)))
                else:
                    update_context['PUT'].append((k, v))
            row = Row(primary_key, update_context)
            condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
            # go
            self.__client.update_row(table_name, row, condition, transaction_id=tid)
            return self
        elif isinstance(pk, list):
            update_row_items = []
            for i in pk:
                primary_key = [(k, v) for k, v in i.items()]
                attribute_columns = {'put': []}
                for k, v in kwargs.items():
                    if isinstance(v, F):
                        value = int(self.get(pk=pk, columns=[v.field]).response['data'][v.field])
                        value = eval(f'value {v.operator} {v.num}')
                        attribute_columns['put'].append((k, value))
                    elif isinstance(v, (AP, EXT, ITEM)):
                        value = json.loads(self.get(pk=pk, columns=[v.field]).response['data'][v.field])
                        if isinstance(v, AP):
                            value.append(v.value)
                        elif isinstance(v, EXT):
                            value.extend(v.value_list)
                        else:
                            value[v.key] = v.value
                        attribute_columns['put'].append((k, json.dumps(value)))
                    else:
                        attribute_columns['put'].append((k, v))
                row = Row(primary_key, attribute_columns)
                condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
                for mark, c in cond.items():
                    condition = Condition(RowExistenceExpectation.EXPECT_EXIST,
                                          SingleColumnCondition(list(c.keys())[0], list(c.values())[0],
                                                                self.__condition_mark[mark.upper()]))
                item = UpdateRowItem(row, condition)
                update_row_items.append(item)

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, update_row_items))
            if tid:
                request.set_transaction_id(tid)

            result = self.__client.batch_write_row(request)

            return self.__update_response(result)

    @add_ots_error
    def delete(self, pk=None, cond={}, tid=None, **kwargs):
        """
        删除
        :param 单条:{'pk01':'1'}  多条:[{'pk01':'1'}]
        :param cond: {'E':{'name':'john'}}  (多条)
        :param tid: 事务id
        :param kwargs: table_name
        :return:
        """
        if not pk:
            pk = self.__pk
            if not pk:
                raise FieldNotFoundError('Missing primary key')
            else:
                self.__pk = None
        table_name = kwargs.get('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        if isinstance(pk, dict):
            primary_key = [(k, v) for k, v in pk.items()]
            row = Row(primary_key)
            # go
            self.__client.delete_row(table_name, row, None, transaction_id=tid)
            return self
        elif isinstance(pk, list):
            delete_row_items = []
            for v in pk:
                primary_key = [(k, v) for k, v in v.items()]
                row = Row(primary_key)
                _condition = cond
                condition = Condition(RowExistenceExpectation.IGNORE)
                for mark, c in _condition.items():
                    condition = Condition(RowExistenceExpectation.IGNORE,
                                          SingleColumnCondition(list(c.keys())[0], list(c.values())[0],
                                                                self.__condition_mark[mark.upper()]))

                item = DeleteRowItem(row, condition)
                delete_row_items.append(item)

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, delete_row_items))
            if tid:
                request.set_transaction_id(tid)

            result = self.__client.batch_write_row(request)
            return self.__update_response(result)

    @add_ots_error
    def get_range(self, pk, direction='forward', cond_mark=None, cond=None, columns=[],
                  limit=None, **kwargs):
        '''
        范围读取数据
        pk:{'id':[0,10],'id1':[0,20]}

        condition_mark #多条件之间的逻辑关系 or/and/not 默认AND

        condition: {'GT':{'num':1},'LT':{'num':10},'NE':{'num':[3,5]}}

        direction: 顺序 forward正序 backward倒序

        :return:
        '''

        table_name = kwargs.get('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')

        # 正序倒序主键列顺序也要相反
        if direction.upper() == 'FORWARD':
            inclusive_start_primary_key = [(k, v[0]) for k, v in pk.items()]
            exclusive_end_primary_key = [(k, v[1]) for k, v in pk.items()]
        elif direction.upper() == 'BACKWARD':
            inclusive_start_primary_key = [(k, v[1]) for k, v in pk.items()]
            exclusive_end_primary_key = [(k, v[0]) for k, v in pk.items()]
        else:
            raise ParamError('Direction parameter error')
        # 设置过滤条件condition关系 参考get_list
        try:
            condition_mark = cond_mark.upper()
        except:
            condition_mark = LogicalOperator.AND

        if condition_mark in self.__conditions:
            logical = self.__conditions[condition_mark]
        else:
            logical = LogicalOperator.AND

        # 构建过滤条件
        condition = None
        if cond:
            condition = CompositeColumnCondition(logical)
            for mark, cond_v in cond.items():
                for k, v in cond_v.items():
                    if isinstance(v, (list, tuple)):
                        for i in v:
                            condition.add_sub_condition(
                                SingleColumnCondition(k, i, self.__condition_mark[mark.upper()]))
                    else:
                        condition.add_sub_condition(SingleColumnCondition(k, v, self.__condition_mark[mark.upper()]))
            if len(condition.sub_conditions) == 1:
                condition = condition.sub_conditions[0]

        # 执行get_range获取数据
        consumed, next_start_primary_key, row_list, next_token = self.__client.get_range(
            table_name, direction.upper(),
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns,
            limit,
            column_filter=condition,
            max_version=1,
        )

        all_rows = []
        all_rows.extend(row_list)
        # 继续执行直到next_start_primary_key为None 数据取完
        while next_start_primary_key is not None:
            inclusive_start_primary_key = next_start_primary_key
            consumed, next_start_primary_key, row_list, next_token = self.__client.get_range(
                table_name, direction.upper(),
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns, limit,
                column_filter=condition,
                max_version=1
            )
            all_rows.extend(row_list)

        # 处理数据
        self.ots_response.total_count = len(all_rows)
        self.ots_response.data = []
        for row in all_rows:
            if row:
                attr = row.attribute_columns
                primary_key = row.primary_key
                data = Data()
                for i in primary_key:
                    data[i[0]] = i[1]
                for i in attr:
                    data[i[0]] = i[1]
                self.ots_response.data.append(data)
        return self

    sample_index_type = {'prefix': PrefixQuery, 'wildcard': WildcardQuery, 'match': MatchQuery,
                         'matchphrase': MatchPhraseQuery, 'geoboundingbox': GeoBoundingBoxQuery,
                         'geodistance': GeoDistanceQuery, 'geopolygon': GeoPolygonQuery, 'term': TermQuery,
                         'terms': TermsQuery, 'range': RangeQuery, 'exist': ExistsQuery}

    complex_index_type = ['bool_query', 'all_query']

    @add_ots_error
    def index_search(self, index_type, index_name='default', **kwargs):
        '''
        索引查询
        :param table_name: 表名
        :param index_name: 索引名
        :param index_type: 索引类型
        :param limit:
        :param offset:
        :param next_token:
        :param columns: ['name','age'](可选获取字段) 不传默认获取所有字段  传[]不获取字段
        :param kwargs: 传参 value:(各方法需要的参数做成字典,不可多传多余参数)

        PrefixQuery:prefix ==> {'field_name':'name', 'prefix':'s'}
        WildcardQuery:wildcard ==> {'field_name':'name', 'value':'s*'}
        MatchQuery:match ==> {'field_name':'name', 'text':'s', 'minimum_should_match':1, 'operator':'操作'}
        MatchPhraseQuery:matchphrase ==> {'field_name':'name', 'text':'s'}
        GeoBoundingBoxQuery:geoboundingbox ==> {'field_name':'name', 'top_left':'20.1,110.1','bottom_right':'30.3,120.2'}
        GeoDistanceQuery:geodistance ==> {'field_name':'name', 'center_point':'20.1,110.1', 'distance':3000}
        GeoPolygonQuery:geopolygon ==> {'field_name':'name', 'points':['30.9,112.0', '30.5,115.0', '30.3, 117.0', '30.2,119.0']}
        TermQuery:term ==> {'field_name':'name', 'column_value':'swz'}
        TermsQuery:terms ==> {'field_name':'name', 'column_values': ['key000', 'key100', 'key888', 'key999']}
        RangeQuery:range ==> {'field_name':'name', 'range_from':'s', 'range_to':'z', 'include_lower':False, 'include_upper':True, }
        ExistsQuery:exist ==> {'field_name':'name'}
        value对应列表

        NestedQuery ==> {'field_name':'detail', 'term':{'field_name':'detail.name', 'column_value':'swz'}}
        # AllQuery ==> value=None

        :return:
        '''

        table_name = kwargs.pop('table_name', self.__table_name)
        if not table_name:
            raise FieldNotFoundError('Missing table_name parameters')
        sort = kwargs.pop('sort', None)
        sort_mode = kwargs.pop('sort_mode', None)
        if sort and sort_mode:
            sort = self.sort(sort, sort_mode)
        limit = kwargs.pop('limit', None)
        offset = kwargs.pop('offset', None)

        next_token = kwargs.pop('next_token', None)
        columns = kwargs.pop('columns', None)
        index_type_list = self.complex_index_type
        index_type_list.extend(self.sample_index_type.keys())
        if index_type in index_type_list:
            rows = []
            next_tokens = None
            total_count = None
            is_all_succeed = None
            if index_type in self.sample_index_type:
                value = kwargs.get('value')
                return_type = self.__return_type(columns)

                for i in self.sample_index_type:
                    if i == index_type:
                        if isinstance(value, list):
                            query = self.sample_index_type[i](*value)
                        else:
                            query = self.sample_index_type[i](**value)
                        rows, next_tokens, total_count, is_all_succeed = self.__client.search(
                            table_name, index_name,
                            SearchQuery(query, sort=sort, offset=offset, limit=limit, next_token=next_token,
                                        get_total_count=True),
                            ColumnsToGet(column_names=columns, return_type=return_type)
                        )
                        self.__value = value

            elif index_type in self.complex_index_type:
                for i in self.complex_index_type:
                    if i == index_type:
                        rows, next_tokens, total_count, is_all_succeed = getattr(self, index_type)(table_name,
                                                                                                   index_name,
                                                                                                   sort=sort,
                                                                                                   limit=limit,
                                                                                                   offset=offset,
                                                                                                   next_token=next_token,
                                                                                                   columns=columns,
                                                                                                   **kwargs)

            self.all_param = {'table_name': table_name, 'index_name': index_name, 'index_type': index_type,
                              'columns': columns}
            self.ots_response.data = []

            for row in rows:
                pk_rows = row[0]
                data_rows = row[1]
                data = Data()
                for pk_row in pk_rows:
                    data[pk_row[0]] = pk_row[1]
                for data_row in data_rows:
                    data[data_row[0]] = data_row[1]
                self.ots_response.data.append(data)
            self.ots_response.next_token = next_tokens
            self.ots_response.total_count = total_count
            self.ots_response.status = is_all_succeed
        else:
            raise ValueError('index type is not find')
        return self

    def all_query(self, table_name, index_name, sort=None, limit=None, next_token=None, offset=None, columns=None):
        '''查询所有值'''
        return_type = self.__return_type(columns)

        query = MatchAllQuery()

        rows, next_token, total_count, is_all_succeed = self.__client.search(
            table_name, index_name,
            SearchQuery(query, sort=sort, next_token=next_token,
                        limit=limit, offset=offset, get_total_count=True),
            columns_to_get=ColumnsToGet(
                columns,
                return_type))

        return rows, next_token, total_count, is_all_succeed

    def bool_query(self, table_name, index_name, sort=None, limit=None, next_token=None, offset=None, columns=None,
                   **kwargs):
        '''
        多条件索引
        :param table_name:
        :param index_name:
        :param limit:
        :param next_token:
        :param offset:
        :param columns:
        :param kwargs:可传
        must ==> must:[{'type': 'range',  'value': {'field_name':'real_fee','range_from':3,'range_to':99,'include_upper':True}},
                {'type': 'bool_query', 'must':[{'type':'wildcard','value':{'field_name':'orderid','value':'B*99*'}}]}]
                满足条件

        must_not  必须不满足
        should    最少满足min_num个条件
        min_num: int
        :return:
        '''
        # 必须满足 must
        must_queries = kwargs.get('must', [])
        # 必须不满足 must_not
        must_not_queries = kwargs.get('must_not', [])
        # 可以满足min_num个 should
        should_queries = kwargs.get('should', [])
        # 最少满足条件个数 min_num
        minimum_should_match = kwargs.get('min_num')
        self.bool_param = {
            'must': must_queries,
            'should': should_queries,
            'must_not': must_not_queries,
            'min_num': minimum_should_match
        }
        bool_condition_list = [must_queries, must_not_queries, should_queries]
        bool_query_list = [[], [], []]
        count = 0
        if should_queries and not minimum_should_match:
            minimum_should_match = 1
        # 创建三种类型的条件
        for queries in bool_condition_list:
            for query in queries:
                query_type = query['type']
                if query_type == 'bool_query':
                    # 多条件递归创建子条件
                    bool_query = self._bool_query(
                        must_queries=query.get('must', []),
                        must_not_queries=query.get('must_not', []),
                        should_queries=query.get('should', []),
                        minimum_should_match=query.get('min_num')
                    )
                    bool_query_list[count].append(bool_query)
                else:
                    value = query['value']
                    if isinstance(value, list):
                        bool_query_list[count].append(self.sample_index_type[query_type](*value))
                    else:
                        bool_query_list[count].append(self.sample_index_type[query_type](**value))
            count += 1
        must_list = bool_query_list[0]
        must_not_list = bool_query_list[1]
        should_list = bool_query_list[2]

        return_type = self.__return_type(columns)

        bool_query = BoolQuery(
            must_queries=must_list,
            must_not_queries=must_not_list,
            should_queries=should_list,
            minimum_should_match=minimum_should_match
        )
        # go
        rows, next_token, total_count, is_all_succeed = self.__client.search(
            table_name, index_name,
            SearchQuery(
                bool_query,
                sort=sort,
                offset=offset, limit=limit, next_token=next_token,
                get_total_count=True),
            ColumnsToGet(column_names=columns, return_type=return_type)
        )
        return rows, next_token, total_count, is_all_succeed

    def _bool_query(self, must_queries, must_not_queries, should_queries, minimum_should_match=None):
        '''BoolQuery递归创建子条件'''
        if should_queries and not minimum_should_match:
            minimum_should_match = 1
        bool_condition_list = [must_queries, must_not_queries, should_queries]
        bool_query_list = [[], [], []]
        count = 0
        for queries in bool_condition_list:
            for query in queries:
                query_type = query['type']
                if query_type == 'bool_query':
                    bool_query = self._bool_query(
                        must_queries=query.get('must', []),
                        must_not_queries=query.get('must_not', []),
                        should_queries=query.get('should', []),
                        minimum_should_match=query.get('min_num')
                    )
                    bool_query_list[count].append(bool_query)
                else:
                    value = query['value']
                    if isinstance(value, list):
                        bool_query_list[count].append(self.sample_index_type[query_type](*value))
                    else:
                        bool_query_list[count].append(self.sample_index_type[query_type](**value))
            count += 1
        must_list = bool_query_list[0]
        must_not_list = bool_query_list[1]
        should_list = bool_query_list[2]

        bool_query = BoolQuery(
            must_queries=must_list,
            must_not_queries=must_not_list,
            should_queries=should_list,
            minimum_should_match=minimum_should_match
        )
        return bool_query

    sort_mode_list = {'score': ScoreSort, 'primary_key': PrimaryKeySort, 'field': FieldSort,
                      'geodistance': GeoDistanceSort}
    sort_order_list = {'ASC': SortOrder.ASC, 'DESC': SortOrder.DESC}

    def sort(self, sort, sort_mode):
        '''
        排序
        sort_mode:四种排序方式 score  primary_key  field  geodistance
        sort:
        score_sort ==>  {'sort_order':'asc'}
        primary_key_sort ==> {'sort_order':'asc'}
        field_sort ==>  {'field_name':'name','sort_order':'desc'}
        geodistance ==>  {'field_name':'name','points':'20.1,110.5','sort_order':'asc'}
        :return:
        '''

        sort_list = []
        if sort:
            order_field = sort.pop('sort_order').upper()
            sort_order = self.sort_order_list[order_field]
            sort_list.append(self.sort_mode_list[sort_mode](sort_order=sort_order, **sort))
        return Sort(sorters=sort_list)

    @staticmethod
    def __return_type(columns):
        # 处理可选字段
        return_type = ColumnReturnType.ALL
        if columns:
            return_type = ColumnReturnType.SPECIFIED
        elif columns == []:
            return_type = ColumnReturnType.NONE
        return return_type

    def __update_response(self, result):
        self.ots_response.data = {}
        self.ots_response.error_code = {}
        self.ots_response.error_message = {}
        for table, results in result.table_of_update.items():
            for result in results:
                self.ots_response.data = []
                if not result.is_ok:
                    self.ots_response.status = False
                    self.ots_response.error_code = result.error_code
                    self.ots_response.error_message = result.error_message
        return self

    # def __getattr__(self, item):
    #     if item in self.sample_index_type:
    #         self.index_search()
