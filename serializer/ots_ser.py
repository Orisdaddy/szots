"""
deposit_obj = ots.deposit.get(
    {'dp_orderid': dp_order_id},
    columns=[
        'auth_no',
        'biz_type',
        'createtime',
        'freeze_req_no',
        'dp_type',
        'prepay_id',
        'operation_id',
        'ali_id',
        'status',
        'uid',
    ]
).response

deposit_ser = DictSerializer(deposit_obj)
deposit_data: dict = deposit_ser.data

user_data = deposit_ser.get_user({'uid': uid}, ['wx_nickname', 'wx_avatar', 'ali_nickname', 'ali_avatar'])


deposit_ser = JsonSerializer(deposit_obj)
deposit_data = deposit_ser.data

user_data = deposit_ser.get_user({'uid': uid}, ['wx_nickname', 'wx_avatar', 'ali_nickname', 'ali_avatar'])
"""
from functools import partial
from collections import Iterator
import json


class Query:
    def __init__(self, ots, table, pk, data):
        self.ots = ots
        self.table = table
        self.pk = pk
        self.data = data

    def update(self, tid=None, **kwargs):
        res = self.ots.update(table_name=self.table, pk=self.pk, tid=tid, **kwargs).response
        return res

    def update_delete(self, put={}, delete={}, delete_all=[], tid=None):
        res = self.ots.update_delete(
            table_name=self.table, pk=self.pk, put=put, delete=delete, delete_all=delete_all, tid=tid
        ).response
        return res

    def delete(self, tid=None):
        res = self.ots.delete(table_name=self.table, pk=self.pk, tid=tid).response
        return res


class BasicSerializer:
    def __init__(self, response, field=None):
        self._response = response
        self.status = response.status
        self.ots = response.ots
        self.field = field

    def parse(self, response):
        raise NotImplementedError('.parse() must be defined')

    @property
    def data(self):
        res = self.parse(self._response)
        return res

    def __get(self, table, pk, columns):
        # get_表名 快捷查询ots

        res = self.ots.get(table_name=table, pk=pk, columns=columns).response

        if res.status is False:
            raise ValueError(
                '%s error search, %s' %
                (table, res)
            )
        elif res.status and res.data:
            return Query(
                ots=self.ots,
                table=table,
                pk=pk,
                data=self.parse(res)
            )
        else:
            return {}

    def __search(
            self, table, index_type, many=10, value=None, must=[], must_not=[],
            should=[], min_num=None, index_name='default', columns=None,
    ):

        if many is True:
            res = self.ots.index_search(
                table_name=table, index_type=index_type, value=value, must=must, must_not=must_not,
                should=should, min_num=min_num, index_name=index_name, columns=columns
            ).all
        elif many is False:
            res = self.ots.index_search(
                table_name=table, index_type=index_type, value=value, must=must, must_not=must_not,
                limit=1, should=should, min_num=min_num, index_name=index_name, columns=columns
            ).first
        elif isinstance(many, int):
            res = self.ots.index_search(
                table_name=table, index_type=index_type, value=value, must=must, must_not=must_not,
                limit=many, should=should, min_num=min_num, index_name=index_name, columns=columns
            ).response
        else:
            return

        if res.status is False:
            raise ValueError(
                '%s error search, %s' %
                (table, res)
            )
        elif res.status and res.data:
            return self.parse(res)
        else:
            return {}

    def __getattr__(self, item: str):
        if item.startswith('get_'):
            table = item[4:]
            return partial(self.__get, table)
        elif item.startswith('search_'):
            table = item[7:]
            return partial(self.__search, table)


class DictSerializer(BasicSerializer):
    def parse(self, response):
        if response.status is False:
            raise ValueError(
                'error search, %s' %
                response
            )

        if response.data:
            if isinstance(response.data, list) or isinstance(response.data, Iterator):
                parse_q = []
                for i in response.data:
                    parse_q.append(i.__dict__)
                data = parse_q
            else:
                data = response.data.__dict__

            if self.field:
                if isinstance(data, list):
                    for i, d in enumerate(data[:]):
                        for k, v in self.field.items():
                            value = d.pop(v, None)
                            if value:
                                d[k] = value
                            data[i] = d
                else:
                    for k, v in self.field.items():
                        value = data.pop(v, None)
                        if value:
                            data[k] = value
            return data
        else:
            return {}


class JSONSerializer(BasicSerializer):
    """
    OTSResponse object to JSON
    """

    def parse(self, response):
        if response.status is False:
            raise ValueError(
                'error search, %s' %
                response
            )

        if response.data:
            if isinstance(response.data, list) or isinstance(response.data, Iterator):
                parse_q = []
                for i in response.data:
                    parse_q.append(i.__dict__)
                data = parse_q
            else:
                data = response.data.__dict__

            if self.field:
                if isinstance(data, list):
                    for i, d in enumerate(data[:]):
                        for k, v in self.field.items():
                            value = d.pop(v, None)
                            if value:
                                d[k] = value
                            data[i] = d
                else:
                    for k, v in self.field.items():
                        value = data.pop(v, None)
                        if value:
                            data[k] = value
            return json.dumps(data, ensure_ascii=False)
        else:
            return '{}'


class XmlSerializer(BasicSerializer):

    def parse(self, response):
        pass


class CsvSerializer(BasicSerializer):

    def parse(self, response):
        pass


class DataFrameSerializer(BasicSerializer):

    def parse(self, response):
        pass


class YamlSerializer(BasicSerializer):

    def parse(self, response):
        pass
