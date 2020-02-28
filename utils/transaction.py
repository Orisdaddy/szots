"""
事务

box = ots.user

pk1 = {'uid': '1'}
pk2 = {'uid': '10'}
value1 = {'name': 'john', 'name1': 'john1'}
value2 = {'name': 'john', 'name1': 'john1'}

with transaction.start(box) as t:                                 # 传入ots表对象会自动获取table_name 只能操作单表
    r1 = box.put(pk=pk1, data=value2, tid=t.tid(pk1)).response    # 方法参数tid(需要传入表分片主键)(每个表只有一个分片键)
    r2 = box.put(pk=pk2, data=value1, tid=t.tid(pk2)).response
    t.status = r1.status and r2.status                            # 需要确认是否成功


with transaction.start(ots) as t:
    agent_res = ots.agent_account.put(
        pk={'account': account},
        data={'password': password},
        tid=t.tid({'account': account}, table='agent_account')    # 直接传入ots对象需要在tid方法传入表名 可以多表操作
    ).response
    t.status = agent_res.status


user = ots.user
pk1 = {'uid': '1'}
pk2 = {'uid': '10'}
value1 = {'name': 'john', 'name1': 'john1'}
value2 = {'name': 'john', 'name1': 'john1'}
t = transaction.start(user)
try:
    r1 = user.put(pk=pk1, data=value2, tid=t.tid(pk1)).response
    r2 = user.put(pk=pk2, data=value1, tid=t.tid(pk2)).response
    t.status = r1.status and r2.status
except Exception as e:
    t.status = False
finally:
    t.commit()
"""


def start(client, table_name=None):
    return Start(client, table_name)


def _commit(client, tid_list):
    [client.commit_transaction(key) for key in tid_list]


def _rollback(client, tid_list):
    [client.abort_transaction(key) for key in tid_list]


class Start:
    def __init__(self, ots, table_name):
        self.client = ots._OTS__client

        if table_name is None:
            table_name = ots._OTS__table_name

        self.table_name = table_name
        self.tid_list = []
        self.__dict__['status'] = True

    def __setattr__(self, key, value):
        if key == 'status':
            if value is False:
                self.__dict__[key] = value
        else:
            self.__dict__[key] = value

    def tid(self, key, table=None):
        # 传入分区键
        key = [(k, v) for k, v in key.items()]
        table_name = table if table else self.table_name

        if not table_name:
            raise ValueError('A table must be specified for transactional operations')

        tid = self.client.start_local_transaction(table_name=self.table_name, key=key)
        self.tid_list.append(tid)
        return tid

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            _rollback(self.client, self.tid_list)
        else:

            if self.status is True:
                _commit(self.client, self.tid_list)
            else:
                _rollback(self.client, self.tid_list)

    def commit(self):
        if self.status is True:
            _commit(self.client, self.tid_list)
        else:
            _rollback(self.client, self.tid_list)

    def fcommit(self):
        _commit(self.client, self.tid_list)

    def rollback(self):
        _rollback(self.client, self.tid_list)
