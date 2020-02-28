from .ots import OTS
from serializer.ots_ser import DictSerializer
from utils import transaction

endpoint = ''
access_key_id = ''
access_key_secret = ''
instance_name = ''
ots = OTS(endpoint, access_key_id, access_key_secret, instance_name)

'''单条操作'''
# 插入 (如果列名有为关键字 使用data_put方法)
# ots.table.put(pk={'pk1': 1},name='john').response
#
# ots.table.data_put(pk, data={'name': 'john'}).response

# 查询
# ots.table.get(pk={'pk1': 1}, columns=['name']).response

# 更新 (如果列名有为关键字 使用update_delete方法)
# ots.table.update(pk={'pk1': 1}, name='swz').response

# 更新+删除值
# ots.table.update_delete(
#       pk={'pk1': 1},
#       put={'name': 'john', 'name1': 'john1'},
#       delete_all=['name']
# ).response

# 删除
# ots.table.delete(pk={'pk1': 1}).response

# 查询后更新或删除
# ots.table.get(pk, columns=['name']).update_delete(delete_all=['name'])


'''多行操作'''
# 查询
# ots.table.get(
#     pk=[{'pk1': 1}],
#     cond_mark='and',
#     cond={'E':{'name':'sw','name1':'swz'}},
#     columns=['name']
# ).response

# 主键范围查询
# ots.table.get_range(
#     pk={'pk1': [1, 7]},
#     cond_mark='and',
#     cond={'E':{'name':'sw','name1':'swz'}}
# ).response

# 插入
# ots.table.put(
#     pk=[{'pk1': 1}],
#     name='john',
#     name1='john1'
# ).response
#
# ots.table.data_put(
#     pk=[{'pk1': 1}],
#     data={'name': 'john', 'name1': 'john1'}
# ).response

# 更新
# ots.table.update(
#   pk=[{'pk1': 1}],
#   name='swz'
# ).response

# 更新+删除值
# ots.table.update_delete(
#   pk=[{'pk1': 1}],
#   put={'name': 'john', 'name1': 'john1'}
# ).response

# 删除
# ots.delete(
#     pk=[{'pk1': 1}]
# ).response


'''更新条件工具'''
# 只在update操作中生效
# F:
#     ots.table.update(pk=pk, price=F('price')+1)     F('price')+1 值price为数字 在price字段原有值上+1  + - * /
#
# AP: list append
#     ots.table.update(pk=pk, price=AP('list')+'value1')   AP('list')+'value1'  值list为数组 在数组中append value1
#
# EXT: list extend
#     ots.table.update(pk=pk, price=EXT('list')+['value1'])  EXT('list')+['value1']  值list为数组  在list中extend列表['value1']
#
# ITEM: dice setitem
#     ots.table.update(pk=pk, price=ITEM('list')['k', 'v'])  ITEM('list')['k', 'v']  值list为json 值list中做list['k'] = 'v'操作


'''多元索引查询'''
# 分页获取
# ots.table1.index_search(
#     index_type='wildcard',    # 索引查询类型
#     value=['name', 's*'],     # 索引查询规则
#     index_name='index2'       # 索引名默认为default
#     offset=offset,
#     limit=limit,
#     next_token=next_token
#     sort_mode='field',                                      # 排序方式
#     sort={'field_name': 'field', 'sort_order': 'desc'},     # 排序规则
# ).response

# 获取全部
# ots.table1.index_search(
#     index_type='wildcard',
#     value=['name', 's*'],
# ).all

# 获取第一条记录
# ots.table1.index_search(
#     index_type='wildcard',
#     value={'field_name':'name', 'value':'s*'},     # 列表对应位置参数 字典对应关键字参数
# ).first

# 多条件查询
# must = [
#     {'type': 'range',  'value': ['real_fee', 3, 99, True]},
#     {'type': 'bool_query', 'must':[{'type':'wildcard','value':['orderid', 'B*99*']}]}
# ]
# must_not = []
# should = []
# min_num = None
# ots.table.index_search(
#       index_type='bool_query',
#       must=must,
#       must_not=must_not,
#       should=should,
#       min_num=min_num,
#       columns=['orderid', 'real_fee']
# ).response


'''序列化'''
# must = [
#     {'type': 'term', 'value': ['agent_id', '0098911012f8ca23e8a325f557145d05']},
#     {'type': 'term', 'value': ['admin', True]}
# ]
# agent_obj = ots.agent_rela.index_search(
#     index_type='bool_query',
#     must=must,
# ).response

# agent_ser = DictSerializer(           # 序列化response对象
#     agent_obj,
#     field={'card_id': 'cardid'}       # 修改获取结果的列名称
# )
# data = agent_ser.data                 # 获取结果

# get_表名 快捷get查询并生成序列化对象
# user_obj = agent_ser.get_user(pk={'uid': uid}, columns=['wx_nickname', 'wx_avatar', 'ali_nickname', 'ali_avatar'])
# user_data = user_obj.data        # 获取结果
# user_obj.delete()     # 快速删除/更新或删除值

# search_表名 快捷多元索引查询
# user_data = agent_ser.search_user(
#                     index_type='bool_query',
#                     must=must,
#                     many=False,
#                     columns=[]
#                 )


'''事务'''
# box = ots.user
# pk1 = {'uid': '1'}
# pk2 = {'uid': '10'}
#
# with transaction.start(box) as t:                                 # 传入ots表对象会自动获取table_name 只能操作单表
#     r1 = box.put(pk=pk1, name='john', tid=t.tid(pk1))             # 方法参数tid(需要传入表分片主键)(每个表只有一个分片键)获取事务id
#     r2 = box.put(pk=pk2, name='john', tid=t.tid(pk2))
#     t.status = r1.status and r2.status                            # 需要确认是否成功
#
#
# with transaction.start(ots) as t:
#     t.status = ots.agent_account.put(
#         pk={'account': account},
#         password=password,
#         tid=t.tid(key={'account': account}, table='agent_account')    # 直接传入ots对象需要在tid方法中传入表名 可以多表操作
#     ).status


# 序列化事务
# agent_obj = ots.agent_rela.index_search(
#     index_type='bool_query',
#     must=must,
# ).response
# agent_ser = DictSerializer(agent_obj)

# with transaction.start(ots) as t:
#     user_obj = agent_ser.get_user({'uid': '1'})
#     t.status = user_obj.update(username='swz', tid=t.tid({'uid': '1'}, 'user')).status
