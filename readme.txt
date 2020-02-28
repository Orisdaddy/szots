    ---------------
    | 阿里云表格存储 |
    ---------------

删除不常用方法 优化返回值  使用方法查看demo.py
=================================================================================================================
常用方法:

索引条件查询value
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



排序sort
    score_sort ==>  {'sort_order':'asc'}
    primary_key_sort ==> {'sort_order':'asc'}
    field_sort ==>  {'field_name':'name','sort_order':'asc'}
    geodistance ==>  {'field_name':'name','points':'20.1,110.5','sort_order':'asc'}
==================================================================================================================

pk={'pk1': 1}
用法1.:
    ots = OTS(endpoint, access_key_id, access_key_secret, instance_name)
    result = ots.table_name.get(pk).response
    result = ots.table_name.get(pk).delete()

用法2:
    ots = OTS(endpoint, access_key_id, access_key_secret, instance_name)
    result = ots.get(pk,table_name=table_name).response
    ots.delete(pk,table_name=table_name)


-------------多元索引-------------

方法:'prefix', 'wildcard', 'bool_query', 'match', 'term', 'range' (常用)
'matchphrase', 'geoboundingbox',
'geodistance', 'geopolygon',
'terms', 'match_all_query',
'nested_query'

单条件
    value位置参数列表
search_obj = box.index_search(index_type='term', value=['active', True])

多条件
must = [
    {'type':'term','value':['active', True]},
    {'type':'term','value':['active', True]},
    {'type':'bool_query','must':[
          {'type':'term','value':['active', True]},
          {'type':'term','value':['active', True]}
    ]}
    构建子条件
]
search_obj = box.index_search(index_type='bool_query', must=must, must_not=must_not, should=should, min_num=1)

res = search_obj.all          获取所有值
res = search_obj.first        多条获取第一条
res = search_obj.response     获取limit条值



-----------------事务--------------------

box = ots.user

pk1 = {'uid': '1'}
pk2 = {'uid': '10'}
value1 = {'name': 'john', 'name1': 'john1'}
value2 = {'name': 'john', 'name1': 'john1'}

with transaction.start(box) as t:                                 # 传入ots表对象会自动获取table_name 只能操作单表
    r1 = box.put(pk=pk1, data=value2, tid=t.tid(pk1)).response    # 方法参数tid(需要传入表分片主键)(每个表只有一个分片键)
    t.status = r1.status                                          # 需要确认是否成功
    r2 = box.put(pk=pk2, data=value1, tid=t.tid(pk2)).response
    t.status = r2.status


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
    t.status = r1.status
    r2 = user.put(pk=pk2, data=value1, tid=t.tid(pk2)).response
    t.status = r2.status
except Exception as e:
    t.status = False
finally:
    t.commit()

-----------------序列化------------------

deposit_obj = ots.deposit.get(
    {'dp_orderid': dp_order_id},
    # columns控制返回列
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

生成字典
deposit_ser = DictSerializer(deposit_obj)     生成序列化对象
deposit_data: dict = deposit_ser.data

user_data: dict = deposit_ser.get_user(pk={'uid': uid}, columns=['wx_nickname', 'wx_avatar', 'ali_nickname', 'ali_avatar'])
    get_表名 快捷查询并生成序列化对象

user_data: dict = deposit_ser.search_user(
                    index_type='bool_query',
                    must=must,
                    many=False,
                    columns=[]
                )
    search_表名 快捷多元索引查询


生成json
deposit_ser = JsonSerializer(deposit_obj)
deposit_data: json = deposit_ser.data

user_data: json = deposit_ser.get_user(pk={'uid': uid}, columns=['wx_nickname', 'wx_avatar', 'ali_nickname', 'ali_avatar'])


---------------返回值---------------

正常返回
get:
    status   True                         状态
    data     {'pk':'1','name':'swz'}      数据

get_range
    status   True                         状态
    data     [{'pk':'1', 'name':'swz'}]   数据

index_search
    status   True                         状态
    data     [{'pk':'1', 'name':'swz'}]   数据

返回错误
    status             False              状态
    error_type                            错误发生端
    error_code                            错误码
    error_message                         错误信息
    error_line                            详细错误发生路径







