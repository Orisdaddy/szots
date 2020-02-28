from logging import Logger

# import sys
# sys.setrecursionlimit(3000)

'''
printobj(obj, depth=0)
打印对象 obj对象 depth层数

'''


def get_obj_info(obj, depth):
    data_type = (str, int, float, bytes, set, bool, Logger, frozenset)
    if isinstance(obj, list) or isinstance(obj, tuple):
        if isinstance(obj, tuple):
            obj = list(obj)
        if depth == 0:
            return obj
        for i, v in enumerate(obj):
            if type(v) in data_type:
                continue
            else:
                obj[i] = get_obj_info(v, depth=depth - 1)
        return obj
    else:
        if isinstance(obj, dict):
            res = obj
        else:
            if hasattr(obj, '__dict__'):
                res = obj.__dict__
            else:
                return obj
        if depth == 0:
            return res
        else:
            for k, v in res.items():
                if type(v) in data_type or v is None:
                    continue
                else:
                    try:
                        res[k] = get_obj_info(v, depth=depth - 1)
                    except:
                        return res
            return res


def bprint(d, n=1, is_list=False):
    if isinstance(d, dict):
        if is_list:
            print('\t' * (n - 1), '{')
        else:
            print('{')
        for k, v in d.items():
            print('\t' * n, end='')
            if isinstance(v, dict):
                if not v:
                    print('"%s" : {}' % k)
                    continue
                print('"%s" : ' % k, end='')
                bprint(v, n + 1)
            elif isinstance(v, list):
                if not v:
                    print('"%s" : []' % k)
                    continue
                print('"%s" : ' % k, end='')
                bprint(v, n + 1)
            else:
                if isinstance(v, str):
                    print('"%s" : "%s"' % (k, v))
                else:
                    print('"%s" : %s' % (k, v))
        print('\t' * (n - 1), '},')
    elif isinstance(d, list):
        if is_list:
            print('\t' * (n - 1), '[')
        else:
            print('[')
        for i in d:
            if isinstance(i, dict):
                bprint(i, n + 1, is_list=True)
            elif isinstance(i, list):
                bprint(i, n + 1, is_list=True)
            else:
                if isinstance(i, str):
                    print('"%s"' % i)
                else:
                    print(i)

        print('\t' * (n - 1), '],')


def printobj(obj, depth=0):
    obj = get_obj_info(obj, depth)
    bprint(obj)
