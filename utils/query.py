"""
query utils

F:
    ots.table.update(pk=pk, price=F('price')+1)

AP: list append
    ots.table.update(pk=pk, price=AP('list')+'value1')

EXT: list extend
    ots.table.update(pk=pk, price=EXT('list')+['value1'])

ITEM: dice setitem
    ots.table.update(pk=pk, price=ITEM('list')['k', 'v'])

"""


class F:
    def __init__(self, field):
        self.field = field

    def __add__(self, other):
        self.num = int(other)
        self.operator = '+'
        return self

    def __sub__(self, other):
        self.num = int(other)
        self.operator = '-'
        return self

    def __mul__(self, other):
        self.num = int(other)
        self.operator = '*'
        return self

    def __truediv__(self, other):
        self.num = int(other)
        self.operator = '/'
        return self

    def __str__(self):
        return f"{self.field} {self.operator} {self.num}"


class AP:
    def __init__(self, field):
        self.field = field

    def __add__(self, other):
        self.value = other
        return self

    def __str__(self):
        return f"{self.__class__.__name__}({self.field}).append({self.value})"


class EXT:
    def __init__(self, field):
        self.field = field

    def __add__(self, other):
        self.value_list = other
        return self

    def __str__(self):
        return f"{self.__class__.__name__}({self.field}).extend({self.value_list})"


class ITEM:
    def __init__(self, field):
        self.field = field

    def __getitem__(self, item):
        k, v = item
        self.key = k
        self.value = v
        return self

    def __str__(self):
        return f"{self.__class__.__name__}({self.field})[{self.key}] = {self.value}"
