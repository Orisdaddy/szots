class Data:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __str__(self):
        return str(self.__dict__)


class OTSResponse:
    '''response类'''

    def __init__(self, ):
        self.status = True

    def __getattr__(self, item):
        pass

    def __getitem__(self, item):
        return getattr(self, item, None)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def add(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def pop(self, value):
        return self.__dict__.pop(value)

    def clear(self):
        self.__dict__.clear()
        self.status = True

    @property
    def response(self):
        return self

    @property
    def first(self):
        return self

    @property
    def all(self):
        return self

    def __str__(self):
        return str(self.__dict__)

    def error(self):
        if self.status is False:
            return {
                'msg': self.error_message,
                'type': self.error_type,
                'code': self.error_code,
                'id': self.request_id,
            }
        else:
            return
