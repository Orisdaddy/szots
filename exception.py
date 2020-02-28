class FieldNotFoundError(Exception):
    def __init__(self, *args):
        super(FieldNotFoundError, self).__init__(*args)


class ParamError(Exception):
    def __init__(self, *args):
        super(ParamError, self).__init__(*args)
