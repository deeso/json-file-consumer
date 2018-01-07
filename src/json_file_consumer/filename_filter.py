import regex


class Filter(object):
    def __init__(self, pattern, name=None):
        self.pattern = pattern
        self.name = name
        self.regex = regex.compile(pattern)

    def matches(self, data):
        v = self.regex.search(data)
        return False if v is None else True
