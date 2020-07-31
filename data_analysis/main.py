class Stud(object):
    def __init__(self, values):
        self.values = values
        self.index = 0

    def iter_value(self):
        for value in self.values:
            a = yield value
            print(a)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index <= len(self.values):
            value = self.values[self.index]
            self.index += 1
            return value
        else:
            raise StopIteration


if __name__ == '__main__':
    stud = Stud(['a', 'b', 'c', 'd', 'f'])
    value = stud.iter_value()
    value.send("aaaa")
