

class Node:
    def __init__(self, item, rank=0):
        """
        initializer of <class 'Node'>

        :param item: denotation of a node.
        :param rank: rank of a node.
        """
        self.item = item
        self.rank = rank

    def __str__(self):
        """
        overload of build-in function '__str__'
        :return: string for printing.
        """
        return '(%s, %d)' % (self.item, self.rank)


class DisjointSet(dict):
    def __init__(self, seq=None, **kwargs):
        """
        initializer of <class 'DisjointSet'>

        :param seq: parameter 'seq' of the initializer of <class 'dict'>.
        :param kwargs: parameter 'kwargs' of the initializer of <class 'dict'>.
        """
        for key in kwargs.keys():
            kwargs[key] = Node(key, 0 if type(kwargs[key]) != int else kwargs[key])

        if seq is None:
            super(DisjointSet, self).__init__(**kwargs)
        else:
            seq = list(map(lambda x: (x, Node(x)), seq))
            super(DisjointSet, self).__init__(seq, **kwargs)

    def add(self, x):
        """
        insert a new node into the forest

        :param x: denotation of a node.
        :return: status of insertion.
        """
        # insert a new node if x doesn't exist in the original forest, and return True
        if self.get(x) is None:
            self[x] = Node(x)
            return True
        # do nothing if x exists in the original forest, and return False
        else:
            return False

    def find(self, x):
        """
        find the parent of x

        :param x: denotation of a node.
        :return: parent of x.
        """
        # return the parent of x if x exists in the original forest
        try:
            # find the parent of x recursively
            return x if x == self[x].item else self.find(self[x].item)
        # return None if x doesn't exist in the original forest
        except KeyError:
            return None

    def unite(self, x, y):
        """
        unite two nodes

        :param x: denotation of the first node.
        :param y: denotation of the second node.
        :return: status of union.
        """
        # try to find the parent of x and y
        try:
            x, y = self.find(x), self.find(y)
            # unite x and y if they have different parents
            if x != y:
                # let y be the parent of x if x has lower rank than y
                if self[x].rank < self[y].rank:
                    self[x] = self[y]
                    self[y].rank += 1
                # let x be the parent of y if x has higher rank than y
                else:
                    self[y] = self[x]
                    self[x].rank += 1
                # return True when perform union successfully
                return True
        # do nothing if fail to find the parent of x or y
        except KeyError:
            pass
        # return False when fail to perform union
        return False
