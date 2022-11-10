from dataclasses import dataclass, field
from itertools import takewhile
from typing import Optional, Iterator, Dict, Callable, Iterable, Generator

__all__ = ['DigitTree']


def common_prefix(s1: str, s2: str):
    return ''.join(c for c, _ in takewhile(lambda c: c[0] == c[1], zip(s1, s2)))


@dataclass
class DigitTree:
    childs: Dict[int, 'DigitTree'] = field(default_factory=dict)
    prefix: str = field(default='')
    terminal: bool = field(default=False)
    used: int = field(default=0)
    covers: int = field(default=0)

    @property
    def empty(self):
        return not self.childs and not self.prefix

    @classmethod
    def from_list(cls, *, nodes: Iterable[str]):
        """
        create a tree from a list of nodes to be added
        :param nodes:
        :return:
        """
        tree = cls()
        for node in nodes:
            tree.add_node(node=node)
        return tree

    def add_node(self, *, node: str):
        common = common_prefix(self.prefix, node)
        if common == node:
            self.terminal = True
            return
        # node needs to be added to the sub-tree for the next digit after the common prefix
        assert len(node) > len(self.prefix)
        next_digit = int(node[len(common)])
        sub_tree = self.childs.get(next_digit)
        if sub_tree is None:
            self.childs[next_digit] = self.__class__(prefix=f'{self.prefix}{next_digit}')
            sub_tree = self.childs[next_digit]
        sub_tree.add_node(node=node)

    def traverse(self, *, pred: Callable[['DigitTree'], bool] = None) -> Iterator['DigitTree']:
        def always_true(foo):
            return True

        pred = pred or always_true
        if pred(self):
            yield self
        for child_key in sorted(self.childs):
            yield from (node for node in self.childs[child_key].traverse()
                        if pred(node))

    def __str__(self):
        r = ''
        if self.terminal:
            r = f'{self.prefix}(terminal)'
        if self.childs:
            child_str = [str(self.childs[c]) for c in sorted(self.childs)]
            if len(child_str) == 1:
                if r:
                    r = f'{r}({child_str})'
                else:
                    r = child_str[0]
            else:
                r = r or f'{self.prefix}'
                child_str = ','.join(child_str)
                r = f'{r}({child_str})'
        return r

    def find_node(self, *, node: str) -> Optional['DigitTree']:
        """
        Find a node in the tree
        :param node:
        :return:
        """
        if self.prefix == node:
            if self.terminal:
                return self
            return None
        if not self.childs:
            return None
        next_digit = int(node[len(self.prefix)])
        child = self.childs.get(next_digit)
        if child is None:
            return None
        return child.find_node(node=node)

    def populate_used(self, *, node_len: int):
        """
        populate utilization based on node_len given
        :param node_len:
        :return:
        """
        if self.terminal:
            assert len(self.prefix) == node_len, f'{self.prefix} does not fit to node_len {node_len}'
        for child in self.childs.values():
            child.populate_used(node_len=node_len)
        self.covers = 10 ** (node_len - len(self.prefix))
        self.used = sum(child.used for child in self.childs.values())
        if self.terminal:
            self.used += 1

    def available(self, *, seed: str = None) -> Generator[str, None, None]:
        """
        Iterate over available numeric strings
        :return:
        """
        seed = seed or '1000'
        if not self.childs and seed:
            self.add_node(node=seed)
        self.populate_used(node_len=len(seed))
        yield from self._rec_availabe()

    def _rec_availabe(self) -> Generator[str, None, None]:
        # we want to hand out numbers from the most utilized ranges 1st
        if not self.childs:
            return

        childs = sorted(self.childs, key=lambda child: self.childs[child].used, reverse=True)
        # yield available numbers from child trees
        for child in childs:
            yield from self.childs[child]._rec_availabe()
        # then yield available from all digits not represented by child trees
        for digit in range(10):
            if digit in self.childs:
                # there is a child tree for this digit -> already yielded from above
                continue
            if not self.prefix and not digit:
                # number starting with zero should be skipped
                continue
            covers = self.childs[childs[0]].covers
            #if not self.prefix and covers>1:
            #    continue
            if covers == 1:
                number = f'{self.prefix}{digit}'
                yield number
            else:
                length = len(str(covers))-1
                yield from (f'{self.prefix}{digit}{i:0{length}}' for i in range(covers))
        return
