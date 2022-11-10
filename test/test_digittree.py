import random
from dataclasses import dataclass
from typing import List
from unittest import TestCase

from digittree import DigitTree, common_prefix


class TextCommonPrefix(TestCase):
    def test_001(self):
        self.assertEqual('', common_prefix('', ''))

    def test_002(self):
        self.assertEqual('', common_prefix('', 'a'))

    def test_003(self):
        self.assertEqual('', common_prefix('a', ''))

    def test_004(self):
        self.assertEqual('3', common_prefix('31', '32'))

    def test_005(self):
        self.assertEqual('3104', common_prefix('3104', '31041'))

    def test_006(self):
        self.assertEqual('310', common_prefix('3104', '3105'))


class TestDigitTree(TestCase):
    """
    Tests for package digittree
    """

    def test_001(self):
        tree = DigitTree()
        self.assertEqual('', str(tree))

    def test_002(self):
        tree = DigitTree.from_list(nodes=['3001'])
        self.assertEqual('3001(terminal)', str(tree))

    def test_003(self):
        tree = DigitTree.from_list(nodes=['3001', '3002'])
        self.assertEqual('300(3001(terminal),3002(terminal))', str(tree))

    def test_004(self):
        """
        tree with 6 nodes
        """
        nodes = ['3001', '3002', '4000', '4100', '4123', '4124']
        tree = DigitTree.from_list(nodes=nodes)
        self.assertEqual(
            '(300(3001(terminal),3002(terminal)),'
            '4(4000(terminal),41(4100(terminal),412(4123(terminal),4124(terminal)))))',
            str(tree))
        self.assertTrue(all(tree.find_node(node=node) for node in nodes))
        tree.populate_used(node_len=4)
        print('\n'.join(f'{node.prefix:4}: {node.used}/{node.covers}' for node in tree.traverse()))
        print()
        print('\n'.join(f'{node.prefix:4}: {node.used}/{node.covers}'
                        for node in tree.traverse(pred=lambda node: len(node.childs) > 1)))

        foo = 1

    def test_005(self):
        """
        Tree with 100 random nodes
        """
        nodes = random.sample(list(range(1000, 10000)), 100)
        nodes = list(map(str, nodes))
        tree = DigitTree.from_list(nodes=nodes)
        print(str(tree))
        nodes_in_tree = [node.prefix for node in tree.traverse()
                         if node.terminal]
        self.assertEqual(sorted(nodes), nodes_in_tree)

    def test_006(self):
        nodes = [1, 2, 3, 4]
        nodes = list(map(str, nodes))
        tree = DigitTree.from_list(nodes=nodes)
        tree.populate_used(node_len=1)
        available = list(tree.available())
        self.assertEqual(list('56789'), available)

    def test_007_availability_issue(self):
        nodes = [527, 853, 600, 786, 457, 670, 595, 467, 821, 461, 486, 993, 641, 135, 281, 465, 431, 861, 833, 451,
                 651, 207, 368, 781, 121, 313, 340, 497, 506, 278, 580, 391, 619, 104, 864, 475, 184, 767, 454, 976,
                 518, 542, 174, 124, 291, 428, 134, 208, 773, 996, 876, 262, 598, 632, 120, 236, 179, 309, 157, 798,
                 462, 123, 602, 889, 645, 908, 648, 824, 596, 592, 809, 749, 867, 567, 250, 399, 947, 537, 491, 440,
                 770, 107, 198, 623, 302, 563, 900, 416, 393, 571, 388, 607, 156, 949, 657, 346, 280, 881, 816, 148,
                 384, 554, 707, 694, 274, 445, 306, 321, 172, 300, 267, 246, 587, 173, 239, 728, 552, 315, 706, 868,
                 975, 942, 668, 711, 583, 837, 152, 523, 699, 762, 753, 999, 687, 484, 746, 394, 779, 206, 351, 287,
                 866, 698, 177, 510, 801, 793, 893, 413, 636, 335, 963, 985, 385, 892, 458, 323, 760, 110, 423, 754,
                 558, 543, 319, 922, 182, 242, 569, 408, 685, 917, 113, 964, 535, 466, 259, 594, 836, 117, 308, 997,
                 409, 447, 992, 695, 939, 901, 125, 220, 709, 365, 533, 256, 165, 241, 328, 437, 768, 277, 316, 136]
        nodes = sorted(nodes)
        nodes = list(map(str, nodes))
        tree = DigitTree.from_list(nodes=nodes)
        tree.populate_used(node_len=3)
        print('\n'.join(f'{node.prefix:4}: {node.used}/{node.covers}'
                        for node in tree.traverse(pred=lambda node: node.childs)))
        # some numbers at some point where not reported as available
        should_be_available = [211, 212, 213, 214, 215, 216, 217, 218, 219, 371, 372, 373, 374, 375, 376, 377, 378, 379,
                               731, 732, 733, 734, 735, 736, 737, 738, 739, 841, 842, 843, 844, 845, 846, 847, 848, 849,
                               951, 952, 953, 954, 955, 956, 957, 958, 959]
        should_be_available = list(map(str, should_be_available))
        available = list(tree.available())
        problems = [av for av in should_be_available if av not in available]
        self.assertFalse(problems)

    def test_008_various_lengths(self):
        """
        Validation for various lengths
        :return:
        """
        for digits in range(1, 6):
            nodes = random.sample(list(range(10 ** (digits - 1), 10 ** digits)), 4 * 10 ** (digits - 1))
            nodes = list(map(str, nodes))
            print(f'digits: {digits}, nodes = [{",".join(nodes)}]')

            tree = DigitTree.from_list(nodes=nodes)
            nodes_in_tree = [node.prefix for node in tree.traverse()
                             if node.terminal]
            self.assertEqual(sorted(nodes), nodes_in_tree)
            tree.populate_used(node_len=digits)
            available = list(tree.available())

            wrong_length = [a for a in available if len(a) != digits]
            wrong_length.sort()
            self.assertFalse(wrong_length)

            available_in_tree = [av for av in available if tree.find_node(node=av)]
            self.assertFalse(available_in_tree)

            covered = set(nodes).union(set(available))
            missing = [node for i in range(10 ** (digits - 1), 10 ** digits)
                       if (node := f'{i}') not in covered]
            total = len(nodes) + len(available)
            print(f'digits: {digits}, nodes: {len(nodes)}, available: {len(available)}, total: {total}')
            self.assertEqual(9 * 10 ** (digits - 1), total, f'Not covered: {",".join(missing)}')

            missing = [node for node in nodes
                       if not tree.find_node(node=node)]
            self.assertFalse(missing)


@dataclass(init=False)
class TestAvailable(TestCase):
    """
    Test tree.available
    """
    nodes: List[str]
    tree: DigitTree
    available: List[str]

    DIGITS = 4
    SAMPLE = 400

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        print(f'{cls.__name__}.setupClass() in TestAvailable.setUpClass()')

        cls.nodes = random.sample(list(range(10 ** (cls.DIGITS - 1), 10 ** cls.DIGITS)), cls.SAMPLE)
        cls.nodes = list(map(str, cls.nodes))

        print(f'nodes = [{",".join(cls.nodes)}]')
        cls.tree = DigitTree.from_list(nodes=cls.nodes)
        cls.tree.populate_used(node_len=cls.DIGITS)
        print('\n'.join(
            f'{node.prefix:4}: {node.used}/{node.covers}: '
            f'{",".join(node.prefix for node in node.traverse(pred=lambda node: not node.childs))}'
            for node in cls.tree.traverse(pred=lambda node: node.childs)))
        cls.available = list(cls.tree.available(seed='1' + '0' * (cls.DIGITS - 1)))
        print(f'available = [{",".join(cls.available)}]')

    def test_001_correct_length(self):
        """
        all available must have the correct length
        """
        wrong_length = [a for a in self.available if len(a) != self.DIGITS]
        wrong_length.sort()
        self.assertFalse(wrong_length)

    def test_002_available_not_in_tree(self):
        """
        numbers reported as available should not be found in tree
        """
        available_in_tree = [av for av in self.available if self.tree.find_node(node=av)]
        self.assertFalse(available_in_tree)

    def test_003_all_available_covered(self):
        """
        number of nodes in tree plus available has to be correct
        """
        covered = set(self.nodes).union(set(self.available))
        missing = [node for i in range(10 ** (self.DIGITS - 1), 10 ** self.DIGITS)
                   if (node := f'{i}') not in covered]
        total = len(self.nodes) + len(self.available)
        print(f'nodes: {len(self.nodes)}, available: {len(self.available)}, total: {total}')
        self.assertEqual(9 * 10 ** (self.DIGITS - 1), total, f'Not covered: {",".join(missing)}')

    def test_004_all_nodes_in_tree(self):
        """
        All nodes should be present in the tree
        """
        missing = [node for node in self.nodes
                   if not self.tree.find_node(node=node)]
        self.assertFalse(missing)

    def test_005_nodes_not_in_available(self):
        """
        None of the nodes should be part of available
        """
        av = set(self.available)
        wrong = [node for node in self.nodes
                 if node in av]
        self.assertFalse(wrong)

    def test_006_available_unique(self):
        self.assertEqual(len(self.available), len(set(self.available)))


@dataclass(init=False)
class TestAvailableExtensions(TestCase):

    def test_empty_tree(self):
        """
        available extensions in empty tree
        :return:
        """
        nodes = []
        tree = DigitTree.from_list(nodes=nodes)
        available = list(tree.available(seed='170'))
        expected = [str(i) for i in range(170, 180)]
        for d2 in '012345689':
            expected.extend(f'1{d2}{i}' for i in range(10))
        for d1 in '23456789':
            expected.extend(f'{d1}{i:02}' for i in range(100))
        print(available)
        print(expected)
        self.assertEqual(expected, available)

    def test_4888(self):
        tree = DigitTree.from_list(nodes=['4888'])
        available = tree.available(seed='0100')
        print('\n'.join(next(available) for _ in range(100)))
