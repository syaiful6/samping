from dataclasses import dataclass
import typing
from collections import namedtuple

T = typing.TypeVar("T")


@dataclass
class Node(typing.Generic[T]):
    count: int = 0
    items: typing.Optional[typing.List[T]] = None
    children: typing.Optional["Node[T]"] = None

    @property
    def leaf(self):
        return self.children is None

    def __len__(self):
        return self.count

    def _update_count(self):
        self.count = len(self.items) if self.items else 0
        if not self.leaf:
            for c in self.children:
                self.count += c.count

    def copy_children(self):
        if not self.children:
            return None
        children = []
        for c in self.children:
            children.append(c.copy())
        return children

    def copy(self):
        return Node(
            count=self.count,
            items=self.items[:] if self.items else None,
            children=self.copy_children(),
        )


PathHint = namedtuple("PathHint", ("used", "path"))


def default_less(a, b):
    return a < b


class Btree(typing.Generic[T]):
    def __init__(
        self, degree, less: typing.Callable[[T, T], bool] = default_less, empty=None
    ):
        self.min, self.max = degree_to_min_max(degree)
        self.less = less
        self.empty = empty
        self.root = None
        self.count = 0

    def _new_node(self, leaf: bool):
        node = Node(items=[])
        if not leaf:
            node.children = []
        return node

    def _binary_search(self, node: Node[T], key: T):
        low, high = 0, len(node.items)
        while low < high:
            h = (low + high) // 2
            if not self.less(key, node.items[h]):
                low = h + 1
            else:
                high = h
        if low > 0 and not self.less(node.items[low - 1], key):
            return low - 1, True

        return low, False

    def set(self, item: T):
        if self.root is None:
            self.root = self._new_node(True)
            self.root.items.append(item)
            self.root.count = 1
            self.count = 1
            return self.empty, False

        prev, replaced, split = self._node_set(self.root, item)
        if split:
            left = self.root.copy()
            right, median = self._node_split(left)
            self.root = self._new_node(False)
            self.root.children.extend([left, right])
            self.root.items.append(median)
            self.root._update_count()
            return self.set(item)
        if replaced:
            return prev, True
        self.count += 1
        return self.empty, False

    def _node_set(self, node: Node[T], item: T):
        i, found = self._binary_search(node, item)
        if found:
            prev = node.items[i]
            node.items[i] = item
            return prev, True, False
        if node.leaf:
            if len(node.items) == self.max:
                return self.empty, False, True
            node.items.append(self.empty)
            copy_at(i + 1, node.items, node.items[i:])
            node.items[i] = item
            node.count += 1
            return self.empty, False, False
        prev, replaced, split = self._node_set(node.children[i], item)
        if split:
            if len(node.items) == self.max:
                return self.empty, False, True
            right, median = self._node_split(node.children[i])
            node.children.append(None)
            copy_at(i + 1, node.children, node.children[i:])
            node.children[i + 1] = right
            node.items.append(self.empty)
            copy_at(i + 1, node.items, node.items[i:])
            node.items[i] = median
            return self._node_set(node, item)
        if not replaced:
            node.count += 1

        return prev, replaced, False

    def _node_split(self, node: Node[T]):
        i = self.max // 2
        median = node.items[i]

        # right node
        right = self._new_node(node.leaf)
        right.items = node.items[i + 1 :]
        if not node.leaf:
            right.children = node.children[i + 1 :]
        right._update_count()

        # left node
        node.items[i] = self.empty
        node.items = node.items[:i]
        if not node.leaf:
            node.children = node.children[: i + 1]
        node._update_count()

        return right, median

    def scan(self):
        yield from self._scan_node(self.root)

    def _scan_node(self, node: Node[T]):
        if node.leaf:
            yield from node.items
            return

        for i, item in enumerate(node.items):
            yield from self._scan_node(node.children[i])

            yield item

        yield from self._scan_node(node.children[len(node.children) - 1])

    def get(self, key: T):
        node = self.root
        while True:
            i, found = self._binary_search(node, key)
            if found:
                return node.items[i], True
            if not node.children:
                return self.empty, False

            node = node.children[i]

    def __len__(self):
        return self.count

    def remove(self, key: T):
        if not self.root:
            return self.empty, False

        prev, deleted = self._remove(self.root, False, key)
        if not deleted:
            return self.empty, False
        if len(self.root.items) == 0 and not self.root.leaf:
            self.root = self.root.children[0]

        self.count -= 1
        if self.count == 0:
            self.root = None
        return prev, True

    def _remove(self, node: Node[T], max: bool, key: T):
        i = 0
        found = False
        if max:
            i, found = len(node.items) - 1, True
        else:
            i, found = self._binary_search(node, key)
        if node.leaf:
            if found:
                # found the items at the leaf, remove it
                prev = node.items[i]
                node.items = node.items[:i] + node.items[i + 1 :]
                node.count -= 1
                return prev, True
            return self.empty, False

        if found:
            if max:
                i += 1
                prev, deleted = self._remove(node.children[i], True, self.empty)
            else:
                prev = node.items[i]
                max_item, _ = self._remove(node.children[i], True, self.empty)
                deleted = True
                node.items[i] = max_item
        else:
            prev, deleted = self._remove(node.children[i], max, key)
        if not deleted:
            return self.empty, False
        node.count -= 1
        if len(node.children[i].items) < self.min:
            self._node_rebalance(node, i)
        return prev, True

    def _node_rebalance(self, node: Node[T], i: int):
        if i == len(node.items):
            i -= 1

        left: Node[T] = node.children[i]
        right: Node[T] = node.children[i + 1]

        if len(left.items) + len(right.items) < self.max:
            # Merges the left and right children nodes together as a single node
            # that includes (left,item,right), and places the contents into the
            # existing left node. Delete the right node altogether and move the
            # following items and child nodes to the left by one slot.

            left.items.append(node.items[i])
            left.items.extend(right.items)
            if not left.leaf:
                left.children.extend(right.children)
            left.count += right.count + 1

            # move the item over one slot
            copy_at(i, node.items, node.items[i + 1 :])
            node.items[len(node.items) - 1] = self.empty
            node.items = node.items[: len(node.items) - 1]

            # move the children over one slot
            copy_at(i + 1, node.children, node.children[i + 2 :])
            node.children[len(node.children) - 1] = None
            node.children = node.children[: len(node.children) - 1]
        elif len(left.items) > len(right.items):
            # move left -> right over one slot
            right.items.append(self.empty)
            copy_at(1, right.items, right.items[:])
            right.items[0] = node.items[i]
            right.count += 1
            node.items[i] = left.items[len(left.items) - 1]
            left.items[len(left.items) - 1] = self.empty
            left.items = left.items[: len(left.items) - 1]
            left.count -= 1

            if not left.leaf:
                right.children.append(None)
                copy_at(1, right.children, right.children[:])
                right.children[0] = left.children[len(left.children) - 1]
                left.children[len(left.children) - 1] = None
                left.children = left.children[: len(left.children) - 1]
                left.count -= right.children[0].count
                right.count += right.children[0].count
        else:
            # move left <- right over one slot
            # Same as above but the other direction
            left.items.append(node.items[i])
            left.count += 1
            node.items[i] = right.items[0]
            copy_at(0, right.items, right.items[1:])
            right.items[len(right.items) - 1] = self.empty
            right.items = right.items[: len(right.items) - 1]
            right.count -= 1

            if not left.leaf:
                left.children.append(right.children[0])
                copy_at(0, right.children, right.children[1:])
                right.children[len(right.childrenn) - 1] = None
                right.children = right.children[: len(right.children) - 1]
                left.count += left.children[len(left.children) - 1].count
                right.count -= left.children[len(left.children) - 1].count

    def ascend(self, pivot: T):
        yield from self._ascend_node(self.root, pivot)

    def _ascend_node(self, node: Node[T], pivot: T):
        i, found = self._binary_search(node, pivot)
        if not found:
            if not node.leaf:
                yield from self._ascend_node(node.children[i], pivot)

        # We are either in case that
        # - node is found, we should iterate throuh it starting at `i`,
        #   the index it was located at.
        # - node is not found, and TODO: fill in.
        while i < len(node.items):
            yield node.items[i]
            if not node.leaf:
                yield from self._scan_node(node.children[i + 1])
            i += 1

    def _node_reverse(self, node: Node[T]):
        if node.leaf:
            yield from reversed(node.items)
            return

        yield from self._node_reverse(node.children[len(node.children) - 1])

        i = len(node.items)
        while i >= 0:
            yield node.items[i]
            yield from self._node_reverse(node.children[i])
            i -= 1

    def descend(self, pivot: T):
        yield from self._descend_node(self.root, pivot)

    def _descend_node(self, node: Node[T], pivot: T):
        i, found = self._binary_search(node, pivot)
        if not found:
            if not node.leaf:
                yield from self._descend_node(node.children[i], pivot)
            i -= 1

        while i >= 0:
            yield node.items[i]
            if not node.leaf:
                yield from self._node_reverse(node.children[i])
            i -= 1


def copy_at(index, dest, source):
    max_copy = min(len(source), len(dest) - index)
    ix = 0
    while ix < max_copy:
        dest[index + ix] = source[ix]
        ix += 1


def degree_to_min_max(deg: int):
    if deg <= 0:
        deg = 32
    elif deg == 1:
        deg = 2
    max = (deg * 2) - 1
    min = max // 2
    return min, max
