from samping.utils.btree import Btree


def test_btree_set():
    tree = Btree(10, empty=1)

    keys = list(range(1000))

    for key in keys:
        _, replaced = tree.set(key)
        assert not replaced

    assert len(tree) == len(keys)