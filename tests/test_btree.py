from samping.utils.btree import Btree


def test_btree_set():
    tree = Btree(10)

    keys = list(range(1000))

    for key in keys:
        _, replaced = tree.set(key)
        assert not replaced

    assert len(tree) == len(keys)


def test_btree_get_remove():
    tree = Btree(10)

    keys = list(range(500))
    for key in keys:
        _, replaced = tree.set(key)
        assert not replaced

    val, ok = tree.get(40)
    assert ok
    assert val == 40

    val, ok = tree.remove(40)
    assert ok
    assert val == 40

    val, ok = tree.get(40)
    assert not ok
    assert val == tree.empty
