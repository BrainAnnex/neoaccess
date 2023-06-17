####  WARNING : the database will get erased!!!


import pytest
from src.neoaccess.cypher_utils import NewCypherUtils



def test_prepare_labels():
    lbl = ""
    assert NewCypherUtils.prepare_labels(lbl) == ""

    lbl = "client"
    assert NewCypherUtils.prepare_labels(lbl) == ":`client`"

    lbl = ["car", "car manufacturer"]
    assert NewCypherUtils.prepare_labels(lbl) == ":`car`:`car manufacturer`"



def test_prepare_where():
    assert NewCypherUtils.prepare_where("") == ""
    assert NewCypherUtils.prepare_where("      ") == ""
    assert NewCypherUtils.prepare_where([]) == ""
    assert NewCypherUtils.prepare_where([""]) == ""
    assert NewCypherUtils.prepare_where(("  ", "")) == ""

    wh = "n.name = 'Julian'"
    assert NewCypherUtils.prepare_where(wh) == "WHERE (n.name = 'Julian')"

    wh = ["n.name = 'Julian'"]
    assert NewCypherUtils.prepare_where(wh) == "WHERE (n.name = 'Julian')"

    wh = ("p.key1 = 123", "   ",  "p.key2 = 456")
    assert NewCypherUtils.prepare_where(wh) == "WHERE (p.key1 = 123 AND p.key2 = 456)"

    with pytest.raises(Exception):
        assert NewCypherUtils.prepare_where(123)    # Not a string, nor tuple, nor list



def test_dict_to_cypher():
    d = {'since': 2003, 'code': 'xyz'}
    assert NewCypherUtils.dict_to_cypher(d) == ('{`since`: $par_1, `code`: $par_2}', {'par_1': 2003, 'par_2': 'xyz'})

    d = {'year first met': 2003, 'code': 'xyz'}
    assert NewCypherUtils.dict_to_cypher(d) == ('{`year first met`: $par_1, `code`: $par_2}', {'par_1': 2003, 'par_2': 'xyz'})

    d = {'year first met': 2003, 'code': 'xyz'}
    assert NewCypherUtils.dict_to_cypher(d, prefix="val_") == ('{`year first met`: $val_1, `code`: $val_2}', {'val_1': 2003, 'val_2': 'xyz'})

    d = {'cost': 65.99, 'code': 'the "red" button'}
    assert NewCypherUtils.dict_to_cypher(d) == ('{`cost`: $par_1, `code`: $par_2}', {'par_1': 65.99, 'par_2': 'the "red" button'})

    d = {'phrase': "it's ready!"}
    assert NewCypherUtils.dict_to_cypher(d) == ('{`phrase`: $par_1}', {'par_1': "it's ready!"})

    d = {'phrase': '''it's "ready"!'''}
    assert NewCypherUtils.dict_to_cypher(d) == ('{`phrase`: $par_1}', {'par_1': 'it\'s "ready"!'})

    d = None
    assert NewCypherUtils.dict_to_cypher(d) == ("", {})

    d = {}
    assert NewCypherUtils.dict_to_cypher(d) == ("", {})
