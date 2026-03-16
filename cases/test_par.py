from common.parametrize import parametrize


@parametrize("data/test.xlsx", dtype={"n1": str})
def test_pardd(n1, n2, n3):
    assert type(n3) is float
    assert type(n2) is int
    assert type(n1) is str
