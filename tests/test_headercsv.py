import py
from csvuploader import HeaderCsv
import pandas as pd
from pandas.util.testing import assert_frame_equal
from StringIO import StringIO


def test_load_file(request):
    test_dir = py.path.local(request.module.__file__)
    with test_dir.dirpath('data', 'simple.csv').open('r') as f:
        text = f.read()
        assert text == 'A,B\n1,2'
        h = HeaderCsv.load(f)
        assert h.metadata is None
        assert_frame_equal(h.df, pd.DataFrame([[1, 2]], columns=['A', 'B']).set_index('A'))


def test_load_file_with_header(request):
    test_dir = py.path.local(request.module.__file__)
    with test_dir.dirpath('data', 'simple_with_header.csv').open('r') as f:
        h = HeaderCsv.load(f)
        assert h.metadata == { 'name': 'Simple with header' }
        assert_frame_equal(h.df, pd.DataFrame([[1, 2]], columns=['A', 'B']).set_index('A'))


def test_roundtrip():
    stream = StringIO()
    h1 = HeaderCsv(None, pd.DataFrame([[1, 2]], columns=['A', 'B']))
    h1.dump(stream)
    h2 = HeaderCsv.load(stream)
    assert h2.metadata is None
    assert_frame_equal(h2.df, pd.DataFrame([[1, 2]], columns=['A', 'B']))