import tempfile
import unittest
from datetime import date, datetime

import numpy as np
import pandas as pd

from s3access.reader import Options
from s3access.s3pandas.reader import Pandas, empty


class StrictPandasReader(unittest.TestCase):

    def test_categorical_combine(self):
        a = b"a\nb\n"
        b = b"b\nc\n"
        columns = {'a': 'category'}
        reader = Pandas(strict=True)
        a_df = reader.read(a, columns, options=Options())
        self.assertIn('a', a_df.columns)
        self.assertIsInstance(a_df['a'].dtype, pd.CategoricalDtype)
        b_df = reader.read(b, columns, options=Options())
        self.assertIn('a', b_df.columns)
        self.assertIsInstance(b_df['a'].dtype, pd.CategoricalDtype)

        c_df = reader.combine([a_df, b_df], options=Options())
        self.assertIn('a', c_df.columns)
        self.assertIsInstance(c_df['a'].dtype, pd.CategoricalDtype)

    def test_date_parsing(self):
        a = b"2021-06-05,2021-06-05T00:10:10\n2021-06-01,2021-06-01T23:59:59"

        columns = {'a': date, 'b': datetime}
        reader = Pandas(strict=True)
        a_df = reader.read(a, columns, options=Options())
        self.assertIn('a', a_df.columns)
        self.assertEqual(a_df['a'].dtype, np.dtype('<M8[ns]'))
        self.assertEqual(a_df['b'].dtype, np.dtype('<M8[ns]'))

    def test_categorical_write_support(self):
        # some version of pyarrow did not support saving categorical data
        a = pd.DataFrame({"a": ['a', 'b', 'a', 'a', 'c', 'd', 'a', 'b', 'c', 'd']})
        a['a'] = a['a'].astype('category')

        with tempfile.TemporaryFile() as fp:
            a.to_parquet(fp)
            fp.seek(0)
            restored = pd.read_parquet(fp)
        self.assertIn('a', restored.columns)
        self.assertIsInstance(restored['a'].dtype, pd.CategoricalDtype)
        self.assertTrue(np.all(a['a'] == restored['a']))

    def test_distinct(self):
        a = b"a\nb\na\n"
        b = b"b\nc\n\na"
        columns = {'a': str}
        reader = Pandas()
        opts = Options(distinct=True)
        a_df = reader.read(a, columns, options=opts)
        self.assertEqual({'a', 'b'}, {x for x in a_df['a']})
        self.assertEqual(2, len(a_df['a'].tolist()))
        b_df = reader.read(b, columns, options=opts)
        self.assertEqual({'a', 'b', 'c'}, {x for x in b_df['a']})
        self.assertEqual(3, len(b_df['a'].tolist()))
        c_df = reader.combine([a_df, b_df], options=opts)
        self.assertEqual({'a', 'b', 'c'}, {x for x in c_df['a']})
        self.assertEqual(3, len(c_df['a'].tolist()))


class EmptyDataFrame(unittest.TestCase):

    def test_empty(self):
        columns = {'a': 'string', 'b': 'int64', 'c': 'float64', 'd': 'category'}
        result = empty(columns)
        self.assertTrue(result.empty)
        self.assertEqual(len(result.columns), len(columns))
        for col, t in columns.items():
            self.assertEqual(result[col].dtype, pd.Series([], dtype=t).dtype)
