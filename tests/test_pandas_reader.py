import unittest
import pandas as pd
import numpy as np
import tempfile
from datetime import date, datetime

from s3access.s3pandas.reader import Pandas


class StrictPandasReader(unittest.TestCase):

    def test_categorical_combine(self):
        a = b"a\nb\n"
        b = b"b\nc\n"
        columns = {'a': 'category'}
        reader = Pandas(strict=True)
        a_df = reader.read(a, columns)
        self.assertIn('a', a_df.columns)
        self.assertIsInstance(a_df['a'].dtype, pd.CategoricalDtype)
        b_df = reader.read(b, columns)
        self.assertIn('a', b_df.columns)
        self.assertIsInstance(b_df['a'].dtype, pd.CategoricalDtype)

        c_df = reader.combine([a_df, b_df])
        self.assertIn('a', c_df.columns)
        self.assertIsInstance(c_df['a'].dtype, pd.CategoricalDtype)

    def test_date_parsing(self):
        a = b"2021-06-05,2021-06-05T00:10:10\n2021-06-01,2021-06-01T23:59:59"

        columns = {'a': date, 'b': datetime}
        reader = Pandas(strict=True)
        a_df = reader.read(a, columns)
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
