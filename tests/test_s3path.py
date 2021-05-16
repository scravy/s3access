import unittest

from s3access.s3path import S3Path


class S3PathTestCase(unittest.TestCase):
    def test_s3path(self):
        self.assertEqual(
            S3Path('s3://bucket/key'),
            S3Path('s3a://bucket/key'),
        )
        self.assertEqual(
            S3Path('s3://bucket/key'),
            S3Path('s3://bucket/key/'),
        )

    def test_params(self):
        p = S3Path('s3://bucket/foo=bar')
        self.assertEqual({
            'foo': 'bar',
        }, p.params)

    def test_with_params(self):
        p = S3Path('s3://bucket/foo=bar')
        p2 = p.with_params(foo='qux')
        self.assertEqual({
            'foo': 'qux',
        }, p2.params)
        p3 = p.with_params(qux='quuz', klm='pqq')
        self.assertEqual({
            'foo': 'bar',
            'qux': 'quuz',
            'klm': 'pqq',
        }, p3.params)
        self.assertEqual(
            S3Path('bucket/foo=bar/qux=quuz/klm=pqq'),
            p3,
        )

    def test_append(self):
        p = S3Path('s3://bucket/foo=bar')
        p /= 'hey=yea'
        self.assertEqual(
            p,
            S3Path('s3n://bucket/foo=bar/hey=yea')
        )

    def test_get_url(self):
        p = S3Path('s3://bucket/key/')
        self.assertEqual('s3://bucket/key', p.get_url())
        self.assertEqual('s3a://bucket/key', p.get_url('s3a'))

    def test_s3path_str(self):
        p = S3Path('s3://bucket/')
        p2 = p.with_params(client='cl')
        self.assertEqual('s3://bucket/client=cl', str(p2))

    def test_readme_example(self):
        p = S3Path("s3://bucket/key/part=value")

        p1 = p.with_params(foo=7, bar=29)
        self.assertEqual('s3://bucket/key/part=value/foo=7/bar=29', str(p1))

        p2 = p.with_params(foo=7, part='else', bar=29)
        self.assertEqual('s3://bucket/key/part=else/foo=7/bar=29', str(p2))

        p3 = p.with_bucket('other')
        self.assertEqual('s3://other/key/part=value', str(p3))

        p4 = p.with_key('path')
        self.assertEqual('s3://bucket/path', str(p4))


if __name__ == '__main__':
    unittest.main()
