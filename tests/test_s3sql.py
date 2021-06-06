import unittest

from s3access.sql import quote


class S3PathTestCase(unittest.TestCase):
    def test_quote(self):
        self.assertEqual("3", quote(3))
        self.assertEqual("'3'", quote('3'))
        self.assertEqual("'foo'", quote('foo'))
        self.assertEqual("'foo'''", quote("foo'"))
        self.assertEqual("(1, 2, 3)", quote([1, 2, 3]))
        self.assertEqual("(1, 2, 3)", quote((1, 2, 3)))
        self.assertEqual("(1)", quote({1}))
        self.assertEqual("('foo', '''bar''')", quote(("foo", "'bar'")))
        self.assertEqual("NULL", quote(None))


if __name__ == '__main__':
    unittest.main()
