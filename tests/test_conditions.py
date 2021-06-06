import unittest

from s3access.sql import AND, OR, EQ, NEQ, GTE, LT


class S3PathTestCase(unittest.TestCase):
    def test_sql_and(self):
        condition = AND(OR(EQ(7), NEQ(None)), GTE(2), LT(10))
        self.assertEqual("((foo = 7) OR (foo IS NOT NULL)) AND (foo >= 2) AND (foo < 10)",
                         condition.get_sql_fragment("foo"))

    def test_check_and(self):
        condition = AND(OR(EQ(7), NEQ(None)), GTE(2), LT(10))
        self.assertFalse(condition.check(None))
        self.assertFalse(condition.check(10))
        self.assertTrue(condition.check(2))
        self.assertTrue(condition.check(7))


if __name__ == '__main__':
    unittest.main()
