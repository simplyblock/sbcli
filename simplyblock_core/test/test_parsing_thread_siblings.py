import unittest

from simplyblock_core.utils import parse_thread_siblings_list


class TestParseThreadSiblingsList(unittest.TestCase):
    def test_single_cpu(self):
        self.assertEqual(parse_thread_siblings_list("9"), [9])


    def test_two_cpus(self):
        self.assertEqual(parse_thread_siblings_list("9,25"), [9, 25])


    def test_range(self):
        self.assertEqual(parse_thread_siblings_list("4-7"), [4, 5, 6, 7])


    def test_multiple_cpus(self):
        self.assertEqual(parse_thread_siblings_list("9,25,41,57"), [9, 25, 41, 57])


    def test_range_with_colon_step(self):
        self.assertEqual(parse_thread_siblings_list("25-33:4"), [25, 29, 33])


    def test_range_with_slash_step(self):
        self.assertEqual(parse_thread_siblings_list("0-6/3"), [0, 3, 6])


    def test_mixed_ranges(self):
        self.assertEqual(parse_thread_siblings_list("2-3,10-11"), [2, 3, 10, 11])


    def test_mixed_ranges_large(self):
        self.assertEqual(parse_thread_siblings_list("2-8,10-16"), [2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16])


    def test_mixed_ranges_with_step(self):
        self.assertEqual(parse_thread_siblings_list("1,2,4-10,12-20:4"), [1, 2, 4, 5, 6, 7, 8, 9, 10, 12, 16, 20])


    def test_whitespace(self):
        self.assertEqual(parse_thread_siblings_list("9,  25  ,41"), [9, 25, 41])


    def test_empty_string(self):
        self.assertEqual(parse_thread_siblings_list(""), [])


    def test_invalid_token(self):
        with self.assertRaises(ValueError):
            parse_thread_siblings_list("a-b")


    def test_invalid_range_order(self):
        with self.assertRaises(ValueError):
            parse_thread_siblings_list("5-2")


    def test_zero_or_negative_step(self):
        with self.assertRaises(ValueError):
            parse_thread_siblings_list("0-5:0")
        with self.assertRaises(ValueError):
            parse_thread_siblings_list("0-5:-1")

if __name__ == '__main__':
    unittest.main()