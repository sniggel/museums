"""
test_wikipedia_utils.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in museums_etl.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
from dependencies.wikipedia_utils import cleanup_number


class WikipediaUtilsTest(unittest.TestCase):
    """Test suite for transformation in museums_etl.py
    """

    def test_cleanup_number(self):
        number_million = "10.4324 million"
        self.assertEqual(cleanup_number(number_million), 10000000)


if __name__ == '__main__':
    unittest.main()
