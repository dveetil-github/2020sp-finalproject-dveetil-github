from unittest import TestCase


class SmokeTests(TestCase):
    """
    testing for pytest
    """

    def test_basic(self):
        self.assertEqual("testing for pytest", "testing for pytest")
