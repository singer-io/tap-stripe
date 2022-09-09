import unittest
from parameterized import parameterized
from tap_stripe import Context, get_date_window_size, DEFAULT_DATE_WINDOW_SIZE

class TestGetWindowSize(unittest.TestCase):
    """
    Test `get_date_window_size` method of the client.
    """

    @parameterized.expand([
        ["integer_value", 10, 10.0],
        ["float_value", 100.5, 100.5],
        ["string_integer", "10", 10.0],
        ["string_float", "100.5", 100.5],
    ])
    def test_window_size_values(self, name, timeout_value, expected_value):
        """
        Test that for the valid value of window size,
        No exception is raised and the expected value is set.
        """
        Context.config = {"date_window_size": timeout_value}

        # Verify window size value is expected
        self.assertEqual(get_date_window_size("date_window_size", DEFAULT_DATE_WINDOW_SIZE), expected_value)

    @parameterized.expand([
        ["integer_zero", 0],
        ["float_zero", 0.0],
        ["string_zero", "0"],
        ["string_float_zero", "0.0"],
        ["string_alphabate", "abc"],
    ])
    def test_invalid_value(self, name, timeout_value):
        """
        Test that for invalid value exception is raised.
        """
        Context.config = {"date_window_size": timeout_value}
        with self.assertRaises(Exception) as e:
            get_date_window_size("date_window_size", DEFAULT_DATE_WINDOW_SIZE)

        # Verify that the exception message is expected.
        self.assertEqual(str(e.exception), "The entered windo size is invalid, it should be a valid none-zero integer.")

    def test_none_value(self):
        """
        Test if no window size is not passed in the config, then set it to the default value.
        """
        Context.config = {}

        # Verify that the default window size value is set.
        self.assertEqual(get_date_window_size("date_window_size", DEFAULT_DATE_WINDOW_SIZE), DEFAULT_DATE_WINDOW_SIZE)
