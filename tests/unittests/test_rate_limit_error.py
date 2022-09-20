import unittest
from unittest import mock
import stripe
from tap_stripe import new_request

class MockRequest():
    '''Mock Request object'''
    def __init__(self, response):
        self.last_response = response

    def request_raw(self, method, url, params=None, supplied_headers=None, is_streaming=False):
        return {}, {}, {}, {}

    def interpret_response(self, rbody, rcode, rheaders):
        raise stripe.error.RateLimitError("Rate Limit Error", 429, {}, {}, {})

class TestRateLimitError(unittest.TestCase):
    """
    Test that the tap retries each request 7 times on rate limit error.
    """
    
    @mock.patch("time.sleep")
    def test_retry_count_of_429_error(self, mock_sleep):
        """
        Test that the tap retries each request 7 times on 429 error.
        - Verify that `time.sleep` was called 6 times. (1 count less than no of retry count)
        """
        mock_request = MockRequest('url')
        with self.assertRaises(stripe.error.RateLimitError) as e:
            new_request(mock_request, 'GET', 'dummy_url')

        # Verify that `time.sleep` was called 6 times.
        self.assertEqual(mock_sleep.call_count, 6)
