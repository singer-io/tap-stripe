import unittest
from unittest import mock
from tap_stripe import new_request

class MockRequest():
    '''Mock Request object'''
    def __init__(self, response):
        self.last_response = response

    def request_raw(self, method, url, params=None, supplied_headers=None, is_streaming=False):
        return {}, {}, {}, {}

    def interpret_response(self, rbody, rcode, rheaders):
        return get_request_id()

class MockResponse():
    '''Mock response object which contains the request_id'''
    def __init__(self, request_id):
        self.request_id = request_id


def get_request_id():
    '''Return the MockRequest object which contains request_id'''
    response = MockResponse('dummy_request_id')
    return response

class TestDebugLogger(unittest.TestCase):
    @mock.patch('tap_stripe.LOGGER.debug')
    def test_debug_logger(self, mock_debug):
        '''Test that the debug is called with proper request id.'''
        mock_request = MockRequest('url')
        new_request(mock_request, 'GET', 'dummy_url')
        mock_debug.assert_called_with('request id : %s', 'dummy_request_id')
