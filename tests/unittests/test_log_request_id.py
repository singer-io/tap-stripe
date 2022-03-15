import unittest
from unittest import mock
from tap_stripe import new_list


class MockRequest():
    def __init__(self, response):
        self.last_response = response

class MockResponse():
    def __init__(self, request_id):
        self.request_id = request_id

class MockList(dict):
    '''Mock the ListObject class'''
    def __init__(self, url):
        self['url'] = url
    
    def request(self, method, url, params):
        '''Mock the request() method of the LisObject class '''
        return get_request_id()

def get_request_id():
    '''Return the MockRequest object which contains request_id'''
    response = MockResponse('dummy_request_id')
    request = MockRequest(response)
    return request

class TestDebugLogger(unittest.TestCase):
    
    @mock.patch('tap_stripe.LOGGER.debug')
    @mock.patch('tap_stripe.stripe.ListObject.request')
    def test_debug_logger(self, mock_request, mock_debug):
        '''Test that the debug is called with proper request id.'''
        list_object = MockList('url')
        new_list(list_object)
        mock_debug.assert_called_with('request id : dummy_request_id')
