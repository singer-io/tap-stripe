import unittest
from unittest import mock
from tap_stripe import Context, configure_stripe_client

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    @mock.patch('stripe.http_client.RequestsClient')
    @mock.patch('tap_stripe.apply_request_timer_to_client')
    @mock.patch('stripe.Account.retrieve')
    def test_config_provided_request_timeout(self, mock_retrieve, mock_req_timer, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = { "client_secret": "test_secret", "account_id": "test_account", "start_date": "test_start_date", "request_timeout": 100}
        Context.config = config
        configure_stripe_client()
        # Verify that the client is called with config provided request timeout
        mock_client.assert_called_with(timeout=100.0)

    @mock.patch('stripe.http_client.RequestsClient')
    @mock.patch('tap_stripe.apply_request_timer_to_client')
    @mock.patch('stripe.Account.retrieve')
    def test_default_value_request_timeout(self, mock_retrieve, mock_req_timer, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "test_start_date"}
        Context.config = config
        configure_stripe_client()
        # Verify that the client is called with default request timeout
        mock_client.assert_called_with(timeout=300.0)

    @mock.patch('stripe.http_client.RequestsClient')
    @mock.patch('tap_stripe.apply_request_timer_to_client')
    @mock.patch('stripe.Account.retrieve')
    def test_config_provided_empty_request_timeout(self, mock_retrieve, mock_req_timer, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "request_timeout": ""}
        Context.config = config
        configure_stripe_client()
        # Verify that the client is called with default request timeout
        mock_client.assert_called_with(timeout=300.0)
        
    @mock.patch('stripe.http_client.RequestsClient')
    @mock.patch('tap_stripe.apply_request_timer_to_client')
    @mock.patch('stripe.Account.retrieve')
    def test_config_provided_string_request_timeout(self, mock_retrieve, mock_req_timer, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "request_timeout": "100"}
        Context.config = config
        configure_stripe_client()
        # Verify that the client is called with config provided request timeout
        mock_client.assert_called_with(timeout=100.0)

    @mock.patch('stripe.http_client.RequestsClient')
    @mock.patch('tap_stripe.apply_request_timer_to_client')
    @mock.patch('stripe.Account.retrieve')
    def test_config_provided_float_request_timeout(self, mock_retrieve, mock_req_timer, mock_client):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "request_timeout": 100.8}
        Context.config = config
        configure_stripe_client()
        # Verify that the client is called with config provided float request timeout
        mock_client.assert_called_with(timeout=100.8)