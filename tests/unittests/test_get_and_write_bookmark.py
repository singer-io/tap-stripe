import tap_stripe
import unittest
from unittest import mock

class TestGetBookmarks(unittest.TestCase):

    @mock.patch("tap_stripe.singer.get_bookmark")
    def test_get_bookmark_for_invoices(self, mocked_get_bookmark):
        '''
            Verify that invoices use `date` field to get bookmark and not a replication key `created` for invoices
        '''
        # Call get_bookmark_for_stream for invoices with `created` replication key
        tap_stripe.get_bookmark_for_stream("invoices", "created")

        # Verify that get_bookmark is called with 'date' field
        args, kwargs = mocked_get_bookmark.call_args
        self.assertEquals(args[1], "invoices")
        self.assertEquals(args[2], "date")

    @mock.patch("tap_stripe.singer.get_bookmark")
    def test_get_bookmark_for_invoice_line_items(self, mocked_get_bookmark):
        '''
            Verify that invoice_line_items use `date` field to get bookmark and not a replication key `created` for invoice_line_items
        '''
        # Call get_bookmark_for_stream for invoice_line_items with `created` replication key
        tap_stripe.get_bookmark_for_stream("invoice_line_items", "created")

        # Verify that get_bookmark is called with 'date' field
        args, kwargs = mocked_get_bookmark.call_args
        self.assertEquals(args[1], "invoice_line_items")
        self.assertEquals(args[2], "date")

    @mock.patch("tap_stripe.singer.get_bookmark")
    def test_get_bookmark_for_normal_streams(self, mocked_get_bookmark):
        '''
            Verify that streams other than invoice and invoice_line_items use passed replication key to get bookmark
        '''
        # Call get_bookmark_for_stream for other test stream with `test_replication_key` replication key
        tap_stripe.get_bookmark_for_stream("test", "test_replication_key")

        # Verify that get_bookmark is called with 'test_replication_key' field which passed in get_bookmark_for_stream()
        args, kwargs = mocked_get_bookmark.call_args
        self.assertEquals(args[1], "test")
        self.assertEquals(args[2], "test_replication_key")


class TestWriteBookmarks(unittest.TestCase):
    
    @mock.patch("tap_stripe.singer.write_bookmark")
    def test_write_bookmark_for_invoices(self, mocked_write_bookmark):
        '''
            Verify that invoices use `date` field to write bookmark and not a replication key `created` for invoices
        '''
        # Call write_bookmark_for_stream for invoices with `created` replication key
        tap_stripe.write_bookmark_for_stream("invoices", "created", "bookmark_value")

        # Verify that write_bookmark is called with 'date' field
        args, kwargs = mocked_write_bookmark.call_args
        self.assertEquals(args[1], "invoices")
        self.assertEquals(args[2], "date")

    @mock.patch("tap_stripe.singer.write_bookmark")
    def test_write_bookmark_for_invoice_line_items(self, mocked_write_bookmark):
        '''
            Verify that invoice_line_items use `date` field to write bookmark and not a replication key `created` for invoice_line_items
        '''
        # Call write_bookmark_for_stream for invoice_line_items with `created` replication key
        tap_stripe.write_bookmark_for_stream("invoice_line_items", "created", "bookmark_value")

        # Verify that write_bookmark is called with 'date' field
        args, kwargs = mocked_write_bookmark.call_args
        self.assertEquals(args[1], "invoice_line_items")
        self.assertEquals(args[2], "date")

    @mock.patch("tap_stripe.singer.write_bookmark")
    def test_write_bookmark_for_normal_streams(self, mocked_write_bookmark):
        '''
            Verify that streams other than invoice and invoice_line_items use passed replication key to write bookmark
        '''
        # Call write_bookmark_for_stream for other test stream with `test_replication_key` replication key
        tap_stripe.write_bookmark_for_stream("test", "test_replication_key", "bookmark_value")

        # Verify that write_bookmark is called with 'test_replication_key' field which passed in write_bookmark_for_stream()
        args, kwargs = mocked_write_bookmark.call_args
        self.assertEquals(args[1], "test")
        self.assertEquals(args[2], "test_replication_key")