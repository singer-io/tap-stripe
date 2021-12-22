import tap_stripe
import unittest
from unittest import mock

class TestInvoicesBookmarks(unittest.TestCase):

    @mock.patch("tap_stripe.paginate")
    @mock.patch("tap_stripe.singer.get_bookmark")
    @mock.patch("tap_stripe.singer.write_bookmark")
    def test_invoices_date_bookmark(self, mocked_write_bookmark, mocked_get_bookmark, mocked_paginate):
        '''
            Verify that invoices use `date` field for bookmark and not a replication key `created` for invoices
        '''
        # Mock paginate, get_bookmark, and configure Context
        mocked_paginate.return_value = []
        mocked_get_bookmark.return_value = None
        tap_stripe.Context.catalog = tap_stripe.discover()
        tap_stripe.Context.config = {"start_date": "2021-01-01T00:00:00Z"}
        replication_key = None

        # Call sync_stream which will use 'date' field for read and write bookmark
        tap_stripe.sync_stream("invoices")

        # Verify that get_bookmark is called with 'date' field
        args, kwargs = mocked_get_bookmark.call_args
        self.assertEquals(args[1], "invoices")
        self.assertEquals(args[2], "date")

        # Verify that write_bookmark is called with 'date' field
        args, kwargs = mocked_write_bookmark.call_args
        self.assertEquals(args[1], "invoices")
        self.assertEquals(args[2], "date")

        self.assertEqual(replication_key, "test")
