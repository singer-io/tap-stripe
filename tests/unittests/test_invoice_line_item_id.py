import unittest
from unittest import mock
import tap_stripe

# mock invoice line items
class MockLines:
    def __init__(self, data):
        self.data = data

    def to_dict_recursive(self):
        return self.data

# mock invoice
class MockInvoice:
    def __init__(self, lines):
        self.lines = lines
        self.id = "inv_testinvoice"

# mock transform function
def transform(*args, **kwargs):
    # return the data with was passed for transformation in the argument
    return args[0]

@mock.patch("singer.Transformer.transform")
@mock.patch("tap_stripe.Context.get_catalog_entry")
@mock.patch("tap_stripe.Context.new_counts")
@mock.patch("tap_stripe.Context.updated_counts")
class InvoiceLineItemId(unittest.TestCase):
    """
        Test cases to verify the invoice line items 'id' is used as expected when syncing 'event updates'
    """

    def test_no_events_updates(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify no data should be changed when function is not called with 'event updates'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "ii_testinvoiceitem",
                "object": "line_item",
                "invoice_item": "ii_testinvoiceitem",
                "subscription": "sub_testsubscription",
                "type": "invoiceitem",
                "unique_id": "il_testlineitem"
            })
        ]

        # function call when 'updates=False'
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), False)

        # expected data
        expected_record = {
            "id": "ii_testinvoiceitem",
            "object": "line_item",
            "invoice_item": "ii_testinvoiceitem",
            "subscription": "sub_testsubscription",
            "type": "invoiceitem",
            "unique_id": "il_testlineitem",
            "invoice": "inv_testinvoice"
        }
        # get args for transform function
        args, kwargs = mocked_transform.call_args
        # verify the data is not changed as function was not called with updates
        self.assertEqual(expected_record, args[0])

    def test_no_unique_id(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify no data should be changed when invoice line item data does not contain 'unique_id'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "ii_testinvoiceitem",
                "object": "line_item",
                "invoice_item": "ii_testinvoiceitem",
                "subscription": "sub_testsubscription",
                "type": "invoiceitem"
            })
        ]

        # function call
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), True)

        # expected data
        expected_record = {
            "id": "ii_testinvoiceitem",
            "object": "line_item",
            "invoice_item": "ii_testinvoiceitem",
            "subscription": "sub_testsubscription",
            "type": "invoiceitem",
            "invoice": "inv_testinvoice"
        }
        # get args for transform function
        args, kwargs = mocked_transform.call_args
        # verify the data is not changed as not 'unique_id' is present
        self.assertEqual(expected_record, args[0])

    def test_no_updates_and_unique_id(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify no data should be changed when invoice line item data
            does not contain 'unique_id' and function is not called with 'event updates'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "ii_testinvoiceitem",
                "object": "line_item",
                "invoice_item": "ii_testinvoiceitem",
                "subscription": "sub_testsubscription",
                "type": "invoiceitem"
            })
        ]

        # function call with 'updates=False'
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), False)

        # expected data
        expected_record = {
            "id": "ii_testinvoiceitem",
            "object": "line_item",
            "invoice_item": "ii_testinvoiceitem",
            "subscription": "sub_testsubscription",
            "type": "invoiceitem",
            "invoice": "inv_testinvoice"
        }
        # get args for tranform function
        args, kwargs = mocked_transform.call_args
        # verify the data is not changed as the function was not called with updates and not unique_id is present
        self.assertEqual(expected_record, args[0])

    def test_invoiceitem_with_invoice_item(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify 'unique_id' is used as 'id' value when invoice line item type is 'invoiceitem'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "ii_testinvoiceitem",
                "object": "line_item",
                "invoice_item": "ii_testinvoiceitem",
                "subscription": "sub_testsubscription",
                "type": "invoiceitem",
                "unique_id": "il_testlineitem"
            })
        ]

        # function call with updates
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), True)

        # expected data
        expected_record = {
            "id": "il_testlineitem",
            "object": "line_item",
            "invoice_item": "ii_testinvoiceitem",
            "subscription": "sub_testsubscription",
            "type": "invoiceitem",
            "unique_id": "il_testlineitem",
            "invoice": "inv_testinvoice"
        }
        # get args for transform function
        args, kwargs = mocked_transform.call_args
        # verify the unique_id's value is used as 'id'
        self.assertEqual(expected_record, args[0])

    def test_invoiceitem_without_invoice_item(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify 'unique_id' is used as 'id' and 'invoice_item' field
            contains 'id' value when invoice line item type is 'invoiceitem'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "ii_testinvoiceitem",
                "object": "line_item",
                "invoice_item": None,
                "subscription": "sub_testsubscription",
                "type": "invoiceitem",
                "unique_id": "il_testlineitem"
            })
        ]

        # function call with updates
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), True)

        # expected data
        expected_record = {
            "id": "il_testlineitem",
            "object": "line_item",
            "invoice_item": "ii_testinvoiceitem",
            "subscription": "sub_testsubscription",
            "type": "invoiceitem",
            "unique_id": "il_testlineitem",
            "invoice": "inv_testinvoice"
        }
        # get args for transform function
        args, kwargs = mocked_transform.call_args
        # verify the unique_id's value is used as 'id' and id's value is used as 'invoice_item' value
        self.assertEqual(expected_record, args[0])

    def test_subscription_without_subscription(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify 'unique_id' is used as 'id' and 'subscription' field
            contains the 'id' value when invoice line item type is 'subscription'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "sub_testsubscription",
                "object": "line_item",
                "subscription": None,
                "type": "subscription",
                "unique_id": "il_testlineitem",
                "unique_line_item_id": "sli_testsublineitem"
            })
        ]

        # function call with updates
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), True)

        # expected data
        expected_record = {
            "id": "il_testlineitem",
            "object": "line_item",
            "subscription": "sub_testsubscription",
            "type": "subscription",
            "unique_id": "il_testlineitem",
            "unique_line_item_id": "sli_testsublineitem",
            "invoice": "inv_testinvoice"
        }
        # get args for transform function
        args, kwargs = mocked_transform.call_args
        # verify the unique_id's value is used as 'id' and id's value is used as 'subscription' value
        self.assertEqual(expected_record, args[0])

    def test_subscription_with_subscription(self, mocked_new_counts, mocked_updated_counts, mocked_get_catalog_entry, mocked_transform):
        """
            Test case to verify 'unique_id' is used as 'id' and 'subscription'
            field is not updated when invoice line item type is 'subscription'
        """
        # mock transform
        mocked_transform.side_effect = transform
        # create line items dummy data
        lines = [
            MockLines({
                "id": "sli_1KJvqbDcBSxinnbLvE4qMiJV",
                "object": "line_item",
                "subscription": "sub_testsubscription",
                "type": "subscription",
                "unique_id": "il_testlineitem",
                "unique_line_item_id": "sli_testsublineitem"
            })
        ]

        # function call with updates
        tap_stripe.sync_sub_stream("invoice_line_items", MockInvoice(lines), True)

        # expected data
        expected_record = {
            "id": "il_testlineitem",
            "object": "line_item",
            "subscription": "sub_testsubscription",
            "type": "subscription",
            "unique_id": "il_testlineitem",
            "unique_line_item_id": "sli_testsublineitem",
            "invoice": "inv_testinvoice"
        }
        # get args for transform function
        args, kwargs = mocked_transform.call_args
        # verify the unique_id's value is used as 'id'
        self.assertEqual(expected_record, args[0])
