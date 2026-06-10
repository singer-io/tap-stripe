import datetime
import unittest
from unittest import mock

import stripe
import tap_stripe
from tap_stripe import Context, should_sync_event, sync_event_updates

MOCK_NOW = datetime.datetime.strptime("2023-05-10T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ")
MOCK_NOW_EPOCH = int(MOCK_NOW.timestamp())
BOOKMARK = MOCK_NOW_EPOCH - 2 * 24 * 60 * 60
EVENT_CREATED = BOOKMARK + 100


class MockEventData:
    def __init__(self, obj):
        self.object = obj


class MockEvent:
    def __init__(self, obj, created, event_type):
        self.data = MockEventData(obj)
        self.created = created
        self.type = event_type


class MockEventList:
    def __init__(self, events):
        self.events = events

    def __len__(self):
        return len(self.events)

    def auto_paging_iter(self):
        return iter(self.events)


def make_discount_event():
    discount = stripe.Discount.construct_from(
        {"id": "di_test", "object": "discount", "customer": "cus_test"}, "sk_test")
    return MockEvent(discount, EVENT_CREATED, "customer.discount.created")


class TestShouldSyncEventResourceDictOverride(unittest.TestCase):
    """Verify the resource_dict override used for customer.discount.* events."""

    def test_override_passes_object_filter_and_dedups_by_customer_id(self):
        event = make_discount_event()
        id_to_created_map = {}

        # Without the override the discount object fails the ['customer'] filter
        self.assertFalse(should_sync_event(event, ["customer"], id_to_created_map))

        # With the override the event passes and registers the customer id
        self.assertTrue(should_sync_event(event, ["customer"], id_to_created_map,
                                          resource_dict={"id": "cus_test", "object": "customer"}))
        self.assertEqual(id_to_created_map, {"cus_test": EVENT_CREATED})

        # A second event for the same customer at the same time is deduped
        self.assertFalse(should_sync_event(event, ["customer"], id_to_created_map,
                                           resource_dict={"id": "cus_test", "object": "customer"}))


@mock.patch("tap_stripe.write_bookmark_for_event_updates")
@mock.patch("tap_stripe.singer.write_record")
@mock.patch("singer.Transformer.transform", side_effect=lambda rec, *args: rec)
@mock.patch("tap_stripe.Context.get_catalog_entry",
            return_value={"metadata": [], "schema": {}})
@mock.patch("tap_stripe.Context.is_selected", return_value=False)
@mock.patch("tap_stripe.Context.updated_counts")
@mock.patch("singer.utils.now", return_value=MOCK_NOW)
@mock.patch("stripe.Customer.retrieve")
@mock.patch("stripe.Event.list")
class TestCustomerDiscountEventUpdates(unittest.TestCase):
    """Verify customer.discount.* events fetch and sync the parent customer."""

    def setUp(self):
        Context.config = {"client_secret": "sk_test", "account_id": "acct_test",
                          "start_date": "2023-04-17T00:00:00"}
        Context.state = {"bookmarks": {"customers_events": {"updates_created": BOOKMARK}}}

    def test_discount_event_retrieves_customer_with_retrieve_safe_expands(
            self, mock_event_list, mock_customer_retrieve, mock_now, mock_updated_counts,
            mock_is_selected, mock_get_catalog_entry, mock_transform,
            mock_write_record, mock_write_bookmark):
        mock_event_list.side_effect = [MockEventList([make_discount_event()]), MockEventList([])]
        mock_customer_retrieve.return_value = stripe.Customer.construct_from(
            {"id": "cus_test", "object": "customer", "email": "test@example.com"}, "sk_test")

        sync_event_updates("customers", False)

        # The retrieve must use single-object expand paths, not the
        # list-endpoint 'data.*' paths (Stripe rejects those with
        # 'This property cannot be expanded (data).')
        mock_customer_retrieve.assert_called_once_with(
            "cus_test",
            expand=["sources", "subscriptions", "tax_ids"],
            stripe_account="acct_test",
        )

        # The fetched parent customer is written to the customers stream
        # (not dropped by the object-type filter)
        mock_write_record.assert_called_once()
        stream_name, rec = mock_write_record.call_args[0][:2]
        self.assertEqual(stream_name, "customers")
        self.assertEqual(rec["id"], "cus_test")
        self.assertEqual(rec["updated_by_event_type"], "customer.discount.created")
        self.assertEqual(rec["updated"], EVENT_CREATED)

    def test_discount_event_without_customer_id_is_skipped(
            self, mock_event_list, mock_customer_retrieve, mock_now, mock_updated_counts,
            mock_is_selected, mock_get_catalog_entry, mock_transform,
            mock_write_record, mock_write_bookmark):
        discount = stripe.Discount.construct_from(
            {"id": "di_test", "object": "discount", "customer": None}, "sk_test")
        event = MockEvent(discount, EVENT_CREATED, "customer.discount.created")
        mock_event_list.side_effect = [MockEventList([event]), MockEventList([])]

        sync_event_updates("customers", False)

        mock_customer_retrieve.assert_not_called()
        mock_write_record.assert_not_called()

    def test_duplicate_discount_events_fetch_customer_once(
            self, mock_event_list, mock_customer_retrieve, mock_now, mock_updated_counts,
            mock_is_selected, mock_get_catalog_entry, mock_transform,
            mock_write_record, mock_write_bookmark):
        mock_event_list.side_effect = [
            MockEventList([make_discount_event(), make_discount_event()]),
            MockEventList([]),
        ]
        mock_customer_retrieve.return_value = stripe.Customer.construct_from(
            {"id": "cus_test", "object": "customer"}, "sk_test")

        sync_event_updates("customers", False)

        # Dedup happens before the API call, so the customer is fetched once
        mock_customer_retrieve.assert_called_once()
        mock_write_record.assert_called_once()
