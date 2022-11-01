from tap_stripe import STREAM_TO_TYPE_FILTER, should_sync_event
import unittest

# create mock classes for returning desired dummy data
class Object:
    def __init__(self, data) -> None:
        self.data = data
    def to_dict_recursive(self):
        return self.data

class Data:
    def __init__(self, data):
        self.object = Object(data)

# mock Payouts class
class MockPayout:
    def __init__(self, data):
        self.data = Data(data)
        self.created = "2022-01-01"

class TestPayoutEventObject(unittest.TestCase):
    """
        Test cases to verify the Tap syncs payout events with 'object' type 'transfer' and 'payout'
    """

    def test_payout_stream_transfer_object(self):
        """
            Test cases to verify the Tap syncs payout events with 'object' type 'transfer'
        """
        # function call
        should_sync = should_sync_event(MockPayout({"id": "po_test123", "object": "transfer"}), STREAM_TO_TYPE_FILTER.get("payouts").get("object"), {})
        # verify if the Tap will sync the payout events
        self.assertTrue(should_sync)

    def test_payout_stream_payout_object(self):
        """
            Test cases to verify the Tap syncs payout events with 'object' type 'payout'
        """
        # function call
        should_sync = should_sync_event(MockPayout({"id": "po_test123", "object": "payout"}), STREAM_TO_TYPE_FILTER.get("payouts").get("object"), {})
        # verify if the Tap will sync the payout events
        self.assertTrue(should_sync)

    def test_payout_stream_invalid_object(self):
        """
            Test cases to verify the Tap do not sync payout events with 'object' type other than 'transfer' or 'payout'
        """
        # function call
        should_sync = should_sync_event(MockPayout({"id": "po_test123", "object": "test"}), STREAM_TO_TYPE_FILTER.get("payouts").get("object"), {})
        # verify if the Tap will sync the payout events
        self.assertFalse(should_sync)
