import stripe
import unittest
from tap_stripe import convert_dict_to_stripe_object

class TestDictTOSTRIPEOBJECT(unittest.TestCase):

    def test_dict_to_stripe_object(self):
        """
        Test that `convert_dict_to_stripe_object` function convert field datatype of dict to stripe.stripe_object.StripeObject
        Example:
        """
        mock_object = {
            "id": "dummy_id",
            "tiers": [
                {
                    "flat_amount": 10,
                    "unit_amount": 7241350
                }
            ],
            "tier_mode": "volume"
        }
        
        # Verify that type of `tiers` field is stripe.stripe_object.StripeObject
        self.assertTrue(isinstance(convert_dict_to_stripe_object(mock_object).get('tiers')[0], stripe.stripe_object.StripeObject))

    def test_nested_dict_to_stripe_object(self):
        """
        Test that `convert_dict_to_stripe_object` function convert field datatype of nested dict to stripe.stripe_object.StripeObject
        """
        mock_object = {
            "id": "dummy_id",
            "tiers": [
                {
                    "flat_amount": [
                        {
                            'amount_1': 10,
                            'amount_2': 20
                        }
                    ],
                    "unit_amount": 7241350
                }
            ],
            "tier_mode": "volume"
        }
        
        # Verify that datatype nested field `flat_amount` is stripe.stripe_object.StripeObject
        self.assertTrue(isinstance(convert_dict_to_stripe_object(mock_object).get('tiers')[0].get('flat_amount')[0], stripe.stripe_object.StripeObject))