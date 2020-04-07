import stripe
import unittest
from tap_stripe import recursive_to_dict

class TestRecursiveToDict(unittest.TestCase):

    def test_recursion(self):

        # Set up cards for customer object
        cards_list = [stripe.Card('card_{}'.format(i)) for i in range(123, 126)]
        for card in cards_list:
            self.assertTrue(isinstance(card, stripe.stripe_object.StripeObject))


        # Set up card for source object
        source_card = stripe.Card('card_314')
        source_object = stripe.Source('source_1')
        source_object['card'] = source_card

        # Set up card to use in a dictionary in customer
        old_card = stripe.Card('card_001')


        customer_object = stripe.Customer('cus_12345')
        self.assertTrue(isinstance(customer_object, stripe.stripe_object.StripeObject))
        customer_object['cards'] = cards_list
        self.assertTrue(isinstance(customer_object, stripe.stripe_object.StripeObject))
        customer_object['sources'] = source_object
        customer_object['metadata'] = {
            'city': 'Stripe City',
            'old_card': old_card
        }


        expected_object = {
            'id': 'cus_12345',
            'cards': [
                {'id': 'card_123'},
                {'id': 'card_124'},
                {'id': 'card_125'}
            ],
            'sources': {
                'id': 'source_1',
                'card': {'id': 'card_314'}
            },
            'metadata': {
                'city': 'Stripe City',
                'old_card': {'id': 'card_001'}
            }
        }

        self.assertEqual(recursive_to_dict(customer_object), expected_object)
