import random
import stripe

from base import BaseTapTest

stripe.api_key = BaseTapTest.get_credentials()["client_secret"]

##########################################################################
# Create Invoice and invoice items methods are defined in the separate file
# to create the records with line details which are automatically populated
# in api response when generated using older version of api
##########################################################################


def create_invoice_items(customer_id, metadata_value, now_value):
    item = stripe.InvoiceItem.create(
        amount=random.randint(1, 10000),
        currency="usd",
        customer=customer_id,
        description="Comfortable cotton t-shirt {}".format(now_value),
        metadata=metadata_value,
        discountable=True,
        subscription_item=None,
        tax_rates=[],  # TODO enter the child attributes
        stripe_version='2020-08-27'
    )
    return item


def create_invoices(customer_id, customer_default_source, metadata_value, now_value):
    invoices_response = stripe.Invoice.create(
        customer=customer_id,
        auto_advance=False,
        collection_method='charge_automatically',
        description="Comfortable cotton t-shirt {}".format(now_value),
        metadata=metadata_value,
        footer='footer',
        statement_descriptor='desc',
        default_source=customer_default_source,
        default_tax_rates=[],
        stripe_version='2020-08-27'
    )
    return invoices_response
