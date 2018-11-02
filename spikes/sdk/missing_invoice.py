import stripe
import singer
import logging
import sys

LOGGER = singer.get_logger()

if len(sys.argv) != 5:
    LOGGER.error('Expected 4 args, got %d', (len(sys.argv)-1))
    LOGGER.error('Example: python missing_invoice.py <stripe_account_id> <stripe_client_secret> <invoice_id> <invoice_date_epoch>')
    exit()

stripe_account_id = sys.argv[1]
stripe_client_secret = sys.argv[2]
invoice_id = sys.argv[3]
invoice_date = sys.argv[4]

def configure_stripe_client():
    stripe.api_key = stripe_client_secret
    stripe.api_version = '2018-09-24'
    stripe.max_network_retries = 15
    client = stripe.http_client.RequestsClient(timeout=15)
    stripe.default_http_client = client
    logging.getLogger('stripe').setLevel(logging.INFO)
    account = stripe.Account.retrieve(stripe_account_id)
    msg = "Successfully connected to Stripe Account with display name" \
          + " `%s`"
    LOGGER.info(msg, account.display_name)

configure_stripe_client()

print('\n \n')
LOGGER.info('========== Trying to get Invoice by ID ===========')
try:
    invoice_by_id = stripe.Invoice.retrieve(invoice_id)
except stripe.error.InvalidRequestError as ex:
    LOGGER.error("Failed to load invoice with id: %s", invoice_id)


print('\n \n')
LOGGER.info(' ========== Trying to get Invoice by date range ===========')

# try to get invoice by created by querying date range +/- 1 ms of target
invoice_greater_than = str(int(invoice_date) - 1)
invoice_less_than = str(int(invoice_date) + 1)

params = {
    "date[gte]": invoice_greater_than,
    "date[lte]": invoice_less_than
}
for invoice_obj in stripe.Invoice.list(stripe_account=stripe_account_id, **params).auto_paging_iter():
    if invoice_obj.id == invoice_id:
        LOGGER.info('Found Invoice Object using Date!')
        LOGGER.info('--------------------------------')
        LOGGER.info('ID: %s',invoice_obj.id)
        LOGGER.info('Date: %s', invoice_obj.date)
