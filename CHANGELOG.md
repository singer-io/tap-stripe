# Changelog

** 1.4.6
  * Removed fields that caused transform errors

** 1.4.5
  * Added various schema [#77](https://github.com/singer-io/tap-stripe/pull/77)

** 1.4.4
  * Use the default timeout in the stripe client [#61](https://github.com/singer-io/tap-stripe/pull/61)

** 1.4.3
  * Revert 1.4.2 changes from #59 [#60](https://github.com/singer-io/tap-stripe/pull/60)
  * Remove invalid and unused schema pieces [#60](https://github.com/singer-io/tap-stripe/pull/60)

** 1.4.2	
  * Revert 1.4.1 [#59](https://github.com/singer-io/tap-stripe/pull/59)

** 1.4.1
  * Add functionality that recursively converts `StripeObject`s to dictionaries. [#53](https://github.com/singer-io/tap-stripe/pull/53)

** 1.4.0
  * Added the `payment_method_details` field to the charges stream. [#49](https://github.com/singer-io/tap-stripe/pull/49)

## 1.3.7
  * Add 5 minute lookback window on `events` and `balance_transactions` requests to catch an edge case. [#45](https://github.com/singer-io/tap-stripe/pull/45)

## 1.3.6
  * Use inclusive comparison for comparing bookmarks in the initial sync phase. [#44](https://github.com/singer-io/tap-stripe/pull/44)

## 1.3.5
  * Add "string" as a valid type for `plan` subschema, to support historical data [#42](https://github.com/singer-io/tap-stripe/pull/42)
  * `Deleted` events will not cause the tap to request their sub-objects to prevent 404 errors [#41](https://github.com/singer-io/tap-stripe/pull/41)

## 1.3.4
  * Filter out invoice line items with null ids in the Events stream because we don't know what to use as the primary key in those cases [#40](https://github.com/singer-io/tap-stripe/pull/40)
  * Add products stream [#32](https://github.com/singer-io/tap-stripe/pull/32)

## 1.3.3
  * Mitigate potential for infinite loop by increasing `limit` on second request for sub-objects (e.g., `invoice_line_items`) [#39](https://github.com/singer-io/tap-stripe/pull/39)

## 1.3.0
  * Add `disputes` stream [#29](https://github.com/singer-io/tap-stripe/pull/29)

## 1.2.8
  * Add subsciption and subscription_item to line_item PK [#28](https://github.com/singer-io/tap-stripe/pull/28)

## 1.2.0
  * Add `payout_transactions` stream and add date windowing to fix bookmarking [#23](https://github.com/singer-io/tap-stripe/pull/23)

## 1.1.2
  * Add optional `whitelist_map` config param which allows users to define a nested field whitelist [#22](https://github.com/singer-io/tap-stripe/pull/22)

## 1.1.1
  * On event updates, handles when `invoice_line_items` comes back as a dictionary instead of a list.
  * On event updates, skip the record when a sub-stream object doesn't have an "id" (e.g., older event update structures)

## 1.1.0
  * Invoice Line Items now use a composite PK [#19](https://github.com/singer-io/tap-stripe/pull/19)

## 1.0.2
  * Fixes `tiers` subschema to include its object properties (when it is an object) [#16](https://github.com/singer-io/tap-stripe/pull/16)

## 1.0.1
  * Fixes an issue where invoice events might have a different schema [#15](https://github.com/singer-io/tap-stripe/pull/15)

## 1.0.0
  * Adds proper support for Events to ensure only the most recent event is emitted [#13](https://github.com/singer-io/tap-stripe/pull/13)
  * Fixes JSON Schema refs to be correct [#14](https://github.com/singer-io/tap-stripe/pull/14)

## 0.2.4
  * Adds standard Singer metrics [#11](https://github.com/singer-io/tap-stripe/pull/11)

## 0.2.3
  * Unwraps data wrappers only if they are of type `list`
  * Adds `type` to the remaining `sources` schemas

## 0.2.2
  * Makes property accessors safer by using `.get()` with a default value
  * Adds `type` to items in `customers.cards`

## 0.2.1
  * Fixes sub-stream requests to not use a separate call to retrieve `subscription_items` and `invoice_line_items` for a parent.

## 0.2.0
  * Add date-window chunking to event updates stream [#9](https://github.com/singer-io/tap-stripe/pull/9)

## 0.1.1
  * Fix schema for subscriptions `details` to be a nullable object.

## 0.1.0
  * Initial release
