# Changelog

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
