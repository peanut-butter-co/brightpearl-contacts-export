# -*- coding: utf-8 -*-

import os
import requests
import json
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

BRIGHTPEARL_ACCOUNT = os.getenv('BRIGHTPEARL_ACCOUNT')
BRIGHTPEARL_API_TOKEN = os.getenv('BRIGHTPEARL_API_TOKEN')
BRIGHTPEARL_API_DOMAIN = os.getenv('BRIGHTPEARL_API_DOMAIN')
BRIGHTPEARL_APP_REF = os.getenv('BRIGHTPEARL_APP_REF')

if not all([BRIGHTPEARL_ACCOUNT, BRIGHTPEARL_API_TOKEN, BRIGHTPEARL_API_DOMAIN, BRIGHTPEARL_APP_REF]):
    print('Missing one or more required environment variables.')
    exit(1)

BASE_URL = 'https://{}/public-api/{}'.format(BRIGHTPEARL_API_DOMAIN, BRIGHTPEARL_ACCOUNT)
HEADERS = {
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'brightpearl-app-ref': BRIGHTPEARL_APP_REF,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

def make_request(url, headers, params=None):
    """Make a request to the Brightpearl API"""
    try:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("Error making request: {}".format(str(e)))
        return None

def get_order_details(order_id):
    """Get full order details by order ID"""
    url = "{}/order-service/order/{}".format(BASE_URL, order_id)
    order_data = make_request(url, HEADERS)
    if not order_data or not order_data.get('response'):
        print("Order not found")
        return None
    order = order_data['response'][0]
    return order

def print_order(order):
    if not order:
        return
    print("\n=== Order Information ===")
    print("Order ID: {}".format(order.get('id')))
    print("Order Type: {}".format(order.get('orderTypeCode')))
    print("Reference: {}".format(order.get('reference')))
    print("Status: {}".format(order.get('orderStatus', {}).get('name')))
    print("Payment Status: {}".format(order.get('orderPaymentStatus')))
    print("Stock Status: {}".format(order.get('stockStatusCode')))
    print("Allocation Status: {}".format(order.get('allocationStatusCode')))
    print("Shipping Status: {}".format(order.get('shippingStatusCode')))
    print("Placed On: {}".format(order.get('placedOn')))
    print("Created On: {}".format(order.get('createdOn')))
    print("Updated On: {}".format(order.get('updatedOn')))
    print("Currency: {}".format(order.get('currency', {}).get('orderCurrencyCode')))
    print("Total Net: {}".format(order.get('totalValue', {}).get('net')))
    print("Total Tax: {}".format(order.get('totalValue', {}).get('taxAmount')))
    print("Total: {}".format(order.get('totalValue', {}).get('total')))
    print("\n=== Parties ===")
    parties = order.get('parties', {})
    for role in ['customer', 'delivery', 'billing']:
        party = parties.get(role)
        if party:
            print("\n{}:".format(role.capitalize()))
            print("  Name: {}".format(party.get('addressFullName')))
            print("  Company: {}".format(party.get('companyName')))
            print("  Address: {} {} {} {}".format(
                party.get('addressLine1', ''),
                party.get('addressLine2', ''),
                party.get('addressLine3', ''),
                party.get('addressLine4', '')
            ))
            print("  Postal Code: {}".format(party.get('postalCode')))
            print("  Country: {}".format(party.get('country')))
            print("  Email: {}".format(party.get('email')))
            print("  Telephone: {}".format(party.get('telephone')))
    print("\n=== Order Rows ===")
    order_rows = order.get('orderRows', {})
    for row_id, row in order_rows.items():
        print("\nRow ID: {}".format(row_id))
        print("  Product ID: {}".format(row.get('productId')))
        print("  Product Name: {}".format(row.get('productName')))
        print("  SKU: {}".format(row.get('productSku')))
        print("  Quantity: {}".format(row.get('quantity', {}).get('magnitude')))
        print("  Net: {}".format(row.get('rowValue', {}).get('rowNet', {}).get('value')))
        print("  Tax: {}".format(row.get('rowValue', {}).get('rowTax', {}).get('value')))
        print("  Tax Rate: {}".format(row.get('rowValue', {}).get('taxRate')))
        print("  Tax Code: {}".format(row.get('rowValue', {}).get('taxCode')))
        if row.get('productOptions'):
            print("  Product Options: {}".format(json.dumps(row.get('productOptions'))))
        if row.get('composition'):
            print("  Composition: {}".format(json.dumps(row.get('composition'))))
    if order.get('customFields'):
        print("\n=== Custom Fields ===")
        for key, value in order['customFields'].items():
            print("{}: {}".format(key, value))
    if order.get('nullCustomFields'):
        print("\n=== Null Custom Fields ===")
        for key in order['nullCustomFields']:
            print(key)

def main():
    if len(sys.argv) != 2:
        print("Usage: python get_order.py <order_id>")
        sys.exit(1)
    try:
        order_id = int(sys.argv[1])
    except ValueError:
        print("Error: Order ID must be a number")
        sys.exit(1)
    order = get_order_details(order_id)
    if order:
        print_order(order)
        print("\n=== Raw JSON Response ===")
        print(json.dumps(order, indent=2, ensure_ascii=False))
    else:
        print("Failed to retrieve order information")

if __name__ == '__main__':
    main()

