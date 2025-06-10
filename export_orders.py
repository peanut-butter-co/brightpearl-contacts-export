# -*- coding: utf-8 -*-

import os
import requests
import csv
from dotenv import load_dotenv
import sys
import time
from typing import List, Dict, Any, Optional

# Set default encoding to UTF-8 for Python 2 compatibility
reload(sys)
sys.setdefaultencoding('utf8')

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

# Constants for rate limiting
REQUEST_DELAY = 0.5  # Half second delay between requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds to wait between retries

# Let's use simple text indicators instead of emojis for better compatibility
INDICATORS = {
    'error': '[ERROR]',
    'warning': '[WARNING]',
    'info': '[INFO]',
    'success': '[SUCCESS]',
    'progress': '[PROGRESS]'
}

def make_request(url, headers, params=None):
    """
    Make a rate-limited request with retries
    """
    start_time = time.time()
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)  # Rate limiting delay
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            elapsed = time.time() - start_time
            if elapsed > 5:  # Log if request takes more than 5 seconds
                print("{} API request took {:.1f}s: {}".format(
                    INDICATORS['warning'],
                    elapsed,
                    url
                ))
            return resp
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                if attempt < MAX_RETRIES - 1:  # Don't sleep on last attempt
                    print("{} Rate limit hit, waiting {} seconds...".format(
                        INDICATORS['warning'],
                        RETRY_DELAY
                    ))
                    time.sleep(RETRY_DELAY)
                    continue
            print("{} HTTP error on {}: {}".format(
                INDICATORS['error'],
                url,
                str(e)
            ))
            raise
        except Exception as e:
            print("{} Request error on {}: {}".format(
                INDICATORS['error'],
                url,
                str(e)
            ))
            raise
    return None

def get_orders(department_id=11):
    """
    Get all orders for the specified department using pagination
    """
    url = "{}/order-service/order-search".format(BASE_URL)
    all_order_ids = []
    first_result = 1
    page_size = 200  # Maximum allowed by Brightpearl
    
    while True:
        params = {
            "firstResult": first_result,
            "maxResults": page_size,
            "departmentId": department_id
        }
        
        try:
            resp = make_request(url, HEADERS, params)
            if not resp:
                break
                
            data = resp.json()
            results = data.get('response', {}).get('results', [])
            
            if not results:
                break
                
            # Extract order IDs from this page
            page_order_ids = [result[0] for result in results if result and len(result) > 0]
            all_order_ids.extend(page_order_ids)
            
            current_page = ((first_result - 1) // page_size) + 1
            print("{} Page {}: {} orders".format(INDICATORS['info'], current_page, len(page_order_ids)))
            
            # Update first_result for next page
            first_result += len(page_order_ids)
            
            # If we got fewer results than requested, we're done
            if len(page_order_ids) < page_size:
                break
            
        except Exception as e:
            print("{} Error fetching orders page {}: {}".format(INDICATORS['error'], current_page, e))
            break
    
    total = len(all_order_ids)
    print("{} Found {} total orders for department {}".format(INDICATORS['success'], total, department_id))
    return all_order_ids

def get_order_details(order_id):
    """
    Get full order details including all line items
    """
    url = "{}/order-service/order/{}".format(BASE_URL, order_id)
    try:
        resp = make_request(url, HEADERS)
        if not resp:
            return None
            
        data = resp.json()
        order = data.get('response', [])[0] if data.get('response') else None
        if not order:
            return None
            
        # Debug: Print the first order structure
        if order_id == data.get('response', [])[0].get('orderId'):
            print("\n{} First order structure:".format(INDICATORS['info']))
            print(order)
            
        return order
    except Exception as e:
        print("{} Error fetching order {} details: {}".format(INDICATORS['error'], order_id, str(e)))
        return None

def write_orders_csv(orders):
    """
    Write orders data to CSV file, with one row per order line item
    """
    if not os.path.exists('exports'):
        os.makedirs('exports')
        
    with open('exports/orders.csv', 'wb') as f:
        writer = csv.DictWriter(f, fieldnames=[
            # Order fields
            'orderId', 'parentOrderId', 'orderTypeCode', 'reference', 
            'orderStatusId', 'orderStatusName', 'orderPaymentStatus',
            'stockStatusCode', 'allocationStatusCode', 'shippingStatusCode',
            'placedOn', 'createdOn', 'updatedOn', 'closedOn',
            'priceListId', 'priceModeCode', 'warehouseId',
            # Currency fields
            'accountingCurrencyCode', 'orderCurrencyCode', 'exchangeRate',
            # Total value fields
            'orderNetTotal', 'orderTaxTotal', 'orderTotal',
            # Delivery fields
            'deliveryDate', 'shippingMethodId',
            # Assignment fields
            'staffOwnerContactId', 'projectId', 'channelId', 'leadSourceId', 'teamId',
            # Party fields - Supplier
            'supplierContactId', 'supplierName', 'supplierCompany', 
            'supplierEmail', 'supplierPhone',
            # Party fields - Delivery
            'deliveryName', 'deliveryCompany', 'deliveryEmail', 'deliveryPhone',
            'deliveryAddress1', 'deliveryAddress2', 'deliveryAddress3', 
            'deliveryAddress4', 'deliveryPostcode', 'deliveryCountry',
            # Party fields - Billing
            'billingContactId', 'billingName', 'billingCompany', 
            'billingEmail', 'billingPhone',
            'billingAddress1', 'billingAddress2', 'billingAddress3', 
            'billingAddress4', 'billingPostcode', 'billingCountry',
            # Row fields
            'rowId', 'rowSequence', 'productId', 'productName', 'productSku',
            'quantity', 'itemCostValue', 'itemCostCurrency',
            'taxRate', 'taxCode', 'rowNetValue', 'rowTaxValue',
            'nominalCode', 'bundleParent', 'bundleChild', 'parentOrderRowId',
            'productOptions'
        ])
        writer.writeheader()
        
        for order in orders:
            # Base order data that will be shared across all rows
            base_order = {
                'orderId': order.get('id', ''),
                'parentOrderId': order.get('parentOrderId', ''),
                'orderTypeCode': order.get('orderTypeCode', ''),
                'reference': order.get('reference', ''),
                'orderStatusId': order.get('orderStatus', {}).get('orderStatusId', ''),
                'orderStatusName': order.get('orderStatus', {}).get('name', ''),
                'orderPaymentStatus': order.get('orderPaymentStatus', ''),
                'stockStatusCode': order.get('stockStatusCode', ''),
                'allocationStatusCode': order.get('allocationStatusCode', ''),
                'shippingStatusCode': order.get('shippingStatusCode', ''),
                'placedOn': order.get('placedOn', ''),
                'createdOn': order.get('createdOn', ''),
                'updatedOn': order.get('updatedOn', ''),
                'closedOn': order.get('closedOn', ''),
                'priceListId': order.get('priceListId', ''),
                'priceModeCode': order.get('priceModeCode', ''),
                'warehouseId': order.get('warehouseId', ''),
                
                # Currency
                'accountingCurrencyCode': order.get('currency', {}).get('accountingCurrencyCode', ''),
                'orderCurrencyCode': order.get('currency', {}).get('orderCurrencyCode', ''),
                'exchangeRate': order.get('currency', {}).get('exchangeRate', ''),
                
                # Total value
                'orderNetTotal': order.get('totalValue', {}).get('net', ''),
                'orderTaxTotal': order.get('totalValue', {}).get('taxAmount', ''),
                'orderTotal': order.get('totalValue', {}).get('total', ''),
                
                # Delivery
                'deliveryDate': order.get('delivery', {}).get('deliveryDate', ''),
                'shippingMethodId': order.get('delivery', {}).get('shippingMethodId', ''),
                
                # Assignment
                'staffOwnerContactId': order.get('assignment', {}).get('current', {}).get('staffOwnerContactId', ''),
                'projectId': order.get('assignment', {}).get('current', {}).get('projectId', ''),
                'channelId': order.get('assignment', {}).get('current', {}).get('channelId', ''),
                'leadSourceId': order.get('assignment', {}).get('current', {}).get('leadSourceId', ''),
                'teamId': order.get('assignment', {}).get('current', {}).get('teamId', ''),
                
                # Parties - Supplier
                'supplierContactId': order.get('parties', {}).get('supplier', {}).get('contactId', ''),
                'supplierName': order.get('parties', {}).get('supplier', {}).get('addressFullName', ''),
                'supplierCompany': order.get('parties', {}).get('supplier', {}).get('companyName', ''),
                'supplierEmail': order.get('parties', {}).get('supplier', {}).get('email', ''),
                'supplierPhone': order.get('parties', {}).get('supplier', {}).get('telephone', ''),
                
                # Parties - Delivery
                'deliveryName': order.get('parties', {}).get('delivery', {}).get('addressFullName', ''),
                'deliveryCompany': order.get('parties', {}).get('delivery', {}).get('companyName', ''),
                'deliveryEmail': order.get('parties', {}).get('delivery', {}).get('email', ''),
                'deliveryPhone': order.get('parties', {}).get('delivery', {}).get('telephone', ''),
                'deliveryAddress1': order.get('parties', {}).get('delivery', {}).get('addressLine1', ''),
                'deliveryAddress2': order.get('parties', {}).get('delivery', {}).get('addressLine2', ''),
                'deliveryAddress3': order.get('parties', {}).get('delivery', {}).get('addressLine3', ''),
                'deliveryAddress4': order.get('parties', {}).get('delivery', {}).get('addressLine4', ''),
                'deliveryPostcode': order.get('parties', {}).get('delivery', {}).get('postalCode', ''),
                'deliveryCountry': order.get('parties', {}).get('delivery', {}).get('country', ''),
                
                # Parties - Billing
                'billingContactId': order.get('parties', {}).get('billing', {}).get('contactId', ''),
                'billingName': order.get('parties', {}).get('billing', {}).get('addressFullName', ''),
                'billingCompany': order.get('parties', {}).get('billing', {}).get('companyName', ''),
                'billingEmail': order.get('parties', {}).get('billing', {}).get('email', ''),
                'billingPhone': order.get('parties', {}).get('billing', {}).get('telephone', ''),
                'billingAddress1': order.get('parties', {}).get('billing', {}).get('addressLine1', ''),
                'billingAddress2': order.get('parties', {}).get('billing', {}).get('addressLine2', ''),
                'billingAddress3': order.get('parties', {}).get('billing', {}).get('addressLine3', ''),
                'billingAddress4': order.get('parties', {}).get('billing', {}).get('addressLine4', ''),
                'billingPostcode': order.get('parties', {}).get('billing', {}).get('postalCode', ''),
                'billingCountry': order.get('parties', {}).get('billing', {}).get('country', '')
            }
            
            # Write a row for each order line item
            order_rows = order.get('orderRows', {})
            for row_id, row in order_rows.items():
                order_row = base_order.copy()
                # Add row-specific fields
                order_row.update({
                    'rowId': row_id,
                    'rowSequence': row.get('orderRowSequence', ''),
                    'productId': row.get('productId', ''),
                    'productName': row.get('productName', ''),
                    'productSku': row.get('productSku', ''),
                    'quantity': row.get('quantity', {}).get('magnitude', ''),
                    'itemCostValue': row.get('itemCost', {}).get('value', ''),
                    'itemCostCurrency': row.get('itemCost', {}).get('currencyCode', ''),
                    'taxRate': row.get('rowValue', {}).get('taxRate', ''),
                    'taxCode': row.get('rowValue', {}).get('taxCode', ''),
                    'rowNetValue': row.get('rowValue', {}).get('rowNet', {}).get('value', ''),
                    'rowTaxValue': row.get('rowValue', {}).get('rowTax', {}).get('value', ''),
                    'nominalCode': row.get('nominalCode', ''),
                    'bundleParent': row.get('composition', {}).get('bundleParent', ''),
                    'bundleChild': row.get('composition', {}).get('bundleChild', ''),
                    'parentOrderRowId': row.get('composition', {}).get('parentOrderRowId', ''),
                    'productOptions': str(row.get('productOptions', {}))
                })
                
                # Ensure all values are encoded as UTF-8 strings
                encoded_row = {}
                for key, value in order_row.items():
                    if isinstance(value, unicode):
                        encoded_row[key] = value.encode('utf-8')
                    else:
                        encoded_row[key] = str(value)
                writer.writerow(encoded_row)

def main():
    # For testing - set to 0 for unlimited orders
    TEST_LIMIT = 4
    
    print("\n{} Starting orders export...".format(INDICATORS['info']))
    order_ids = get_orders(department_id=11)
    if TEST_LIMIT:
        original_count = len(order_ids)
        order_ids = order_ids[:TEST_LIMIT]
        print("\n{} TEST MODE: Processing {} of {} orders\n".format(
            INDICATORS['warning'],
            len(order_ids),
            original_count
        ))
    else:
        print("\n{} Processing all {} orders\n".format(INDICATORS['info'], len(order_ids)))
    
    orders = []
    total_orders = len(order_ids)
    for idx, oid in enumerate(order_ids, 1):
        try:
            # Progress indicator (update on same line)
            sys.stdout.write("\r{} Progress: {}/{} orders processed".format(
                INDICATORS['progress'],
                idx,
                total_orders
            ))
            sys.stdout.flush()
            
            # Get order details
            order = get_order_details(oid)
            if not order:
                print("\n{} Skipping order ID {} - no details found".format(INDICATORS['warning'], oid))
                continue
                
            orders.append(order)
                
        except Exception as e:
            print("\n{} Error processing order {}: {}".format(INDICATORS['error'], oid, str(e)))
            continue

    # Print a newline after progress is complete
    print("\n")
    print("{} Writing export file:".format(INDICATORS['info']))
    print("- orders.csv: {} orders with line items".format(len(orders)))
    write_orders_csv(orders)
    print('\n{} Export complete!'.format(INDICATORS['success']))

if __name__ == '__main__':
    main()
